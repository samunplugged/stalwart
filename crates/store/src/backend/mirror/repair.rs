/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! The mirrored blob store's repair list and the repair run.
//!
//! An entry says that one side missed a write or a delete of a blob. Blobs are
//! named by a hash of their content, so a repair only has to make the side
//! that missed the operation match the other side: copy the blob if the other
//! side has it, delete it if not. When both sides missed something for the
//! same blob, the most recent operation decides.

use super::{MirroredBlob, PRIMARY, SECONDARY};
use crate::{
    IterateParams, Store, Subspace,
    write::{AnyClass, AnyKey, BatchBuilder, ValueClass, assert::AssertValue, now},
};
use ahash::AHashMap;
use trc::{AddContext, StoreEvent};

/// First byte of every key the fork keeps in the `System` subspace. Stalwart
/// itself uses 0x00 (schema version) and 0x01 (node ids) there.
pub const REPAIR_KEY_PREFIX: u8 = 0x57;
/// Second byte: a mirrored blob store repair entry.
pub const REPAIR_KIND_ENTRY: u8 = 0x01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairOp {
    Put = 1,
    Delete = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairEntry {
    /// The side that missed the operation.
    pub side: usize,
    pub key: Vec<u8>,
    pub op: RepairOp,
    /// When the operation was missed (seconds since the Unix epoch).
    pub timestamp: u64,
    raw_value: Vec<u8>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RepairStats {
    /// Entries read from the repair list in this run.
    pub examined: usize,
    /// Entries resolved and removed.
    pub repaired: usize,
    /// Entries left for a later run (a side down or an error).
    pub deferred: usize,
    /// Entries left in the repair list after the run.
    pub remaining: usize,
}

pub struct RepairList {
    store: Store,
}

impl RepairList {
    pub fn new(store: Store) -> Self {
        RepairList { store }
    }

    fn entry_key(side: usize, key: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(key.len() + 3);
        out.push(REPAIR_KEY_PREFIX);
        out.push(REPAIR_KIND_ENTRY);
        out.push(side as u8);
        out.extend_from_slice(key);
        out
    }

    fn class(side: usize, key: &[u8]) -> ValueClass {
        ValueClass::Any(AnyClass {
            subspace: Subspace::System,
            key: Self::entry_key(side, key),
        })
    }

    /// Records that `side` missed `op` on the blob `key`.
    pub async fn record(&self, side: usize, key: &[u8], op: RepairOp) -> trc::Result<()> {
        let mut value = Vec::with_capacity(9);
        value.push(op as u8);
        value.extend_from_slice(&now().to_be_bytes());
        let mut batch = BatchBuilder::new();
        batch.set(Self::class(side, key), value);
        self.store
            .write_batch(&mut batch)
            .await
            .map(|_| ())
            .caused_by(trc::location!())
    }

    fn range() -> IterateParams<AnyKey<Vec<u8>>> {
        IterateParams::new(
            AnyKey {
                subspace: Subspace::System,
                key: vec![REPAIR_KEY_PREFIX, REPAIR_KIND_ENTRY],
            },
            AnyKey {
                subspace: Subspace::System,
                key: vec![REPAIR_KEY_PREFIX, REPAIR_KIND_ENTRY + 1],
            },
        )
    }

    /// Reads up to `max` entries.
    pub async fn entries(&self, max: usize) -> trc::Result<Vec<RepairEntry>> {
        let mut entries = Vec::new();
        self.store
            .iterate(Self::range(), |key, value| {
                if let Some(([REPAIR_KEY_PREFIX, REPAIR_KIND_ENTRY, side], blob_key)) =
                    key.split_first_chunk::<3>()
                    && let Some((op, timestamp)) = value.split_first()
                    && (*side as usize) < 2
                {
                    entries.push(RepairEntry {
                        side: *side as usize,
                        key: blob_key.to_vec(),
                        op: if *op == RepairOp::Delete as u8 {
                            RepairOp::Delete
                        } else {
                            RepairOp::Put
                        },
                        timestamp: timestamp
                            .try_into()
                            .map(u64::from_be_bytes)
                            .unwrap_or_default(),
                        raw_value: value.to_vec(),
                    });
                }
                Ok(entries.len() < max)
            })
            .await
            .caused_by(trc::location!())?;
        Ok(entries)
    }

    /// Number of entries in the repair list.
    pub async fn len(&self) -> trc::Result<usize> {
        let mut count = 0;
        self.store
            .iterate(Self::range().no_values(), |_, _| {
                count += 1;
                Ok(true)
            })
            .await
            .caused_by(trc::location!())?;
        Ok(count)
    }

    pub async fn is_empty(&self) -> trc::Result<bool> {
        self.entries(1).await.map(|entries| entries.is_empty())
    }

    /// Removes an entry, unless it was recorded again since it was read.
    async fn remove(&self, entry: &RepairEntry) -> trc::Result<()> {
        let class = Self::class(entry.side, &entry.key);
        let mut batch = BatchBuilder::new();
        batch
            .assert_value(
                class.clone(),
                AssertValue::Hash(xxhash_rust::xxh3::xxh3_64(&entry.raw_value)),
            )
            .clear(class);
        match self.store.write_batch(&mut batch).await {
            Ok(_) => Ok(()),
            Err(err) if err.matches(trc::EventType::Store(StoreEvent::AssertValueFailed)) => Ok(()),
            Err(err) => Err(err.caused_by(trc::location!())),
        }
    }

    pub(crate) async fn run(
        &self,
        mirror: &MirroredBlob,
        max_entries: usize,
    ) -> trc::Result<RepairStats> {
        let entries = self.entries(max_entries).await?;
        let mut stats = RepairStats {
            examined: entries.len(),
            ..Default::default()
        };

        // Group the entries by blob
        let mut by_blob: AHashMap<Vec<u8>, Vec<RepairEntry>> = AHashMap::new();
        for entry in entries {
            by_blob.entry(entry.key.clone()).or_default().push(entry);
        }

        for (key, entries) in by_blob {
            match self.repair_blob(mirror, &key, &entries).await {
                Ok(true) => {
                    for entry in &entries {
                        self.remove(entry).await?;
                    }
                    stats.repaired += entries.len();
                }
                Ok(false) => stats.deferred += entries.len(),
                Err(err) => {
                    stats.deferred += entries.len();
                    trc::error!(
                        err.details("Mirrored blob store repair failed")
                            .ctx(trc::Key::Key, key)
                            .caused_by(trc::location!())
                    );
                }
            }
        }

        stats.remaining = self.len().await?;
        Ok(stats)
    }

    /// Makes both sides agree about one blob. Returns `false` when a side it
    /// needs is marked down, so the entries are kept for a later run.
    async fn repair_blob(
        &self,
        mirror: &MirroredBlob,
        key: &[u8],
        entries: &[RepairEntry],
    ) -> trc::Result<bool> {
        if mirror.is_down(PRIMARY) || mirror.is_down(SECONDARY) {
            return Ok(false);
        }

        let missed_primary = entries.iter().any(|entry| entry.side == PRIMARY);
        let missed_secondary = entries.iter().any(|entry| entry.side == SECONDARY);

        if missed_primary && missed_secondary {
            // Both sides missed something: the latest operation decides
            let latest = entries
                .iter()
                .max_by_key(|entry| entry.timestamp)
                .map(|entry| entry.op)
                .unwrap_or(RepairOp::Put);
            match latest {
                RepairOp::Put => {
                    for (from, to) in [(PRIMARY, SECONDARY), (SECONDARY, PRIMARY)] {
                        if let Some(data) = mirror.read_side(from, key, None).await? {
                            // Blobs never change, so a copy is always safe
                            mirror.sides[to].put(key, data).await?;
                            return Ok(true);
                        }
                    }
                    // Neither side has it any more: nothing to copy
                    Ok(true)
                }
                RepairOp::Delete => {
                    for side in [PRIMARY, SECONDARY] {
                        mirror.sides[side].delete(key).await?;
                    }
                    Ok(true)
                }
            }
        } else {
            // One side missed one or more operations: make it match the other
            let (missed, source) = if missed_primary {
                (PRIMARY, SECONDARY)
            } else {
                (SECONDARY, PRIMARY)
            };
            match mirror.read_side(source, key, None).await? {
                Some(data) => {
                    mirror.sides[missed].put(key, data).await?;
                }
                None => {
                    mirror.sides[missed].delete(key).await?;
                }
            }
            Ok(true)
        }
    }
}
