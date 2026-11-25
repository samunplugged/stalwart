/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: a blob store that keeps every blob in two stores.
//!
//! | Operation         | Rule |
//! |-------------------|------|
//! | Write             | Write both at once. Succeed when both succeed, or when one succeeds and the other fails or times out; record the missed copy in the repair list |
//! | Both writes fail  | Return an error |
//! | Read              | Read the primary; on an error, a timeout or "not found", read the secondary |
//! | Delete            | Delete from both; a side that fails is recorded in the repair list |
//! | Store keeps failing | After a few failures in a row, mark it down for a while: reads go to the other store and missed writes are recorded |
//! | Repair            | A background task, run by one server at a time, copies or deletes what each side missed |
//!
//! The repair list lives in the data store (subspace `System`, keys starting
//! with [`REPAIR_KEY_PREFIX`]), so every server in a cluster shares it.
//! See `FORK.md`.

pub mod health;
pub mod repair;

use crate::{BlobStore, Store};
use health::SideHealth;
use registry::schema::structs::MirroredBlobStore;
use repair::{RepairList, RepairOp};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use trc::{AddContext, StoreEvent};

pub type SideFuture<'x, T> = Pin<Box<dyn Future<Output = trc::Result<T>> + Send + 'x>>;

/// One side of a mirror. Implemented by [`BlobStore`]; tests use fakes.
pub trait MirrorSide: Send + Sync {
    fn get<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, Option<Vec<u8>>>;
    fn put<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()>;
    fn replace<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()>;
    fn delete<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, bool>;
}

impl MirrorSide for BlobStore {
    fn get<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, Option<Vec<u8>>> {
        Box::pin(self.get_raw_blob(key))
    }

    fn put<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()> {
        Box::pin(self.write_blob(key, Ok(data)))
    }

    fn replace<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()> {
        match self {
            BlobStore::Fs(store) => Box::pin(async move {
                store
                    .replace_blob(key, &data)
                    .await
                    .caused_by(trc::location!())
            }),
            store => Box::pin(store.write_blob(key, Ok(data))),
        }
    }

    fn delete<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, bool> {
        Box::pin(self.delete_blob(key))
    }
}

pub const PRIMARY: usize = 0;
pub const SECONDARY: usize = 1;
const SIDE_NAMES: [&str; 2] = ["primary", "secondary"];

#[derive(Debug, Clone, Copy)]
pub struct MirrorConfig {
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    pub failure_threshold: u32,
    pub down_duration: Duration,
    pub repair_interval: Duration,
}

impl Default for MirrorConfig {
    fn default() -> Self {
        let config = MirroredBlobStore::default();
        MirrorConfig::from(&config)
    }
}

impl From<&MirroredBlobStore> for MirrorConfig {
    fn from(config: &MirroredBlobStore) -> Self {
        MirrorConfig {
            read_timeout: config.read_timeout.0,
            write_timeout: config.write_timeout.0,
            failure_threshold: u32::try_from(config.failure_threshold)
                .unwrap_or(u32::MAX)
                .max(1),
            down_duration: config.down_duration.0,
            repair_interval: config.repair_interval.0,
        }
    }
}

pub struct MirroredBlob {
    pub(crate) sides: [Arc<dyn MirrorSide>; 2],
    pub(crate) health: [SideHealth; 2],
    pub(crate) repair: RepairList,
    pub(crate) config: MirrorConfig,
}

/// What happened on one side of a write or delete.
enum SideResult<T> {
    Done(T),
    Failed(trc::Error),
    /// Not attempted because the side is marked down.
    Skipped,
}

impl MirroredBlob {
    pub fn new(
        primary: Arc<dyn MirrorSide>,
        secondary: Arc<dyn MirrorSide>,
        repair_store: Store,
        config: MirrorConfig,
    ) -> Self {
        MirroredBlob {
            sides: [primary, secondary],
            health: [
                SideHealth::new(config.failure_threshold, config.down_duration),
                SideHealth::new(config.failure_threshold, config.down_duration),
            ],
            repair: RepairList::new(repair_store),
            config,
        }
    }

    pub(crate) async fn open(
        config: MirroredBlobStore,
        primary: BlobStore,
        secondary: BlobStore,
        data_store: Store,
    ) -> Result<BlobStore, String> {
        Ok(BlobStore::Mirrored(Arc::new(MirroredBlob::new(
            Arc::new(primary),
            Arc::new(secondary),
            data_store,
            MirrorConfig::from(&config),
        ))))
    }

    pub fn config(&self) -> &MirrorConfig {
        &self.config
    }

    pub fn repair_list(&self) -> &RepairList {
        &self.repair
    }

    /// Whether a side (`PRIMARY` or `SECONDARY`) is currently marked down.
    pub fn is_down(&self, side: usize) -> bool {
        self.health[side].is_down()
    }

    pub(crate) async fn get_blob(&self, key: &[u8]) -> trc::Result<Option<Vec<u8>>> {
        // Read the primary first, unless it is down and the secondary is not
        let (first, second) = if self.is_down(PRIMARY) && !self.is_down(SECONDARY) {
            (SECONDARY, PRIMARY)
        } else {
            (PRIMARY, SECONDARY)
        };
        let second_up = !self.is_down(second);

        // The first read only waits as long as the timeout when there is
        // another store to fall back to
        let first_result = self
            .read_side(first, key, second_up.then_some(self.config.read_timeout))
            .await;
        if let Ok(Some(data)) = first_result {
            return Ok(Some(data));
        }

        // An error, a timeout or "not found" (a write may have reached only
        // the other store): read the other side
        let second_result = if second_up {
            Some(self.read_side(second, key, None).await)
        } else {
            None
        };

        match (first_result, second_result) {
            (_, Some(Ok(Some(data)))) => Ok(Some(data)),
            (Ok(None), Some(Ok(None)) | None) => Ok(None),
            (Err(err), _) | (Ok(None), Some(Err(err))) => Err(err),
            (Ok(Some(_)), _) => unreachable!(),
        }
    }

    pub(crate) async fn put_blob(&self, key: &[u8], data: Vec<u8>) -> trc::Result<()> {
        self.write_both(key, data, false).await
    }

    pub(crate) async fn replace_blob(&self, key: &[u8], data: Vec<u8>) -> trc::Result<()> {
        self.write_both(key, data, true).await
    }

    async fn write_both(&self, key: &[u8], data: Vec<u8>, replace: bool) -> trc::Result<()> {
        let attempt = self.sides_to_attempt();
        let primary_data = attempt[PRIMARY].then(|| data.clone());
        let secondary_data = attempt[SECONDARY].then_some(data);

        let (primary, secondary) = tokio::join!(
            self.write_side(PRIMARY, key, primary_data, replace),
            self.write_side(SECONDARY, key, secondary_data, replace),
        );

        self.settle(key, RepairOp::Put, [primary, secondary])
            .await
            .map(|_| ())
    }

    pub(crate) async fn delete_blob(&self, key: &[u8]) -> trc::Result<bool> {
        let attempt = self.sides_to_attempt();
        let (primary, secondary) = tokio::join!(
            self.delete_side(PRIMARY, key, attempt[PRIMARY]),
            self.delete_side(SECONDARY, key, attempt[SECONDARY]),
        );

        self.settle(key, RepairOp::Delete, [primary, secondary])
            .await
            .map(|deleted| deleted.into_iter().any(|deleted| deleted))
    }

    /// Sides a write or delete goes to: every side not marked down, or both
    /// when both are down (otherwise nothing could be written at all).
    fn sides_to_attempt(&self) -> [bool; 2] {
        let up = [!self.is_down(PRIMARY), !self.is_down(SECONDARY)];
        if up.iter().any(|up| *up) {
            up
        } else {
            [true, true]
        }
    }

    /// Applies the mirror's rules to the outcome of a write or delete on both
    /// sides: an error when no side succeeded, otherwise a repair entry for
    /// each side that missed the operation.
    async fn settle<T>(
        &self,
        key: &[u8],
        op: RepairOp,
        results: [SideResult<T>; 2],
    ) -> trc::Result<Vec<T>> {
        let mut done = Vec::with_capacity(2);
        let mut missed = Vec::with_capacity(2);
        let mut first_error = None;
        for (side, result) in results.into_iter().enumerate() {
            match result {
                SideResult::Done(value) => done.push(value),
                SideResult::Failed(err) => {
                    missed.push(side);
                    first_error.get_or_insert(err);
                }
                SideResult::Skipped => missed.push(side),
            }
        }

        if done.is_empty() {
            return Err(first_error.unwrap_or_else(|| {
                StoreEvent::UnexpectedError
                    .into_err()
                    .details("Mirrored blob store: no store was available")
            }))
            .caused_by(trc::location!());
        }

        // Record each missed copy; if that fails, fail the operation so the
        // caller retries rather than leave a single unrecorded copy
        for side in missed {
            self.repair
                .record(side, key, op)
                .await
                .caused_by(trc::location!())?;
        }

        Ok(done)
    }

    async fn read_side(
        &self,
        side: usize,
        key: &[u8],
        timeout: Option<Duration>,
    ) -> trc::Result<Option<Vec<u8>>> {
        let read = self.sides[side].get(key);
        let result = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, read).await {
                Ok(result) => result,
                Err(_) => Err(self.timeout_error(side, "read")),
            },
            None => read.await,
        };
        self.track(side, &result);
        result
    }

    async fn write_side(
        &self,
        side: usize,
        key: &[u8],
        data: Option<Vec<u8>>,
        replace: bool,
    ) -> SideResult<()> {
        let Some(data) = data else {
            return SideResult::Skipped;
        };
        let write = if replace {
            self.sides[side].replace(key, data)
        } else {
            self.sides[side].put(key, data)
        };
        let result = match tokio::time::timeout(self.config.write_timeout, write).await {
            Ok(result) => result,
            Err(_) => Err(self.timeout_error(side, "write")),
        };
        self.track(side, &result);
        match result {
            Ok(()) => SideResult::Done(()),
            Err(err) => SideResult::Failed(err),
        }
    }

    async fn delete_side(&self, side: usize, key: &[u8], attempt: bool) -> SideResult<bool> {
        if !attempt {
            return SideResult::Skipped;
        }
        let result =
            match tokio::time::timeout(self.config.write_timeout, self.sides[side].delete(key))
                .await
            {
                Ok(result) => result,
                Err(_) => Err(self.timeout_error(side, "delete")),
            };
        self.track(side, &result);
        match result {
            Ok(deleted) => SideResult::Done(deleted),
            Err(err) => SideResult::Failed(err),
        }
    }

    fn track<T>(&self, side: usize, result: &trc::Result<T>) {
        match result {
            Ok(_) => {
                if self.health[side].record_success() {
                    trc::event!(
                        Store(StoreEvent::BlobMirrorUp),
                        Id = SIDE_NAMES[side],
                        Details = "Mirrored blob store side is back up",
                    );
                }
            }
            Err(err) => {
                if self.health[side].record_failure() {
                    trc::event!(
                        Store(StoreEvent::BlobMirrorDown),
                        Id = SIDE_NAMES[side],
                        Elapsed = self.config.down_duration,
                        Reason = err.to_string(),
                        Details = "Mirrored blob store side marked down",
                    );
                }
            }
        }
    }

    fn timeout_error(&self, side: usize, operation: &'static str) -> trc::Error {
        StoreEvent::UnexpectedError
            .into_err()
            .details("Mirrored blob store side timed out")
            .ctx(trc::Key::Id, SIDE_NAMES[side])
            .ctx(trc::Key::Type, operation)
    }

    /// Copies or deletes what each side missed, for up to `max_entries`
    /// entries of the repair list. Call from one server at a time.
    pub async fn repair(&self, max_entries: usize) -> trc::Result<repair::RepairStats> {
        self.repair.run(self, max_entries).await
    }
}

#[cfg(test)]
mod tests;
