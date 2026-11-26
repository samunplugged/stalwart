/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: background repair of the mirrored blob store.
//!
//! Every server runs this loop; a lease in the in-memory store (the same
//! mechanism Stalwart uses to run each cluster task on one server at a time)
//! makes sure only one of them repairs at once. See `FORK.md`.

use common::{BuildServer, Inner, KV_LOCK_BLOB_MIRROR};
use std::{sync::Arc, time::Duration};
use store::{BlobStore, rand};
use trc::StoreEvent;

/// Longest a server holds the repair lease without renewing it, in seconds.
const LEASE_DURATION: u64 = 5 * 60;
/// Entries repaired per batch; the lease is renewed between batches.
const REPAIR_BATCH: usize = 500;
/// Interval used while no mirrored blob store is configured.
const IDLE_INTERVAL: Duration = Duration::from_secs(60);
const LEASE_KEY: &[u8] = b"blob-mirror-repair";

pub fn spawn_blob_mirror_repair(inner: Arc<Inner>) {
    tokio::spawn(async move {
        let lease = rand::random::<u64>();
        loop {
            let interval = match inner.build_server().blob_store() {
                BlobStore::Mirrored(mirror) => mirror.config().repair_interval,
                _ => IDLE_INTERVAL,
            };
            tokio::time::sleep(interval).await;

            let server = inner.build_server();
            let BlobStore::Mirrored(mirror) = server.blob_store().clone() else {
                continue;
            };
            let memory = server.in_memory_store();

            match memory
                .try_lease(KV_LOCK_BLOB_MIRROR, LEASE_KEY, lease, LEASE_DURATION)
                .await
            {
                Ok(true) => {}
                Ok(false) => continue,
                Err(err) => {
                    trc::error!(
                        err.details("Failed to lock the mirrored blob store repair")
                            .caused_by(trc::location!())
                    );
                    continue;
                }
            }

            let mut repaired = 0;
            loop {
                match mirror.repair(REPAIR_BATCH).await {
                    Ok(stats) => {
                        repaired += stats.repaired;
                        if stats.examined > 0 || stats.remaining > 0 {
                            trc::event!(
                                Store(StoreEvent::BlobMirrorRepair),
                                Total = stats.remaining,
                                Size = repaired,
                                Details = "Mirrored blob store repair list length",
                            );
                        }
                        // Stop when the list is empty or nothing could be done
                        if stats.repaired == 0 || stats.remaining == 0 {
                            break;
                        }
                    }
                    Err(err) => {
                        trc::error!(
                            err.details("Mirrored blob store repair failed")
                                .caused_by(trc::location!())
                        );
                        break;
                    }
                }
                if !matches!(
                    memory
                        .renew_lease(KV_LOCK_BLOB_MIRROR, LEASE_KEY, lease, LEASE_DURATION)
                        .await,
                    Ok(true)
                ) {
                    break;
                }
            }

            if let Err(err) = memory
                .release_lease(KV_LOCK_BLOB_MIRROR, LEASE_KEY, lease)
                .await
            {
                trc::error!(
                    err.details("Failed to unlock the mirrored blob store repair")
                        .caused_by(trc::location!())
                );
            }
        }
    });
}
