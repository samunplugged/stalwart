/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

use super::{
    MirrorConfig, MirrorSide, MirroredBlob, PRIMARY, SECONDARY, SideFuture,
    repair::{RepairOp, RepairStats},
};
use crate::{
    BlobStore,
    backend::ephemeral::EphemeralStore,
    write::blob_codec::{BlobKeyring, BlobOpts},
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// An in-memory blob store that can be told to fail or to be slow.
#[derive(Default)]
struct FakeSide {
    blobs: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
    fail: AtomicBool,
    delay_ms: AtomicUsize,
    calls: AtomicUsize,
}

impl FakeSide {
    fn has(&self, key: &[u8]) -> bool {
        self.blobs.lock().contains_key(key)
    }

    fn insert(&self, key: &[u8], data: &[u8]) {
        self.blobs.lock().insert(key.to_vec(), data.to_vec());
    }

    fn set_failing(&self, fail: bool) {
        self.fail.store(fail, Ordering::SeqCst);
    }

    fn set_delay(&self, delay_ms: usize) {
        self.delay_ms.store(delay_ms, Ordering::SeqCst);
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    async fn enter(&self) -> trc::Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let delay = self.delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
        }
        if self.fail.load(Ordering::SeqCst) {
            Err(trc::StoreEvent::S3Error.reason("fake store failure"))
        } else {
            Ok(())
        }
    }
}

impl MirrorSide for FakeSide {
    fn get<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, Option<Vec<u8>>> {
        Box::pin(async move {
            self.enter().await?;
            Ok(self.blobs.lock().get(key).cloned())
        })
    }

    fn put<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()> {
        Box::pin(async move {
            self.enter().await?;
            self.blobs.lock().insert(key.to_vec(), data);
            Ok(())
        })
    }

    fn replace<'x>(&'x self, key: &'x [u8], data: Vec<u8>) -> SideFuture<'x, ()> {
        self.put(key, data)
    }

    fn delete<'x>(&'x self, key: &'x [u8]) -> SideFuture<'x, bool> {
        Box::pin(async move {
            self.enter().await?;
            Ok(self.blobs.lock().remove(key).is_some())
        })
    }
}

struct Mirror {
    mirror: MirroredBlob,
    primary: Arc<FakeSide>,
    secondary: Arc<FakeSide>,
}

fn config() -> MirrorConfig {
    MirrorConfig {
        read_timeout: Duration::from_millis(100),
        write_timeout: Duration::from_millis(100),
        failure_threshold: 3,
        down_duration: Duration::from_millis(300),
        repair_interval: Duration::from_secs(60),
    }
}

fn mirror() -> Mirror {
    let primary = Arc::new(FakeSide::default());
    let secondary = Arc::new(FakeSide::default());
    Mirror {
        mirror: MirroredBlob::new(
            primary.clone(),
            secondary.clone(),
            EphemeralStore::open(),
            config(),
        ),
        primary,
        secondary,
    }
}

impl Mirror {
    async fn entries(&self) -> Vec<(usize, Vec<u8>, RepairOp)> {
        let mut entries = self
            .mirror
            .repair_list()
            .entries(usize::MAX)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| (entry.side, entry.key, entry.op))
            .collect::<Vec<_>>();
        entries.sort_by(|a, b| (a.0, &a.1).cmp(&(b.0, &b.1)));
        entries
    }

    async fn repair(&self) -> RepairStats {
        self.mirror.repair(1000).await.unwrap()
    }
}

#[tokio::test]
async fn write_goes_to_both_stores() {
    let m = mirror();
    m.mirror.put_blob(b"k1", b"hello".to_vec()).await.unwrap();
    assert!(m.primary.has(b"k1"));
    assert!(m.secondary.has(b"k1"));
    assert!(m.entries().await.is_empty());
}

#[tokio::test]
async fn write_succeeds_when_one_store_fails() {
    let m = mirror();
    m.primary.set_failing(true);
    m.mirror.put_blob(b"k1", b"hello".to_vec()).await.unwrap();
    assert!(!m.primary.has(b"k1"));
    assert!(m.secondary.has(b"k1"));
    assert_eq!(
        m.entries().await,
        vec![(PRIMARY, b"k1".to_vec(), RepairOp::Put)]
    );

    // The same for the secondary
    m.primary.set_failing(false);
    m.secondary.set_failing(true);
    m.mirror.put_blob(b"k2", b"world".to_vec()).await.unwrap();
    assert!(m.primary.has(b"k2"));
    assert!(!m.secondary.has(b"k2"));
    assert_eq!(
        m.entries().await,
        vec![
            (PRIMARY, b"k1".to_vec(), RepairOp::Put),
            (SECONDARY, b"k2".to_vec(), RepairOp::Put)
        ]
    );

    // Once both work again, the repair copies what each side missed
    m.secondary.set_failing(false);
    let stats = m.repair().await;
    assert_eq!(stats.repaired, 2);
    assert_eq!(stats.remaining, 0);
    for key in [b"k1", b"k2"] {
        assert!(m.primary.has(key));
        assert!(m.secondary.has(key));
    }
    assert!(m.entries().await.is_empty());
}

#[tokio::test]
async fn write_succeeds_when_one_store_times_out() {
    let m = mirror();
    m.secondary.set_delay(500);
    let started = std::time::Instant::now();
    m.mirror.put_blob(b"slow", b"data".to_vec()).await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(400));
    assert!(m.primary.has(b"slow"));
    assert_eq!(
        m.entries().await,
        vec![(SECONDARY, b"slow".to_vec(), RepairOp::Put)]
    );
}

#[tokio::test]
async fn write_fails_when_both_stores_fail() {
    let m = mirror();
    m.primary.set_failing(true);
    m.secondary.set_delay(500);
    assert!(m.mirror.put_blob(b"k", b"data".to_vec()).await.is_err());
    assert!(m.entries().await.is_empty());

    m.secondary.set_delay(0);
    m.secondary.set_failing(true);
    assert!(m.mirror.put_blob(b"k", b"data".to_vec()).await.is_err());
    assert!(m.entries().await.is_empty());
}

#[tokio::test]
async fn read_prefers_primary_and_falls_back() {
    let m = mirror();
    m.primary.insert(b"both", b"from-primary");
    m.secondary.insert(b"both", b"from-secondary");
    m.secondary.insert(b"secondary-only", b"copy");

    // Primary first
    assert_eq!(
        m.mirror.get_blob(b"both").await.unwrap(),
        Some(b"from-primary".to_vec())
    );
    // Not found on the primary: read the secondary
    assert_eq!(
        m.mirror.get_blob(b"secondary-only").await.unwrap(),
        Some(b"copy".to_vec())
    );
    // Missing on both
    assert_eq!(m.mirror.get_blob(b"none").await.unwrap(), None);

    // Primary error: read the secondary
    m.primary.set_failing(true);
    assert_eq!(
        m.mirror.get_blob(b"both").await.unwrap(),
        Some(b"from-secondary".to_vec())
    );
    // Primary error and not found on the secondary: an error, not "not found"
    assert!(m.mirror.get_blob(b"none").await.is_err());

    // Primary too slow: read the secondary
    m.primary.set_failing(false);
    m.primary.set_delay(500);
    let started = std::time::Instant::now();
    assert_eq!(
        m.mirror.get_blob(b"both").await.unwrap(),
        Some(b"from-secondary".to_vec())
    );
    assert!(started.elapsed() < Duration::from_millis(400));

    // Secondary error after "not found" on the primary: an error
    m.primary.set_delay(0);
    m.secondary.set_failing(true);
    assert!(m.mirror.get_blob(b"secondary-only").await.is_err());
}

#[tokio::test]
async fn delete_removes_both_and_records_misses() {
    let m = mirror();
    m.mirror.put_blob(b"k", b"data".to_vec()).await.unwrap();
    assert!(m.mirror.delete_blob(b"k").await.unwrap());
    assert!(!m.primary.has(b"k") && !m.secondary.has(b"k"));
    assert!(m.entries().await.is_empty());

    // One side fails: the delete succeeds and the leftover is recorded
    m.mirror.put_blob(b"k", b"data".to_vec()).await.unwrap();
    m.secondary.set_failing(true);
    assert!(m.mirror.delete_blob(b"k").await.unwrap());
    assert!(!m.primary.has(b"k"));
    assert!(m.secondary.has(b"k"));
    assert_eq!(
        m.entries().await,
        vec![(SECONDARY, b"k".to_vec(), RepairOp::Delete)]
    );

    // The repair deletes the leftover
    m.secondary.set_failing(false);
    assert_eq!(m.repair().await.repaired, 1);
    assert!(!m.secondary.has(b"k"));
    assert!(m.entries().await.is_empty());

    // Both sides fail: an error
    m.mirror.put_blob(b"k", b"data".to_vec()).await.unwrap();
    m.primary.set_failing(true);
    m.secondary.set_failing(true);
    assert!(m.mirror.delete_blob(b"k").await.is_err());
}

#[tokio::test]
async fn failing_store_is_marked_down() {
    let m = mirror();
    m.primary.set_failing(true);
    for n in 0..3 {
        assert!(!m.mirror.is_down(PRIMARY), "down after {n} failures");
        m.mirror
            .put_blob(format!("k{n}").as_bytes(), b"data".to_vec())
            .await
            .unwrap();
    }
    assert!(m.mirror.is_down(PRIMARY));
    assert!(!m.mirror.is_down(SECONDARY));

    // While down, writes skip the primary but are recorded for repair
    let calls = m.primary.calls();
    m.mirror
        .put_blob(b"skipped", b"data".to_vec())
        .await
        .unwrap();
    assert_eq!(m.primary.calls(), calls);
    assert!(m.secondary.has(b"skipped"));
    assert_eq!(m.entries().await.len(), 4);

    // Reads go to the secondary without touching the primary
    assert_eq!(
        m.mirror.get_blob(b"skipped").await.unwrap(),
        Some(b"data".to_vec())
    );
    assert_eq!(m.primary.calls(), calls);

    // The repair waits while a side is down
    let stats = m.repair().await;
    assert_eq!(stats.repaired, 0);
    assert_eq!(stats.deferred, 4);

    // After the pause the primary is tried again; it works, so the repair
    // copies every missed blob
    m.primary.set_failing(false);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert!(!m.mirror.is_down(PRIMARY));
    let stats = m.repair().await;
    assert_eq!(stats.repaired, 4);
    assert_eq!(stats.remaining, 0);
    for key in ["k0", "k1", "k2", "skipped"] {
        assert!(m.primary.has(key.as_bytes()), "{key} not repaired");
    }
}

#[tokio::test]
async fn both_stores_down_still_attempts_writes() {
    let m = mirror();
    m.primary.set_failing(true);
    m.secondary.set_failing(true);
    for _ in 0..3 {
        assert!(m.mirror.put_blob(b"k", b"data".to_vec()).await.is_err());
    }
    assert!(m.mirror.is_down(PRIMARY) && m.mirror.is_down(SECONDARY));

    // Both down: writes still go to both rather than fail without trying
    m.secondary.set_failing(false);
    m.mirror.put_blob(b"k", b"data".to_vec()).await.unwrap();
    assert!(m.secondary.has(b"k"));
}

#[tokio::test]
async fn repair_follows_the_latest_operation() {
    // The primary missed a write, then the secondary missed the delete
    let m = mirror();
    m.primary.set_failing(true);
    m.mirror.put_blob(b"k", b"data".to_vec()).await.unwrap();
    m.primary.set_failing(false);
    m.secondary.set_failing(true);
    // Make the delete clearly later than the write
    tokio::time::sleep(Duration::from_millis(1100)).await;
    m.mirror.delete_blob(b"k").await.unwrap();
    m.secondary.set_failing(false);
    assert_eq!(
        m.entries().await,
        vec![
            (PRIMARY, b"k".to_vec(), RepairOp::Put),
            (SECONDARY, b"k".to_vec(), RepairOp::Delete)
        ]
    );
    assert!(m.secondary.has(b"k"));
    assert_eq!(m.repair().await.repaired, 2);
    assert!(!m.primary.has(b"k"));
    assert!(!m.secondary.has(b"k"));

    // A missed write whose blob was deleted since: nothing to copy
    let m = mirror();
    m.secondary.set_failing(true);
    m.mirror.put_blob(b"gone", b"data".to_vec()).await.unwrap();
    m.secondary.set_failing(false);
    m.mirror.delete_blob(b"gone").await.unwrap();
    assert_eq!(m.repair().await.repaired, 1);
    assert!(!m.primary.has(b"gone") && !m.secondary.has(b"gone"));
}

#[tokio::test]
async fn mirrored_blob_store_through_dispatch() {
    // The mirror as a `BlobStore`, with two in-memory stores as its sides,
    // so the blob codec and dispatch are exercised too
    let primary = BlobStore::Store(EphemeralStore::open());
    let secondary = BlobStore::Store(EphemeralStore::open());
    let store = BlobStore::Mirrored(Arc::new(MirroredBlob::new(
        Arc::new(primary.clone()),
        Arc::new(secondary.clone()),
        EphemeralStore::open(),
        config(),
    )));
    let keys = BlobKeyring::default();

    store
        .put_blob(b"blob", b"message body", &BlobOpts::PLAIN)
        .await
        .unwrap();
    assert!(primary.get_raw_blob(b"blob").await.unwrap().is_some());
    assert!(secondary.get_raw_blob(b"blob").await.unwrap().is_some());
    assert_eq!(
        store.get_blob(b"blob", 0..usize::MAX, &keys).await.unwrap(),
        Some(b"message body".to_vec())
    );

    // Lost from the primary: still readable
    primary.delete_blob(b"blob").await.unwrap();
    assert_eq!(
        store.get_blob(b"blob", 0..usize::MAX, &keys).await.unwrap(),
        Some(b"message body".to_vec())
    );

    assert!(store.delete_blob(b"blob").await.unwrap());
    assert!(secondary.get_raw_blob(b"blob").await.unwrap().is_none());
    assert_eq!(
        store.get_blob(b"blob", 0..usize::MAX, &keys).await.unwrap(),
        None
    );
}
