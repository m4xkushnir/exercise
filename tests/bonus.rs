//! INC-5 (reconcile) and INC-6 (reload). The reconcile tests control exactly when the
//! slow engine call returns, so the interleaving is the same on every run.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};
use tokio::sync::oneshot;

fn config(clusters: &[(&str, u64)]) -> PoolConfig {
    PoolConfig::default().group(
        "g",
        clusters
            .iter()
            .map(|(name, max)| ClusterConfig::new(*name, *max))
            .collect(),
    )
}

/// Runs reconcile with an engine call that returns `engine` once `go` fires, so the
/// test can admit and release queries while the call is in flight.
fn start_reconcile(
    pool: &Arc<ClusterPool>,
    engine: u64,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (go, wait) = oneshot::channel::<()>();
    let pool = Arc::clone(pool);
    let task = tokio::spawn(async move {
        pool.reconcile("g", "a", || async move {
            wait.await.unwrap();
            engine
        })
        .await;
    });
    (go, task)
}

#[tokio::test]
async fn inc5_queries_admitted_during_the_fetch_are_not_wiped() {
    let pool = ClusterPool::new(config(&[("a", 10)]));
    // The engine counts 0 right at the start. Three queries arrive while the answer
    // is on its way back.
    let (go, task) = start_reconcile(&pool, 0);
    tokio::task::yield_now().await;
    let permits: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();
    go.send(()).unwrap();
    task.await.unwrap();

    assert_eq!(pool.running("g", "a"), Some(3));
    drop(permits);
    assert_eq!(pool.running("g", "a"), Some(0), "releases after reconcile went below zero");
}

#[tokio::test]
async fn inc5_queries_released_during_the_fetch_are_not_resurrected() {
    let pool = ClusterPool::new(config(&[("a", 10)]));
    let permits: Vec<_> = (0..3).map(|_| pool.try_acquire("g").unwrap()).collect();
    // The engine counts our 3 queries, then they all finish before the answer lands.
    let (go, task) = start_reconcile(&pool, 3);
    tokio::task::yield_now().await;
    drop(permits);
    go.send(()).unwrap();
    task.await.unwrap();

    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.try_acquire("g").map(|p| p.cluster().to_owned()), Ok("a".into()));
}

#[tokio::test]
async fn inc5_real_drift_is_corrected_when_quiet() {
    // Someone runs 4 queries straight on the engine, bypassing the gateway.
    let pool = ClusterPool::new(config(&[("a", 5)]));
    let _ours = pool.try_acquire("g").unwrap();
    pool.reconcile("g", "a", || async { 5 }).await;
    assert_eq!(pool.running("g", "a"), Some(5));
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    // They finish. The next reconcile gives the room back.
    pool.reconcile("g", "a", || async { 1 }).await;
    assert_eq!(pool.running("g", "a"), Some(1));
    assert!(pool.try_acquire("g").is_ok());
}

#[test]
fn inc6_reload_keeps_running_counts_and_caps() {
    let pool = ClusterPool::new(config(&[("a", 2)]));
    let held: Vec<_> = (0..2).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(config(&[("a", 2), ("b", 1)]));
    assert_eq!(pool.running("g", "a"), Some(2), "dashboard reset to zero");
    let extra = pool.try_acquire("g").unwrap();
    assert_eq!(extra.cluster(), "b", "a is still full after the reload");
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.admissions("g", "a"), 2);
}

#[test]
fn inc6_removed_cluster_drains_and_comes_back_with_its_count() {
    let pool = ClusterPool::new(config(&[("a", 2), ("b", 2)]));
    let held: Vec<_> = (0..4).map(|_| pool.try_acquire("g").unwrap()).collect();

    pool.reload(config(&[("b", 2)]));
    assert_eq!(pool.running("g", "a"), None);
    assert_eq!(pool.try_acquire("g").err(), Some(AcquireError::Busy));

    // a comes back while two of its queries are still running.
    pool.reload(config(&[("a", 2), ("b", 2)]));
    assert_eq!(pool.running("g", "a"), Some(2));
    drop(held);
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.running("g", "b"), Some(0));
}

/// Reloads hammered against admissions from many threads. No cluster may ever be
/// over its cap, and every counter must land back on zero.
#[test]
fn inc6_cap_holds_while_reloading_under_load() {
    const THREADS: usize = 8;
    let pool = ClusterPool::new(config(&[("a", 3), ("b", 3)]));
    let held = [AtomicU64::new(0), AtomicU64::new(0)];
    let over_cap = AtomicBool::new(false);
    let stop = AtomicBool::new(false);
    let barrier = Barrier::new(THREADS + 1);

    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                barrier.wait();
                while !stop.load(Ordering::Relaxed) {
                    let Ok(permit) = pool.try_acquire("g") else { continue };
                    let slot = &held[(permit.cluster() == "b") as usize];
                    if slot.fetch_add(1, Ordering::SeqCst) + 1 > 3 {
                        over_cap.store(true, Ordering::SeqCst);
                    }
                    slot.fetch_sub(1, Ordering::SeqCst);
                    drop(permit);
                }
            });
        }
        barrier.wait();
        for i in 0..2_000 {
            // Flip between one and two clusters. Caps never change, so any count over
            // 3 is a reload letting queries through twice.
            if i % 2 == 0 {
                pool.reload(config(&[("a", 3)]));
            } else {
                pool.reload(config(&[("a", 3), ("b", 3)]));
            }
        }
        stop.store(true, Ordering::Relaxed);
    });

    assert!(!over_cap.load(Ordering::SeqCst), "a reload let a cluster run over its cap");
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.running("g", "b"), Some(0));
}
