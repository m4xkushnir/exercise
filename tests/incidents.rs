//! One test (or more) per incident from the README. Each one fails on the starter code.
//! The race tests are probabilistic on the starter but deterministic on the fix, since
//! the fixed code has no window for them to hit.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

use admission::{ClusterConfig, ClusterPool, PoolConfig};

fn pool(clusters: &[(&str, u64)]) -> Arc<ClusterPool> {
    ClusterPool::new(
        PoolConfig::default().group(
            "g",
            clusters
                .iter()
                .map(|(name, max)| ClusterConfig::new(*name, *max))
                .collect(),
        ),
    )
}

/// INC-1. Many threads race for a small cap. We track how many permits each cluster
/// really has out and check it never goes over the cap. Afterwards every counter must
/// be back at zero and the group must still admit.
#[test]
fn inc1_cap_is_never_exceeded_and_counters_return_to_zero() {
    const THREADS: usize = 16;
    const ROUNDS: usize = 20_000;
    let pool = pool(&[("a", 3), ("b", 3)]);
    let held = [AtomicU64::new(0), AtomicU64::new(0)];
    let over_cap = AtomicBool::new(false);
    let barrier = Barrier::new(THREADS);

    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                barrier.wait();
                for _ in 0..ROUNDS {
                    let Ok(permit) = pool.try_acquire("g") else {
                        continue;
                    };
                    let slot = &held[(permit.cluster() == "b") as usize];
                    if slot.fetch_add(1, Ordering::SeqCst) + 1 > 3 {
                        over_cap.store(true, Ordering::SeqCst);
                    }
                    slot.fetch_sub(1, Ordering::SeqCst);
                    permit.release();
                }
            });
        }
    });

    assert!(
        !over_cap.load(Ordering::SeqCst),
        "a cluster ran more than max_running"
    );
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.running("g", "b"), Some(0));
    assert!(
        pool.try_acquire("g").is_ok(),
        "group is wedged after the spike"
    );
}

/// INC-2. Cutting the cap below the running count must not panic, and the group must
/// keep using its other clusters while the cut one drains.
#[test]
fn inc2_lowering_the_cap_while_busy_does_not_panic_or_block_the_group() {
    let pool = pool(&[("a", 40), ("b", 40)]);
    let mut on_a = Vec::new();
    let mut on_b = Vec::new();
    for _ in 0..80 {
        let p = pool.try_acquire("g").unwrap();
        if p.cluster() == "a" {
            on_a.push(p)
        } else {
            on_b.push(p)
        }
    }
    pool.set_max_running("g", "a", 10);

    // b frees up. a is still over its new cap, so every new query must go to b.
    on_b.drain(..).for_each(|p| p.release());
    for _ in 0..40 {
        assert_eq!(pool.try_acquire("g").unwrap().cluster(), "b");
    }

    // a drains to below its new cap and starts taking queries again.
    on_a.truncate(9);
    assert_eq!(pool.running("g", "a"), Some(9));
    let mut fresh = Vec::new();
    while let Ok(p) = pool.try_acquire("g") {
        fresh.push(p);
    }
    assert_eq!(pool.running("g", "a"), Some(10));
}

/// INC-3. Fewer callers than total slots, so there is always room somewhere.
/// Nobody may ever see Busy.
#[test]
fn inc3_no_busy_while_the_group_has_free_slots() {
    const THREADS: usize = 12;
    let pool = pool(&[("a", 4), ("b", 4), ("c", 4), ("d", 4)]);
    let busy = AtomicU64::new(0);
    let barrier = Barrier::new(THREADS);

    thread::scope(|s| {
        for _ in 0..THREADS {
            s.spawn(|| {
                barrier.wait();
                for _ in 0..20_000 {
                    match pool.try_acquire("g") {
                        Ok(p) => p.release(),
                        Err(_) => {
                            busy.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            });
        }
    });
    assert_eq!(
        busy.load(Ordering::Relaxed),
        0,
        "got Busy while slots were free"
    );
}

/// INC-4. A client that disconnects drops its permit without calling release.
#[test]
fn inc4_dropped_permit_gives_its_slot_back() {
    let pool = pool(&[("a", 1)]);
    drop(pool.try_acquire("g").unwrap());
    assert_eq!(pool.running("g", "a"), Some(0));
    assert!(pool.try_acquire("g").is_ok());
}

/// INC-4, the async version. The task holding the permit is cancelled mid-query,
/// which is what a client timeout looks like inside the gateway.
#[tokio::test]
async fn inc4_cancelled_query_gives_its_slot_back() {
    let pool = pool(&[("a", 1)]);
    let task = {
        let pool = Arc::clone(&pool);
        tokio::spawn(async move {
            let _permit = pool.try_acquire("g").unwrap();
            std::future::pending::<()>().await;
        })
    };
    while pool.running("g", "a") != Some(1) {
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    assert_eq!(pool.running("g", "a"), Some(0));
    assert!(pool.try_acquire("g").is_ok());
}
