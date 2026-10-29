//! Part 3 cases the public tests do not cover. Capacity can come back without a
//! release, and a reload can pull the group out from under a waiter.

use std::sync::Arc;
use std::time::{Duration, Instant};

use admission::{AcquireError, ClusterConfig, ClusterPool, PoolConfig};

fn config(max_running: u64) -> PoolConfig {
    PoolConfig::default().group("g", vec![ClusterConfig::new("a", max_running)])
}

async fn queued_waiter(
    pool: &Arc<ClusterPool>,
) -> tokio::task::JoinHandle<Result<admission::Permit, AcquireError>> {
    let waiter = {
        let pool = Arc::clone(pool);
        tokio::spawn(async move { pool.acquire_timeout("g", Duration::from_secs(5)).await })
    };
    while pool.queued("g") == 0 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    waiter
}

#[tokio::test]
async fn raising_the_cap_wakes_a_waiter() {
    let pool = ClusterPool::new(config(1));
    let _held = pool.try_acquire("g").unwrap();
    let waiter = queued_waiter(&pool).await;

    let raised_at = Instant::now();
    pool.set_max_running("g", "a", 2);
    assert!(waiter.await.unwrap().is_ok());
    assert!(raised_at.elapsed() < Duration::from_millis(100));
}

#[tokio::test]
async fn reload_with_more_room_wakes_a_waiter() {
    let pool = ClusterPool::new(config(1));
    let _held = pool.try_acquire("g").unwrap();
    let waiter = queued_waiter(&pool).await;

    pool.reload(config(2));
    let _permit = waiter.await.unwrap().expect("waiter should get the new slot");
    assert_eq!(pool.running("g", "a"), Some(2));
}

#[tokio::test]
async fn reload_that_drops_the_group_ends_the_wait() {
    let pool = ClusterPool::new(config(1));
    let _held = pool.try_acquire("g").unwrap();
    let waiter = queued_waiter(&pool).await;

    pool.reload(PoolConfig::default());
    assert_eq!(waiter.await.unwrap().err(), Some(AcquireError::UnknownGroup));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn timeouts_racing_releases_never_strand_a_waiter() {
    // Half the callers give up almost at once, so many of them time out right as a
    // release wakes them. The wakeup must pass on to someone else, or the patient
    // half would sit there while a slot is free.
    let pool = ClusterPool::new(config(1));
    let tasks: Vec<_> = (0..200)
        .map(|i| {
            let pool = Arc::clone(&pool);
            let wait = if i % 2 == 0 { Duration::from_millis(1) } else { Duration::from_secs(10) };
            tokio::spawn(async move {
                match pool.acquire_timeout("g", wait).await {
                    Ok(permit) => {
                        tokio::time::sleep(Duration::from_micros(200)).await;
                        drop(permit);
                        true
                    }
                    Err(AcquireError::Timeout) => wait < Duration::from_secs(1),
                    Err(_) => false,
                }
            })
        })
        .collect();
    for task in tasks {
        assert!(task.await.unwrap(), "a patient caller timed out");
    }
    assert_eq!(pool.running("g", "a"), Some(0));
    assert_eq!(pool.queued("g"), 0);
}
