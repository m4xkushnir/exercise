//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::future::Future;
use std::hash::BuildHasher;
use std::sync::atomic::{fence, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard};
use std::time::Duration;

use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    pub name: String,
    pub max_running: u64,
}

impl ClusterConfig {
    pub fn new(name: impl Into<String>, max_running: u64) -> Self {
        Self {
            name: name.into(),
            max_running,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolConfig {
    pub groups: HashMap<String, Vec<ClusterConfig>>,
}

impl PoolConfig {
    pub fn group(mut self, name: impl Into<String>, clusters: Vec<ClusterConfig>) -> Self {
        self.groups.insert(name.into(), clusters);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The group does not exist in the current config.
    UnknownGroup,
    /// Every cluster in the group is at capacity.
    Busy,
    /// `acquire_timeout` waited for its full duration without getting a slot.
    Timeout,
}

/// Live state of one cluster. A cluster keeps the same state across reloads as long
/// as its (group, name) stays in the config, so permits, the running count and the
/// admissions metric all survive a reload.
///
/// The counters are hammered by every core, so each cluster gets its own 128 byte
/// block. Without it two clusters can share a cache line and slow each other down.
#[derive(Debug)]
#[repr(align(128))]
struct ClusterState {
    /// Permits handed out and not yet dropped. Only permits move it, so it is exact.
    leased: AtomicU64,
    /// What the engine runs beyond our permits (engine count minus leased), as learned
    /// by reconcile. Kept apart so reconcile never overwrites a counter permits rely on.
    drift: AtomicI64,
    /// Set to 0 when the cluster leaves the config, so it takes no new queries.
    max_running: AtomicU64,
    admissions: AtomicU64,
    name: String,
    waiters: Arc<Waiters>,
}

impl ClusterState {
    fn new(name: &str, waiters: Arc<Waiters>) -> Self {
        Self {
            leased: AtomicU64::new(0),
            drift: AtomicI64::new(0),
            max_running: AtomicU64::new(0),
            admissions: AtomicU64::new(0),
            name: name.to_owned(),
            waiters,
        }
    }

    fn running(&self) -> u64 {
        Self::effective(
            self.leased.load(Ordering::Relaxed),
            self.drift.load(Ordering::Relaxed),
        )
    }

    fn effective(leased: u64, drift: i64) -> u64 {
        leased.saturating_add_signed(drift)
    }

    fn free(&self) -> u64 {
        // running can sit above max after the cap is lowered.
        self.max_running
            .load(Ordering::Relaxed)
            .saturating_sub(self.running())
    }

    /// Take a slot if there is one. The cap check and the increment are one atomic step.
    /// Relaxed is enough because the counter guards no other memory. Every
    /// read-modify-write on one atomic sees the latest value anyway.
    fn try_lease(&self) -> bool {
        let mut leased = self.leased.load(Ordering::Relaxed);
        loop {
            let running = Self::effective(leased, self.drift.load(Ordering::Relaxed));
            if running >= self.max_running.load(Ordering::Relaxed) {
                return false;
            }
            match self.leased.compare_exchange_weak(
                leased,
                leased + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Same cache line as leased, which this core already owns, so it is cheap.
                    self.admissions.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                Err(actual) => leased = actual,
            }
        }
    }

    fn release(&self) {
        // SeqCst on both operations pairs with the fence in acquire_timeout. Either the
        // waiter's retry sees this decrement, or this load sees the waiter. That rules
        // out a lost wakeup. On x86 both compile to the same instructions as Relaxed.
        let before = self.leased.fetch_sub(1, Ordering::SeqCst);
        debug_assert!(before > 0, "released more permits than were taken");
        if self.waiters.queued.load(Ordering::SeqCst) > 0 {
            self.waiters.notify.notify_one();
        }
    }
}

/// Counters read together, before and after the slow engine call in reconcile.
#[derive(Clone, Copy)]
struct Counts {
    admitted: u64,
    leased: u64,
}

impl ClusterState {
    fn counts_before(&self) -> Counts {
        let admitted = self.admissions.load(Ordering::SeqCst);
        let leased = self.leased.load(Ordering::SeqCst);
        Counts { admitted, leased }
    }

    fn counts_after(&self) -> Counts {
        let leased = self.leased.load(Ordering::SeqCst);
        let admitted = self.admissions.load(Ordering::SeqCst);
        Counts { admitted, leased }
    }

    /// The engine counted its queries at some moment during the fetch. We do not know
    /// when, but we know how many permits came and went in that time, which bounds what
    /// `leased` was at that moment and so bounds the real drift. The drift only moves
    /// as far as it must to land inside those bounds. When nothing moved during the
    /// fetch the bounds meet and the correction is exact. Under load they are wide and
    /// we change little, instead of writing a stale number over live counters.
    ///
    /// The read order in counts_before and counts_after only ever widens the bounds,
    /// so a race there makes the correction more cautious, never wrong.
    fn correct_drift(&self, engine: u64, before: Counts, after: Counts) {
        let acquired = after.admitted.saturating_sub(before.admitted);
        let released = (before.leased + acquired).saturating_sub(after.leased);
        let lowest = before
            .leased
            .saturating_sub(released)
            .max(after.leased.saturating_sub(acquired));
        let highest = (before.leased + acquired).min(after.leased + released);
        if lowest > highest {
            return;
        }
        let engine = engine as i64;
        let (min_drift, max_drift) = (engine - highest as i64, engine - lowest as i64);
        let _ = self
            .drift
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |drift| {
                Some(drift.clamp(min_drift, max_drift))
            });
    }
}

/// Pick a cluster and take a slot on it.
///
/// Power of two choices. Look at two random clusters and take the one with more free
/// slots. It is close to least loaded but reads two counters instead of all of them,
/// and threads spread over the clusters instead of all piling onto the same "best"
/// one. If that cluster fills up first we fall back to a full least loaded scan, and
/// only report Busy once a scan finds no room at all.
fn admit(clusters: &[Arc<ClusterState>]) -> Option<&Arc<ClusterState>> {
    if clusters.len() >= 2 {
        let (a, b) = two_random(clusters.len());
        let (a, b) = (&clusters[a], &clusters[b]);
        let first = if a.free() >= b.free() { a } else { b };
        if first.try_lease() {
            return Some(first);
        }
    }
    loop {
        let (free, best) = clusters
            .iter()
            .map(|c| (c.free(), c))
            .max_by_key(|(free, _)| *free)?;
        if free == 0 {
            return None;
        }
        if best.try_lease() {
            return Some(best);
        }
    }
}

/// Two different indices below `n` (n >= 2) from a per thread xorshift generator.
/// No shared state, so it costs a few instructions and no cache traffic.
fn two_random(n: usize) -> (usize, usize) {
    thread_local! {
        static RNG: Cell<u64> = Cell::new(RandomState::new().hash_one(0u8) | 1);
    }
    let r = RNG.with(|rng| {
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        rng.set(x);
        x
    });
    let a = (r % n as u64) as usize;
    let b = (a + 1 + ((r >> 32) % (n as u64 - 1)) as usize) % n;
    (a, b)
}

/// Callers parked in `acquire_timeout` for one group. Shared by the group's clusters
/// and kept across reloads, like the cluster state.
#[derive(Debug, Default)]
struct Waiters {
    queued: AtomicU64,
    notify: Notify,
}

/// Counts a caller as queued for as long as it waits, including when its future is
/// dropped half way.
struct Queued<'a>(&'a Waiters);

impl<'a> Queued<'a> {
    fn enter(waiters: &'a Waiters) -> Self {
        waiters.queued.fetch_add(1, Ordering::SeqCst);
        Self(waiters)
    }
}

impl Drop for Queued<'_> {
    fn drop(&mut self) {
        self.0.queued.fetch_sub(1, Ordering::SeqCst);
    }
}

struct Group {
    clusters: Vec<Arc<ClusterState>>,
    waiters: Arc<Waiters>,
}

/// Every cluster ever configured, by (group, name). Only reload and metric reads
/// touch it, never the hot path.
#[derive(Default)]
struct Registry {
    clusters: HashMap<(String, String), Arc<ClusterState>>,
    waiters: HashMap<String, Arc<Waiters>>,
}

impl Registry {
    fn build(&mut self, config: &PoolConfig) -> HashMap<String, Group> {
        config
            .groups
            .iter()
            .map(|(group, clusters)| {
                let waiters = Arc::clone(self.waiters.entry(group.clone()).or_default());
                let clusters = clusters
                    .iter()
                    .map(|c| {
                        let state = self
                            .clusters
                            .entry((group.clone(), c.name.clone()))
                            .or_insert_with(|| {
                                Arc::new(ClusterState::new(&c.name, Arc::clone(&waiters)))
                            });
                        state.max_running.store(c.max_running, Ordering::Relaxed);
                        Arc::clone(state)
                    })
                    .collect();
                (group.clone(), Group { clusters, waiters })
            })
            .collect()
    }
}

pub struct ClusterPool {
    groups: RwLock<HashMap<String, Group>>,
    registry: Mutex<Registry>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        let mut registry = Registry::default();
        let groups = registry.build(&config);
        Arc::new(Self {
            groups: RwLock::new(groups),
            registry: Mutex::new(registry),
        })
    }

    // Nothing panics while holding these locks, but if it ever did the data is still
    // consistent (it is all atomics), so carry on instead of taking the gateway down.
    fn read_groups(&self) -> RwLockReadGuard<'_, HashMap<String, Group>> {
        self.groups.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn registry(&self) -> MutexGuard<'_, Registry> {
        self.registry.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.read_groups()
            .get(group)?
            .clusters
            .iter()
            .find(|c| c.name == cluster)
            .cloned()
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        // The read lock is held across the pick so a reload cannot retire a cluster
        // between the cap check and the increment.
        let groups = self.read_groups();
        let group = groups.get(group).ok_or(AcquireError::UnknownGroup)?;
        let cluster = admit(&group.clusters).ok_or(AcquireError::Busy)?;
        Ok(Permit {
            cluster: Arc::clone(cluster),
        })
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        let deadline = tokio::time::Instant::now() + wait;
        let waiters = {
            let groups = self.read_groups();
            let g = groups.get(group).ok_or(AcquireError::UnknownGroup)?;
            if let Some(cluster) = admit(&g.clusters) {
                return Ok(Permit {
                    cluster: Arc::clone(cluster),
                });
            }
            Arc::clone(&g.waiters)
        };

        let _queued = Queued::enter(&waiters);
        let wait_for_slot = async {
            loop {
                // Register for a wakeup before looking at the counters, then look.
                // A release between the look and the await still wakes us, because
                // we are already on the list. The fence pairs with release().
                let notified = waiters.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                fence(Ordering::SeqCst);
                match self.try_acquire(group) {
                    Err(AcquireError::Busy) => notified.await,
                    // A permit, or the group was removed by a reload.
                    done => return done,
                }
            }
        };
        tokio::time::timeout_at(deadline, wait_for_slot)
            .await
            .unwrap_or(Err(AcquireError::Timeout))
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        self.read_groups()
            .get(group)
            .map_or(0, |g| g.waiters.queued.load(Ordering::Relaxed))
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.registry()
            .clusters
            .get(&(group.to_owned(), cluster.to_owned()))
            .map_or(0, |c| c.admissions.load(Ordering::Relaxed))
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        // Store while holding the read lock. Otherwise a reload could run between the
        // lookup and the store, and this stale store would overwrite the new config.
        let groups = self.read_groups();
        let state = groups
            .get(group)
            .and_then(|g| g.clusters.iter().find(|c| c.name == cluster));
        if let Some(state) = state {
            state.max_running.store(max_running, Ordering::Relaxed);
            // A raised cap is free room nobody released, so wake the waiters to look.
            state.waiters.notify.notify_waiters();
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        let mut registry = self.registry();
        let mut groups = self.groups.write().unwrap_or_else(PoisonError::into_inner);
        // Retire every current cluster, then build() gives the ones still in the
        // config their cap back. We hold the write lock, so no caller sees the gap.
        for cluster in groups.values().flat_map(|g| &g.clusters) {
            cluster.max_running.store(0, Ordering::Relaxed);
        }
        *groups = registry.build(&config);
        drop(groups);
        // Caps may have grown and groups may be gone. Let every waiter look again.
        for waiters in registry.waiters.values() {
            waiters.notify.notify_waiters();
        }
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        // Hold the state itself across the await. If a reload lands meanwhile the
        // cluster keeps its identity, so the correction still goes to the right place.
        let Some(state) = self.find(group, cluster) else {
            return;
        };
        let before = state.counts_before();
        let engine = fetch().await;
        let after = state.counts_after();
        state.correct_drift(engine, before, after);
        // A smaller drift is free room nobody released.
        state.waiters.notify.notify_waiters();
    }
}

/// Proof of admission. The holder may run one query on `cluster()`.
///
/// It holds the cluster state itself, not a name to look up later, so it always gives
/// the slot back to the cluster it came from, even if a reload happened in between.
#[derive(Debug)]
pub struct Permit {
    cluster: Arc<ClusterState>,
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.cluster.name
    }

    /// Give the slot back once the query has finished.
    pub fn release(self) {
        // Drop does the work, so a permit is given back exactly once on every path.
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.cluster.release();
    }
}
