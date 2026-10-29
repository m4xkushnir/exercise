//! Cluster admission control for a SQL gateway.
//!
//! A gateway fronts several *groups* of engine clusters (Trino, StarRocks, ...).
//! Every incoming query must be admitted to one cluster of its target group
//! before it is forwarded, and each cluster accepts at most `max_running`
//! concurrent queries. The gateway handles thousands of queries per second,
//! so `try_acquire` sits on the hot path of every request.
//!
//! See README.md for the tasks.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

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

#[derive(Debug)]
struct ClusterState {
    name: String,
    max_running: AtomicU64,
    running: AtomicU64,
}

impl ClusterState {
    fn new(config: &ClusterConfig) -> Self {
        Self {
            name: config.name.clone(),
            max_running: AtomicU64::new(config.max_running),
            running: AtomicU64::new(0),
        }
    }

    fn running(&self) -> u64 {
        self.running.load(Ordering::Relaxed)
    }

    fn max_running(&self) -> u64 {
        self.max_running.load(Ordering::Relaxed)
    }

    fn available(&self) -> u64 {
        self.max_running() - self.running()
    }

    fn try_increment(&self) -> bool {
        let current = self.running.load(Ordering::Relaxed);
        if current >= self.max_running() {
            return false;
        }
        self.running.store(current + 1, Ordering::Relaxed);
        true
    }

    fn decrement(&self) {
        self.running.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    name: String,
    available: u64,
}

/// Least-loaded selection: the candidate with the most free slots wins.
fn pick(candidates: &[Candidate]) -> Option<usize> {
    candidates
        .iter()
        .enumerate()
        .max_by_key(|(_, c)| c.available)
        .map(|(i, _)| i)
}

pub struct ClusterPool {
    groups: RwLock<HashMap<String, Vec<Arc<ClusterState>>>>,
    admissions: Mutex<HashMap<String, u64>>,
}

impl ClusterPool {
    pub fn new(config: PoolConfig) -> Arc<Self> {
        Arc::new(Self {
            groups: RwLock::new(build_groups(&config)),
            admissions: Mutex::new(HashMap::new()),
        })
    }

    fn clusters(&self, group: &str) -> Option<Vec<Arc<ClusterState>>> {
        self.groups.read().unwrap().get(&group.to_string()).cloned()
    }

    fn find(&self, group: &str, cluster: &str) -> Option<Arc<ClusterState>> {
        self.clusters(group)?
            .into_iter()
            .find(|c| c.name == cluster)
    }

    /// Admit one query to the least-loaded cluster of `group`, or fail fast with `Busy`.
    pub fn try_acquire(self: &Arc<Self>, group: &str) -> Result<Permit, AcquireError> {
        let clusters = self.clusters(group).ok_or(AcquireError::UnknownGroup)?;

        let candidates: Vec<Candidate> = clusters
            .iter()
            .map(|c| Candidate {
                name: c.name.clone(),
                available: c.available(),
            })
            .filter(|c| c.available > 0)
            .collect();

        let chosen = pick(&candidates).ok_or(AcquireError::Busy)?;
        let name = candidates[chosen].name.clone();
        let state = clusters
            .iter()
            .find(|c| c.name == name)
            .ok_or(AcquireError::Busy)?;

        if !state.try_increment() {
            return Err(AcquireError::Busy);
        }

        *self
            .admissions
            .lock()
            .unwrap()
            .entry(format!("{group}/{name}"))
            .or_insert(0) += 1;

        Ok(Permit {
            pool: Arc::clone(self),
            group: group.to_string(),
            cluster: name,
        })
    }

    /// Part 3: like `try_acquire`, but wait up to `wait` for a slot to free up.
    pub async fn acquire_timeout(
        self: &Arc<Self>,
        group: &str,
        wait: Duration,
    ) -> Result<Permit, AcquireError> {
        let _ = (group, wait);
        todo!("Part 3")
    }

    fn release(&self, group: &str, cluster: &str) {
        if let Some(state) = self.find(group, cluster) {
            state.decrement();
        }
    }

    /// Current running count for a cluster, or `None` if it is not in the current config.
    pub fn running(&self, group: &str, cluster: &str) -> Option<u64> {
        self.find(group, cluster).map(|c| c.running())
    }

    /// Number of callers currently waiting inside `acquire_timeout` for `group`.
    pub fn queued(&self, group: &str) -> u64 {
        let _ = group;
        todo!("Part 3")
    }

    /// Total queries ever admitted to a cluster (a metric scraped by Prometheus).
    pub fn admissions(&self, group: &str, cluster: &str) -> u64 {
        self.admissions
            .lock()
            .unwrap()
            .get(&format!("{group}/{cluster}"))
            .copied()
            .unwrap_or(0)
    }

    /// Admin API: change a cluster's capacity at runtime.
    pub fn set_max_running(&self, group: &str, cluster: &str, max_running: u64) {
        if let Some(state) = self.find(group, cluster) {
            state.max_running.store(max_running, Ordering::Relaxed);
        }
    }

    /// Bonus: swap in a new config while queries are in flight.
    ///
    /// A cluster keeps its identity across reloads by (group, name). Clusters removed
    /// from the config take no new queries; clusters added to it take traffic at once.
    pub fn reload(&self, config: PoolConfig) {
        *self.groups.write().unwrap() = build_groups(&config);
    }

    /// Bonus: correct a cluster's running counter against the engine's own count.
    /// `fetch` asks the engine how many queries it is running; it is slow.
    pub async fn reconcile<F, Fut>(&self, group: &str, cluster: &str, fetch: F)
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = u64>,
    {
        let target = fetch().await;
        if let Some(state) = self.find(group, cluster) {
            state.running.store(target, Ordering::Relaxed);
        }
    }
}

fn build_groups(config: &PoolConfig) -> HashMap<String, Vec<Arc<ClusterState>>> {
    config
        .groups
        .iter()
        .map(|(group, clusters)| {
            let states = clusters.iter().map(|c| Arc::new(ClusterState::new(c))).collect();
            (group.clone(), states)
        })
        .collect()
}

/// Proof of admission. The holder may run one query on `cluster()`.
pub struct Permit {
    pool: Arc<ClusterPool>,
    group: String,
    cluster: String,
}

impl Permit {
    pub fn cluster(&self) -> &str {
        &self.cluster
    }

    /// Give the slot back once the query has finished.
    pub fn release(self) {
        // Drop does the work, so a permit is given back exactly once on every path.
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.pool.release(&self.group, &self.cluster);
    }
}
