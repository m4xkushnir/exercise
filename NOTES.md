# Notes

## Part 1: Incidents

All four reproducing tests are in `tests/incidents.rs` and each one fails on the starter. The two race tests (INC-1 and INC-3) are probabilistic on the starter, but they fail on every run I tried because the debug build panics as soon as the counter goes over the cap.

| Incident | Root cause | Reproducing test | Fix |
|----------|------------|------------------|-----|
| INC-1 | `try_increment` did a load, a cap check and then a store. Two threads could both read 5, both pass the check and both store 6. Both queries run but the counter only went up by one, so the cluster ran over its cap. When those queries finish, each release subtracts one, so the counter drifts below the real load and eventually below zero. A u64 below zero wraps to about 1.8e19, the cluster looks full forever, and `available()` wraps too, so `pick` keeps choosing it and the group returns Busy. Only a restart cleared the counter. | `inc1_cap_is_never_exceeded_and_counters_return_to_zero` | Check and increment in one compare and swap loop. |
| INC-2 | `available()` was `max - running` with no guard. Lowering the cap below the running count makes that negative. A debug build panics on overflow. A release build wraps to a huge number, so the cut cluster looks like it has the most free slots, `pick` always picks it, the increment fails and the whole group says Busy until the cluster drains. | `inc2_lowering_the_cap_while_busy_does_not_panic_or_block_the_group` | `saturating_sub`, so a cluster over its cap just has 0 free and the others take the traffic. |
| INC-3 | `try_acquire` picked one cluster and returned Busy if another thread filled it first, even when other clusters had room. INC-2's wrapped free count is a second cause of the same symptom. | `inc3_no_busy_while_the_group_has_free_slots` | Rescan after losing a race. Busy only when a scan finds no free slot anywhere. |
| INC-4 | `Permit` had no `Drop`. A client that disconnects or times out drops the permit without calling `release`, so the slot leaks until restart. | `inc4_dropped_permit_gives_its_slot_back`, `inc4_cancelled_query_gives_its_slot_back` | Release in `Drop`. `release(self)` now just consumes the permit, so a slot is given back exactly once on every path. |

INC-1 and INC-2 share the "wrapped counter wedges the group" ending, and INC-3 has two causes. The commit history fixes each one on the starter code first, before the Part 2 rewrite, so each root cause has its own small diff.

## Part 2: Performance

Machine is a MacBook Air with an Intel i5-1030NG7, 4 cores and 8 threads, so the 16 thread bench is oversubscribed 2x. Numbers are criterion medians. A laptop throttles, so expect about 10% noise between runs.

| Bench | Before | After Part 2 | Final (with Part 3 and bonus) |
|-------|--------|--------------|-------------------------------|
| acquire_release/1  | 3.53 µs, 284 K/s | 103 ns, 9.7 M/s | 126 ns, 7.9 M/s |
| acquire_release/4  | 5.80 µs, 690 K/s | 1.18 µs, 3.4 M/s | 1.45 µs, 2.8 M/s |
| acquire_release/16 | 16.2 µs, 985 K/s | 3.39 µs, 4.7 M/s | 3.71 µs, 4.3 M/s |

The final result is 4.4x the starter on `acquire_release/16` and 28x on one thread.

Changes, and why each one helps (or didn't):

- **No allocations on the hot path.** The starter allocated around 12 times per acquire and release. It cloned the cluster Vec, built a Vec of `Candidate` with a `String` per cluster, cloned the chosen name, formatted a `"group/cluster"` key, allocated the permit's group and cluster strings, and on release cloned the Vec again to look the cluster up by name. Allocation is slow on its own, and the allocator shares state between threads. This is most of the 28x on one thread.
- **No global Mutex.** The admissions metric took one Mutex for every query in the process, so all 16 threads queued on one lock. It is now an `AtomicU64` sitting next to the running counter. The CAS that admits a query has just pulled that cache line into this core, so the extra `fetch_add` is nearly free.
- **Permit holds `Arc<ClusterState>`.** Release is one `fetch_sub` with no lock and no lookup. It is also a correctness fix, because the permit can no longer release into a different cluster after a reload.
- **No Arc refcount storm.** Cloning the Vec bumped 8 shared refcounts per call, twice per query. Every thread wrote the same 8 cache lines. Now there is one clone, for the permit.
- **Lookup by `&str`.** `get(&group.to_string())` allocated just to hash. `HashMap<String, _>` can be queried with `&str` directly.
- **Power of two choices.** Max-by-free over all clusters sends every thread to the same "best" cluster, which is the same cache line, so they fight over it with CAS retries. Picking two clusters at random and taking the freer one spreads threads across the 8 clusters and only reads 2 counters. The random numbers come from a per thread xorshift, with no shared state.
- **`#[repr(align(128))]` on cluster state.** Each cluster's counters get their own cache line (two lines on Intel, where the adjacent line prefetcher pairs them), so a busy cluster does not slow its neighbour through false sharing. I did not measure this one alone, so I can't claim a number for it.
- **What it cost later.** Part 3 and the bonus took about 20% back off the single thread number. `release` now also loads `queued`, `try_lease` loads `drift`, and the `SeqCst` on release adds a store fence on ARM, though on x86 it compiles to the same `lock xadd` as Relaxed. I think that is the right trade, since lost wakeups and a broken reconcile are worse than 23 ns.
- **What is left.** The `RwLock` read is now the main shared write. Every acquire bumps the lock's reader count, which is one cache line that every core writes to. That is why 16 threads are only about 2x faster than 4 here. The next step would be an `arc-swap` style snapshot, or per thread caching with a generation number. I left it alone because the target was met and it adds subtle reload races, see "Before shipping".

Selection policy, and why:

Power of two choices with a least-loaded fallback. Take two random clusters and admit to the one with more free slots. If it filled up meanwhile, scan all clusters and take the one with the most free, and retry until a scan finds nothing free. This is "roughly least loaded" in a well studied way. With two choices the busiest cluster is only a small amount above average, compared to a large gap with one random choice, and it avoids the thundering herd where everyone picks the same max. The fallback means it never returns Busy while any cluster has room. Free slots are absolute numbers, not a percentage, which matches the README ("by how much capacity each has free").

## Part 3: Waiting for a slot

Each group has one `tokio::sync::Notify` and a `queued` counter, both shared by its clusters and kept across reloads.

- `acquire_timeout` tries once. If the group is full, it counts itself in `queued` with a guard (so a dropped or timed out future always leaves the queue), then loops. Each pass creates a `Notified`, calls `enable()` to get on the wait list, issues a `SeqCst` fence and retries. If the retry is still Busy it awaits the notification. The whole loop sits inside `timeout_at(deadline)`, so a wakeup that loses its slot to someone else goes back to waiting without resetting the clock.
- `release` decrements with `SeqCst`, then loads `queued` with `SeqCst`, and calls `notify_one` only if someone is waiting. So the common case with no waiters pays one extra load.
- **Why no wakeup is lost.** The waiter does "write queued, fence, read running" and the releaser does "write running, read queued". With SeqCst on both sides, at least one of them sees the other. Either the waiter's retry sees the free slot, or the releaser sees the waiter and notifies it. Because the waiter enabled its `Notified` before the fence, the notify cannot slip in between the retry and the await.
- **A woken waiter that times out.** Tokio forwards an unconsumed `notify_one` to the next waiter when a `Notified` is dropped, so a timeout racing a wakeup does not strand anyone. `timeouts_racing_releases_never_strand_a_waiter` stresses this.
- Room can come back without a release, so `set_max_running`, `reload` and `reconcile` all call `notify_waiters`. A reload that removes the group wakes its waiters, and they return `UnknownGroup`.
- **Not FIFO.** A fresh `try_acquire` can take a slot that just freed before the woken waiter gets to it (barging). It gives better throughput and never loses a wakeup, but a waiter can be overtaken. If the product needs strict fairness, release would hand the slot straight to the head waiter.

Extra tests are in `tests/waiting_edges.rs`. They cover a raised cap and a reload waking waiters, a reload removing the group, and timeouts racing releases.

## Part 4: Global admission across 6 pods

**The design is a leased quota.** Keep the hot path exactly as it is, with a local atomic CAS and no network. What changes is where each pod's `max_running` comes from. A small coordinator (Redis or etcd, something that already gives atomic ops and TTLs) owns each cluster's real cap of 40. Every pod holds a lease on a share of it, say 7 slots with a 10 second TTL, and the pod's local cap is that share. A background task per pod renews every 2 to 3 seconds and asks for more when it is above 80% of its share, or hands slots back when it is idle. The coordinator never hands out more than 40 in total. The analogy is a cinema with 40 seats selling through 6 ticket booths. Each booth gets a block of tickets and asks the box office for more when it runs low, instead of phoning the box office for every single ticket.

Hot path cost is zero network calls. It is the same CAS as today. The coordinator sees one call per pod per cluster every few seconds, so 6 pods and 30 clusters is about 60 to 90 small requests a second.

**A pod dies holding slots.** Its lease expires after the TTL and the coordinator gives those slots to the other pods. For up to 10 seconds the cluster runs below its cap. That errs on the safe side, and the TTL is the knob between "fast recovery" and "renewal traffic".

**Network partition.** A pod that cannot reach the coordinator keeps using its current lease until the lease is about to expire (TTL minus a safety margin for clock drift), then drops its local cap to 0 and returns Busy or queues. The coordinator only reassigns those slots after the full TTL. So the two sides never use the same slot at once, and the cap holds. The cost is that a partitioned pod stops admitting after about 10 seconds. If that is too harsh, it could fall back to a static `40 / 6` floor, which accepts a small overshoot in exchange for availability. Which way to go is a product call.

**Safety net.** The per-pod `reconcile` already measures what the engine runs beyond our permits. With 6 pods that drift is roughly "everyone else's load", so even without the coordinator it pulls each pod towards the global cap, just slowly and roughly. It also catches leaked leases and anything that bypasses the gateway.

**What I would deliberately not build.**
- A coordinator call per query. That adds a network round trip and a single point of failure to every query in order to get exact accuracy, when a cap of 40 does not need it.
- My own consensus or Raft. Use the TTL and atomic ops that Redis or etcd already have.
- Strict global fairness or a global queue across pods. Each pod queues locally against its share.
- A first version that is clever. Version zero is a static split (`max_running / pod count` pushed by config) plus reconcile, which needs no new service. Leasing comes when skewed load makes the static split waste capacity.

## Bonus: INC-5 and INC-6

**INC-5, reconcile made things worse.** It awaited a slow engine call and then stored the engine's count over the live counter. Every acquire and release during the call was wiped out. Releases after the store then pushed the counter below the number of live permits, and with enough of them below zero (the same wrap-and-wedge as INC-1). The faster the traffic, the more there is to wipe out, which is why it got worse under load.

The fix splits the counter in two.
- `leased` counts permits we handed out. Only permits move it, so it is always exact, and a release can never underflow it.
- `drift` is what the engine runs beyond our permits (manual queries, other pods, leaks). Running = `leased + drift`, and the cap check uses that sum.

`reconcile` reads `admissions` and `leased` before and after the fetch. The engine counted at some unknown moment during the fetch, but we know how many permits arrived and left in that window, which bounds what `leased` was at that moment and so bounds the real drift to a range. `drift` is clamped into that range with one atomic `fetch_update`, so it only moves as far as the evidence forces. On a quiet cluster the range is a single point and the correction is exact. Under heavy load the range is wide and it changes little, instead of writing a stale number. The read order in the snapshots can only widen the range, never narrow it.

**INC-6, reload ran over caps and reset dashboards.** `reload` built brand new cluster state with `running = 0`. In-flight queries were forgotten, so the new state admitted a full cap on top of them, which is over the cap. The dashboard showed 0. And when the old permits released, they looked their cluster up by name in the new config and decremented the new counter, which pushed it below zero.

The fix keeps a registry of every cluster ever configured, keyed by (group, name). Reload reuses the same `Arc<ClusterState>` for a cluster that stays, so counts, caps and metrics carry over. Permits hold their state directly, so they always release to the cluster they came from. Removed clusters are retired by setting their cap to 0, and re-added ones get their old state back with their in-flight count. The retire-and-rebuild happens under the write lock while `try_acquire` holds the read lock across its check and CAS, so no admission can see a half applied config. `try_acquire` never takes an exclusive lock.

Tests are in `tests/bonus.rs`. The reconcile ones are deterministic interleaving tests. A oneshot channel holds the "slow" engine call open while the test admits or releases queries, so the race happens in the same order every run. `inc6_cap_holds_while_reloading_under_load` runs 2,000 reloads against 8 threads and checks the cap is never exceeded.

The guarantees are these. A cluster never runs more permits than its cap at the moment of the check, across reloads too. A cluster's running count is never reset by a reload. Reconcile never moves the counter outside what the engine's answer allows, and never touches permit accounting.

## Before shipping, I would still want

- A `loom` model of the release and waiter handshake (the fence pairing) and of reload against try_acquire. The stress tests give me confidence but they don't prove it.
- Metrics for `drift` per cluster and `queued` per group. Big drift is the most useful alarm here, since it means someone is bypassing the gateway or a pod is leaking.
- Taking the RwLock off the hot path (arc-swap or a sharded reader count), since it is now the main point where cores contend.
- A decision on fairness. Barging is fine for throughput, but a long wait queue under steady load could starve unlucky waiters.
- A decision on `set_max_running` against reload. Right now the next reload resets the cap to the config value, which I think is correct (config is the source of truth), but the admin API should say so.
- The registry keeps every cluster ever configured, so admissions survive a remove and re-add. That's fine for a config of a few hundred clusters but would need pruning if cluster names are generated.
