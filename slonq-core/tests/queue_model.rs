//! This module contains a formal model of the Slonq queue system using `stateright`.
//!
//! `stateright` is a model checker that takes this definition and "explores" every possible
//! sequence of actions. If there is any path—no matter how convoluted—that leads to a state
//! where a property is violated, `stateright` will find it and show you the exact steps
//! to reproduce it.
//!
//! This model validates the core logic of our PostgreSQL queue:
//! - Visibility timeouts (jobs becoming visible again if not finished).
//! - Lease safety (only the current holder can ACK/NACK).
//! - Max attempt limits.
//! - Race conditions between concurrent workers.

use stateright::{Checker, Model, Property};
use std::collections::HashSet;

/// The possible statuses for a job in our model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Status {
    Pending,
    InProgress,
    Done,
    Failed,
}

/// A simplified representation of a job in the queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Job {
    idempotency_key: u8,
    status: Status,
    /// The time at which this job becomes visible to workers.
    visible_at: u8,
    /// How many times this job has been dequeued.
    attempt_count: u8,
    /// How long a lease lasts for this job.
    lease_timeout: u8,

    /// The ID of the current lease (if any).
    lease_id: Option<u8>,
    /// The ID of the worker currently holding the lease (if any).
    leased_by: Option<u8>,

    // Fields used for verifying correctness:
    /// Which attempt successfully completed the job.
    completed_attempt: Option<u8>,
    /// Which attempt caused the job to fail.
    failed_attempt: Option<u8>,
}

/// The local memory of a worker about a lease it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct LeaseMem {
    job: u8,
    lease_id: u8,
    attempt_count: u8,
}

/// A worker that can interact with the queue.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Worker {
    /// The lease information the worker currently remembers.
    held: Option<LeaseMem>,
}

/// The global state of our modeled world.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    /// Monotonic "logical clock" to model passage of time.
    now: u8,
    /// Generator for unique lease IDs.
    next_lease_id: u8,
    /// The set of jobs in the queue.
    jobs: Vec<Option<Job>>,
    /// The set of workers in the system.
    workers: Vec<Worker>,
}

/// Actions that can cause a state transition.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Action {
    /// Time passes (increment `now`).
    Tick,
    /// A worker dequeues a specific job.
    Dequeue { worker: u8, job: u8 },
    /// A worker attempts to ACK its current lease.
    Ack { worker: u8 },
    /// A worker attempts to NACK its current lease.
    Nack { worker: u8 },
    /// A worker attempts to extend its lease.
    Touch { worker: u8 },
    /// A worker crashes, losing its local memory but not affecting the global DB state.
    Crash { worker: u8 },
    /// Enqueue a job with a given idempotency key.
    Enqueue { key: u8 },
}

/// Parameters for the model.
#[derive(Clone)]
struct QueueModel {
    /// Number of workers to simulate.
    workers: u8,
    /// Number of jobs to simulate.
    jobs: u8,
    /// Maximum allowed attempts before a job is considered failed.
    max_attempts: u8,
    /// Time limit for the simulation (to keep the search space finite).
    max_time: u8,
}

impl QueueModel {
    fn new(workers: u8, jobs: u8, max_attempts: u8, max_time: u8) -> Self {
        Self {
            workers,
            jobs,
            max_attempts,
            max_time,
        }
    }

    fn job_eligible(&self, st: &State, j: u8) -> bool {
        let Some(job) = &st.jobs[j as usize] else {
            return false;
        };
        let non_terminal = matches!(job.status, Status::Pending | Status::InProgress);
        non_terminal && job.visible_at <= st.now && job.attempt_count < self.max_attempts
    }

    fn reap_exhausted(&self, st: &mut State) {
        for job_opt in &mut st.jobs {
            if let Some(job) = job_opt {
                if job.status == Status::InProgress
                    && job.visible_at <= st.now
                    && job.attempt_count >= self.max_attempts
                {
                    job.status = Status::Failed;
                    job.failed_attempt = Some(job.attempt_count);
                    job.lease_id = None;
                    job.leased_by = None;
                }
            }
        }
    }
}

impl Model for QueueModel {
    type State = State;
    type Action = Action;

    /// Defines the starting state(s) of the system.
    fn init_states(&self) -> Vec<Self::State> {
        let jobs = vec![None; self.jobs as usize];

        let workers = (0..self.workers).map(|_| Worker { held: None }).collect();

        vec![State {
            now: 0,
            next_lease_id: 1,
            jobs,
            workers,
        }]
    }

    /// Given a current state, returns all possible actions that can be taken.
    /// `stateright` will explore every action returned here.
    fn actions(&self, st: &Self::State, actions: &mut Vec<Self::Action>) {
        // We can always advance time, up to our simulation limit.
        if st.now < self.max_time {
            actions.push(Action::Tick);
        }

        // Idempotent Enqueue actions.
        // We model a small set of keys (e.g. 0 and 1) to explore idempotency.
        for key in 0..2 {
            actions.push(Action::Enqueue { key });
        }

        for w in 0..self.workers {
            let widx = w as usize;

            if st.workers[widx].held.is_none() {
                // An idle worker can try to dequeue any eligible job.
                for j in 0..self.jobs {
                    if self.job_eligible(st, j) {
                        actions.push(Action::Dequeue { worker: w, job: j });
                    }
                }
            } else {
                // A worker holding a lease can try to ACK, NACK, TOUCH, or it might CRASH.
                actions.push(Action::Ack { worker: w });
                actions.push(Action::Nack { worker: w });
                actions.push(Action::Touch { worker: w });
                actions.push(Action::Crash { worker: w });
            }
        }
    }

    /// Defines how the state changes when a specific action is taken.
    /// This is where the core business logic of the queue is modelled.
    fn next_state(&self, st: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut st2 = st.clone();

        match action {
            Action::Tick => {
                st2.now = st2.now.saturating_add(1);
                Some(st2)
            }

            Action::Enqueue { key } => {
                // Does nothing if key already exists
                if st2.jobs.iter().any(|j| {
                    j.as_ref()
                        .map(|j| j.idempotency_key == key)
                        .unwrap_or(false)
                }) {
                    return Some(st2);
                }

                // Otherwise inserts into the first empty slot
                if let Some(slot) = st2.jobs.iter_mut().find(|s| s.is_none()) {
                    *slot = Some(Job {
                        idempotency_key: key,
                        status: Status::Pending,
                        visible_at: st2.now,
                        attempt_count: 0,
                        lease_timeout: 2 + key, // model some variation
                        lease_id: None,
                        leased_by: None,
                        completed_attempt: None,
                        failed_attempt: None,
                    });
                    Some(st2)
                } else {
                    // No empty slot, do nothing (or we could return None to say action not possible,
                    // but "do nothing" usually means stay in same state or just not possible)
                    // Given it's a model of bounded capacity, "do nothing" on full is reasonable.
                    Some(st2)
                }
            }

            Action::Dequeue { worker, job } => {
                let widx = worker as usize;
                if st2.workers[widx].held.is_some() {
                    return None; // Worker already has a job
                }

                // In Slonq, the dequeue SQL query also "reaps" jobs that have
                // timed out and exceeded max attempts. We model this atomic behaviour here.
                self.reap_exhausted(&mut st2);

                if !self.job_eligible(&st2, job) {
                    return None; // Job was taken by someone else or is not ready
                }

                let jidx = job as usize;
                let lease_id = st2.next_lease_id;
                st2.next_lease_id = st2.next_lease_id.saturating_add(1);

                let jobref = st2.jobs[jidx].as_mut()?;
                jobref.status = Status::InProgress;
                jobref.attempt_count = jobref.attempt_count.saturating_add(1);
                // Set the visibility timeout:
                jobref.visible_at = st2.now.saturating_add(jobref.lease_timeout);
                jobref.lease_id = Some(lease_id);
                jobref.leased_by = Some(worker);

                // Worker updates its local memory:
                st2.workers[widx].held = Some(LeaseMem {
                    job,
                    lease_id,
                    attempt_count: jobref.attempt_count,
                });

                Some(st2)
            }

            Action::Ack { worker } => {
                let widx = worker as usize;
                let lease = st2.workers[widx].held?;

                let job = st2.jobs[lease.job as usize].as_mut()?;
                // CRITICAL SAFETY CHECK: Slonq only allows ACK if the lease_id matches.
                // This prevents a worker from ACKing a job it lost due to timeout.
                if job.status == Status::InProgress && job.lease_id == Some(lease.lease_id) {
                    job.status = Status::Done;
                    job.completed_attempt = Some(lease.attempt_count);
                    job.lease_id = None;
                    job.leased_by = None;
                }

                // Worker always drops its local memory of the lease after attempting ACK.
                st2.workers[widx].held = None;
                Some(st2)
            }

            Action::Nack { worker } => {
                let widx = worker as usize;
                let lease = st2.workers[widx].held?;

                let job = st2.jobs[lease.job as usize].as_mut()?;
                // Safety check for lease matching, same as ACK.
                if job.status == Status::InProgress && job.lease_id == Some(lease.lease_id) {
                    if job.attempt_count >= self.max_attempts {
                        job.status = Status::Failed;
                        job.failed_attempt = Some(lease.attempt_count);
                    } else {
                        job.status = Status::Pending;
                        job.visible_at = st2.now; // Make it immediately available again
                    }
                    job.lease_id = None;
                    job.leased_by = None;
                }

                st2.workers[widx].held = None;
                Some(st2)
            }

            Action::Touch { worker } => {
                let widx = worker as usize;
                let lease = st2.workers[widx].held?;

                let job = st2.jobs[lease.job as usize].as_mut()?;
                // Safety check for lease matching.
                if job.status == Status::InProgress && job.lease_id == Some(lease.lease_id) {
                    // Extend the timeout.
                    job.visible_at = st2.now.saturating_add(job.lease_timeout);
                } else {
                    // If touch fails (lease lost), the worker realizes it and drops it.
                    st2.workers[widx].held = None;
                }
                Some(st2)
            }

            Action::Crash { worker } => {
                let widx = worker as usize;
                // Worker loses its local memory, but nothing changes in the "database".
                st2.workers[widx].held = None;
                Some(st2)
            }
        }
    }

    /// Defines the invariants that must hold true in EVERY state.
    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "attempt_count never exceeds max_attempts",
                prop_attempt_leq_max,
            ),
            Property::always(
                "in_progress implies lease_id and leased_by are set",
                prop_inprogress_has_lease,
            ),
            Property::always(
                "done/failed imply lease fields cleared",
                prop_terminal_clears_lease,
            ),
            Property::always(
                "done attempt matches final attempt_count",
                prop_done_attempt_matches,
            ),
            Property::always(
                "failed attempt matches final attempt_count",
                prop_failed_attempt_matches,
            ),
            Property::always("active lease_ids are unique", prop_unique_active_lease_ids),
            Property::always("no job has two CURRENT owners", prop_no_two_current_owners),
            Property::always(
                "no duplicate idempotency keys among jobs",
                prop_unique_idempotency_keys,
            ),
        ]
    }

    /// Optimisation: stop exploring if we exceed the time limit.
    fn within_boundary(&self, st: &Self::State) -> bool {
        st.now <= self.max_time
    }
}

// ---------- Properties (must be fn pointers, not capturing closures) ----------

/// INVARIANT: The number of attempts for any job should never exceed the configured maximum.
fn prop_attempt_leq_max(m: &QueueModel, st: &State) -> bool {
    st.jobs
        .iter()
        .flatten()
        .all(|j| j.attempt_count <= m.max_attempts)
}

/// INVARIANT: Any job marked as 'InProgress' MUST have a lease assigned to it.
fn prop_inprogress_has_lease(_: &QueueModel, st: &State) -> bool {
    st.jobs.iter().flatten().all(|j| {
        if j.status == Status::InProgress {
            j.lease_id.is_some() && j.leased_by.is_some()
        } else {
            true
        }
    })
}

/// INVARIANT: Once a job is terminal ('Done' or 'Failed'), its lease fields must be cleared.
fn prop_terminal_clears_lease(_: &QueueModel, st: &State) -> bool {
    st.jobs.iter().flatten().all(|j| {
        if matches!(j.status, Status::Done | Status::Failed) {
            j.lease_id.is_none() && j.leased_by.is_none()
        } else {
            true
        }
    })
}

/// INVARIANT: If a job is marked 'Done', we ensure that it was completed by the correct attempt.
/// This guards against "stale ACK" bugs where a worker might accidentally ACK a job
/// that was already reassigned to someone else.
fn prop_done_attempt_matches(_: &QueueModel, st: &State) -> bool {
    st.jobs.iter().flatten().all(|j| {
        if j.status == Status::Done {
            j.completed_attempt == Some(j.attempt_count)
        } else {
            true
        }
    })
}

/// INVARIANT: If a job is marked 'Failed', we ensure it happened at the expected attempt count.
fn prop_failed_attempt_matches(_: &QueueModel, st: &State) -> bool {
    st.jobs.iter().flatten().all(|j| {
        if j.status == Status::Failed {
            // failed via nack or via reap; either way we record attempt_count at failure
            j.failed_attempt == Some(j.attempt_count)
        } else {
            true
        }
    })
}

/// INVARIANT: Every active lease in the system must have a unique ID.
fn prop_unique_active_lease_ids(_: &QueueModel, st: &State) -> bool {
    let mut seen = HashSet::new();
    for job in st.jobs.iter().flatten() {
        if let Some(l) = job.lease_id {
            if !seen.insert(l) {
                return false;
            }
        }
    }
    true
}

/// INVARIANT: No job can be owned by two different workers at the same time.
/// Note that "stale" leases (where a worker THINKS it owns a job but actually lost it)
/// are allowed, but the "source of truth" (the jobs table) must never show two owners.
fn prop_no_two_current_owners(_: &QueueModel, st: &State) -> bool {
    // For each job, count how many workers hold a lease that matches the job's CURRENT lease_id.
    for (jidx, job_opt) in st.jobs.iter().enumerate() {
        let Some(job) = job_opt else { continue };
        let Some(cur) = job.lease_id else { continue };
        let mut owners = 0;
        for w in &st.workers {
            if let Some(mem) = w.held {
                if mem.job as usize == jidx && mem.lease_id == cur {
                    owners += 1;
                    if owners > 1 {
                        return false;
                    }
                }
            }
        }
    }
    true
}

/// INVARIANT: No two jobs in the queue should have the same idempotency key.
fn prop_unique_idempotency_keys(_: &QueueModel, st: &State) -> bool {
    let mut seen = HashSet::new();
    for job in st.jobs.iter().flatten() {
        if !seen.insert(job.idempotency_key) {
            return false;
        }
    }
    true
}

#[test]
fn queue_model_safety_holds() {
    // Small parameters but they are enough to explore a massive state space
    // of interleavings, timeouts, and crashes.
    let model = QueueModel::new(
        2, // workers
        2, // jobs
        2, // max_attempts
        6, // max_time ticks
    );

    // Standard Stateright pattern: spawn BFS, join, assert properties.
    // BFS (Breadth-First Search) ensures we find the shortest possible bug trace.
    model.checker().spawn_bfs().join().assert_properties();
}
