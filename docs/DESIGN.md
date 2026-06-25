# distsched — Design

This document records the design decisions and, more importantly, the
**tradeoffs and their limits**. The goal of the project is to demonstrate
distributed-systems judgment, so where a corner is cut, this doc says so and
explains why the cut is safe for the stated scope.

## Goals

- Durably accept jobs and run them exactly the way the user asked: by priority,
  with retries, respecting DAG dependencies and schedules.
- Survive the failure of any single worker and any single scheduler without
  losing jobs or silently dropping work.
- Be small enough to read end-to-end and verify by running it.

## Non-goals

- Linearizable consensus / a hand-rolled Raft log (see below).
- Multi-tenancy, auth, quotas, or a UI.
- Horizontal scale benchmarks. Correctness and recoverability are the point.

## Leadership: why leases, not Raft

A multi-scheduler deployment needs exactly one node *dispatching* at a time,
otherwise the same job could be leased to two workers. The textbook answer is
Raft. I deliberately did **not** implement Raft, and want to be explicit about
why.

A from-scratch Raft implementation is very easy to ship in a state that *looks*
correct in a demo but is subtly broken — off-by-one in log truncation, a
missing persistence barrier before responding, an election-restriction edge
case. Verifying it genuinely requires randomized/Jepsen-style testing that is a
project in itself. Shipping a plausible-but-unverified consensus log would be
**dishonest engineering**: it signals more than it delivers.

### What we *did* implement

The tractable, well-understood, **testable half** of consensus: **Raft-style
leader election** (`internal/cluster`). It is "lease-based" in that leadership is
a renewable, time-bounded grant — a follower that stops hearing from the leader
for an election timeout campaigns for a new term.

- **Terms.** A monotonically increasing `term` is the logical clock and the
  fencing token. `currentTerm` and `votedFor` are persisted to BoltDB so a
  restart cannot grant two votes in one term.
- **Election.** On an election timeout (randomized, to avoid split votes) a
  follower becomes a **candidate**, increments its term, votes for itself, and
  requests votes from peers (`SchedulerService.RequestVote`). A peer grants at
  most one vote per term. A candidate that collects votes from a **majority**
  becomes leader.
- **Lease renewal.** The leader periodically sends `Replicate` to every follower
  (heartbeat + metadata). Each heartbeat resets the follower's election timer —
  this *is* the lease renewal. Miss them for an election timeout and a new term
  begins.
- **Fencing.** Every heartbeat and replication carries the leader's `term`. A
  node that sees a higher term steps down; a follower rejects a lower-term leader
  and returns its higher term, forcing the stale leader to step down.

**Safety:** at most one leader per term, because election needs a majority and
each member casts one vote per term. This is the same core as Raft/Chubby/etcd
leader election and as Kubernetes' lease-based election.

### What we deliberately did *not* implement — the honest part

We do **not** implement Raft's replicated-log commit machinery (quorum-committed,
linearizable log with truncation/snapshots/membership changes). That is the part
that is subtly-broken-by-default and needs Jepsen-grade testing to trust.

Consequently **replication is eager and best-effort, not quorum-committed.** The
leader keeps an in-memory log of metadata mutations (job/worker upserts), seeded
with a full snapshot when it takes office, and ships each follower the entries it
has not acked. Followers apply immediately — there is **no wait for a majority to
durably ack before the leader acts.** The implications, stated plainly:

- **At-least-once, never at-most-once.** A job acknowledged by a leader that dies
  before replicating it may not exist on the new leader and would be lost, or a
  task may run twice across a failover. We lean on **idempotent execution**
  (unique `task_id` per attempt; Milestone 5) so a double-dispatch wastes work
  rather than causing double side effects. Tasks with non-idempotent external
  effects are out of scope.
- **Split-brain window.** A partitioned old leader does not self-demote; it keeps
  acting until it next contacts a higher-term node. Its writes are fenced by
  `term` on the replication path, and only a majority partition can elect a new
  leader, so at most one leader can make *progress* — but the minority-side stale
  leader can still accept local writes that will be discarded. Conservative
  timing (heartbeat interval ≪ election timeout) keeps this window small.

If a deployment needs linearizable leadership and zero-loss failover, the right
move is to delegate the lease to a system that already solved consensus — etcd or
Consul — exactly as Kubernetes does. The election here is intentionally small and
isolated in `internal/cluster` so that swap is a contained change, not a rewrite.

## Persistence

Each scheduler embeds **BoltDB** (single-file, B+tree, fully ACID via mmap +
write-ahead). It is not a server, which suits an embedded control-plane store.

Buckets:

- `jobs` — `job_id → serialized Job` (source of truth).
- `workers` — `worker_id → serialized WorkerInfo`.
- `dead_letter` — jobs that exhausted retries.
- `meta` — election state (`election_term`, `election_voted_for`), persisted so a
  restart cannot grant two votes in one term.

The in-memory priority queue is always a cache over `jobs`. A node builds it only
when it becomes leader (`onBecomeLeader` → rebuild from the store); a follower
that wins an election therefore resumes from its replicated store — the
**checkpoint recovery** path hardened in Milestone 5. The queue is never the
system of record.

## Scheduling model

One mode, implemented fully:

- **Priority** — a binary heap keyed by `(−priority, created_at)`.
- **Retry/backoff** — on failure, attempt *n* is requeued after
  `min(max_backoff, initial_backoff · multiplier^(n−1))` perturbed by `±jitter`.
  After `max_attempts`, the job moves to the dead-letter queue.
- **DAG dependencies** — `depends_on` lists predecessor job IDs. A job is
  `PENDING` until all predecessors are `SUCCEEDED`, then transitions to
  `QUEUED`. Cycles are rejected at submit time (DFS back-edge check).
- **Schedules** — `run_at` (one-shot delayed) and `cron_expr` (recurring) hold a
  job `PENDING` until the fire time.

## Task dispatch and liveness

- Workers **register**, then **poll** for tasks (pull model) and **report**
  terminal results. Pull avoids needing inbound connectivity to workers.
- Workers **heartbeat** on a fixed interval. The leader marks a worker
  `SUSPECT` after one missed interval and `DEAD` after the eviction threshold,
  at which point its in-flight tasks are requeued (Milestone 5).
- Each dispatched attempt carries a **lease deadline**; if no terminal report
  arrives by then, the attempt is assumed lost and requeued.

## Failure model — what we defend against

| Failure                         | Response                                                        |
| ------------------------------- | -------------------------------------------------------------- |
| Worker crash mid-task           | Missed heartbeats → mark DEAD → requeue its in-flight attempts |
| Worker slow / network blip      | Lease deadline expiry → requeue; duplicate suppressed by `task_id` |
| Scheduler (follower) crash      | No effect on dispatch; rejoins and re-syncs                    |
| Scheduler (leader) crash        | Lease expires → follower acquires, bumps `term`, resumes from store |
| Stale leader returns from pause | Fenced by `term`; steps down                                   |
| Full cluster restart            | Each node reloads BoltDB; leader re-elected; queue rebuilt     |

## Observability (Milestone 7)

- **Prometheus** metrics: queue depth by state, dispatch/complete/fail/retry
  counters, dead-letter size, lease term and current role, worker liveness.
- **zap** structured logs with job/worker/task IDs and the lease term on every
  leadership transition.
- **`schedulerctl`**: submit/get/list/cancel jobs and `status` for cluster +
  lease view.

## Testing strategy

- Unit tests for the priority queue, backoff math, and DAG cycle detection.
- Integration tests that stand up an in-process scheduler + workers and assert
  end-to-end job completion, retry, and dead-lettering.
- A **chaos script** (Milestone 8) that kills the leader under load and asserts
  a follower takes over and outstanding jobs still complete — the headline
  demonstration that failover actually works.
