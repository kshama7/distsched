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

Instead leadership is **lease-based**:

- A single record holds `(holder_id, term, acquired_at, expires_at)`.
- To become leader, a node performs a guarded write: claim the record only if
  it is empty or expired. The winner increments `term`.
- The leader **renews** the lease well before `expires_at` (renew interval ≪
  lease TTL). If it stops renewing — crash, partition, GC pause — the lease
  expires and a follower acquires it on the next attempt.
- Every leader-authored side effect (replication, eventually external writes)
  carries the leader's `term`. A node that sees a higher term **steps down**;
  this fences a stale leader that wakes up from a pause.

This is the same primitive behind Kubernetes' `Lease`-based leader election and
Chubby/ZooKeeper lock-based leadership. It is small, easy to reason about, and
straightforward to test.

### What this costs — the honest part

Lease leadership gives **mutual exclusion that is correct only as far as clocks
and the fencing token make it**. The known weakness is the failover window:

- If a leader is partitioned or stalls (e.g. a long GC pause) but still believes
  it holds the lease, it may keep acting for up to roughly the lease TTL while a
  new leader is already elected. This is the classic split-brain window.
- distsched bounds the damage three ways rather than pretending it can't happen:
  1. **Fencing tokens (`term`)** make a stale leader's late writes rejectable —
     followers and the replication path refuse a lower term.
  2. **Idempotent task execution** (Milestone 5): each attempt has a unique
     `task_id`; a worker that receives a duplicate for an already-finished
     attempt drops it. Double-dispatch degrades to wasted work, not double
     side-effects, for idempotent tasks.
  3. **Conservative timing**: renew interval ≪ lease TTL ≪ heartbeat-eviction
     window, so a transient stall doesn't trigger failover.

What can still be lost: a job acknowledged by the old leader but not yet
replicated when it dies may need to be re-derived from the durable store on the
new leader. We accept *at-least-once* execution, never *at-most-once*, and lean
on idempotency. Tasks with non-idempotent external effects are out of scope.

If a deployment genuinely needs linearizable leadership, the lease record is
deliberately tiny and can be backed by etcd/Consul instead of the built-in
store — that swap is the supported escape hatch, not a rewrite.

## Persistence

Each scheduler embeds **BoltDB** (single-file, B+tree, fully ACID via mmap +
write-ahead). It is not a server, which suits an embedded control-plane store.

Planned buckets:

- `jobs` — `job_id → serialized Job` (source of truth).
- `queue_index` — ordering hints for the ready/priority queue, rebuildable from
  `jobs` on startup.
- `dead_letter` — jobs that exhausted retries.
- `lease` — the single leadership record.
- `meta` — schema version, last-applied replication seq.

On restart a scheduler reloads `jobs`, rebuilds the in-memory priority queue,
and resumes — the **checkpoint recovery** path (Milestone 5). The in-memory
queue is always a cache over the durable store, never the system of record.

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
