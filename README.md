# distsched

A distributed job scheduler in Go, built to exercise the core problems of
infrastructure systems: concurrency, gRPC networking, leader election, state
replication, and recovery from process and node failure.

[![ci](https://github.com/kshama7/distsched/actions/workflows/ci.yml/badge.svg)](https://github.com/kshama7/distsched/actions/workflows/ci.yml)
[![deploy-validate](https://github.com/kshama7/distsched/actions/workflows/deploy-validate.yml/badge.svg)](https://github.com/kshama7/distsched/actions/workflows/deploy-validate.yml)

> **Status: feature-complete across all 8 milestones** (see [Roadmap](#roadmap)).
> A cluster of schedulers elects a leader (Raft-style majority vote with term
> fencing), the leader dispatches tasks to workers that execute them as
> subprocesses, failures are retried with backoff or dead-lettered, and metadata
> is replicated to followers so a new leader takes over on failover. A reaper
> requeues the work of dead workers and expired leases, orphaned jobs are
> reclaimed on restart, and execution is idempotent at-least-once. Jobs can
> declare **DAG dependencies** (cycles rejected at submit) and **delayed or
> recurring cron schedules**. Workers auto-discover and follow the leader.
> Operability is covered by **Prometheus metrics**, **zap structured logging**,
> and the **`schedulerctl` CLI**. A **Docker Compose** cluster and a **chaos
> script** demonstrate failover; **Kubernetes** manifests are a CI-validated
> reference. See [docs/DEPLOY.md](docs/DEPLOY.md).

## Engineering highlights

- **Leader election from first principles** — terms, randomized timeouts,
  majority voting, and lease renewal in a standalone, unit-tested
  [`internal/cluster`](internal/cluster) package; covered by race-clean
  election / failover / no-majority tests.
- **Correct under failure** — at-least-once idempotent execution, fenced stale
  leaders, dead-worker and expired-lease requeue, and checkpoint recovery of
  orphaned jobs — proven by a Docker **chaos test that kills the leader under
  load** and watches a follower finish the work.
- **Honest engineering** — it implements the tractable half of consensus
  (election) and *documents what it deliberately does not do* (Raft's replicated
  log) and what can be lost on failover, instead of overclaiming.
- **End-to-end Go infrastructure** — gRPC + Protobuf, embedded BoltDB,
  Prometheus metrics, zap structured logging, a `schedulerctl` CLI, a Docker
  Compose cluster, and CI-validated Kubernetes manifests.

## What this is

`distsched` accepts jobs over gRPC, persists them durably, and dispatches them
to a pool of workers. A small cluster of scheduler replicas coordinates through
a **lease-based leader**: exactly one scheduler dispatches at a time, and a
follower takes over within a lease interval if the leader dies.

The scheduling model is deliberately narrow but complete:

- **Priority queue** — higher-priority jobs run first; FIFO within a band.
- **Retry with exponential backoff + jitter** — failed jobs are retried up to a
  configurable limit, then moved to a **dead-letter queue**.
- **DAG dependencies** — a job can declare `depends_on` predecessors and only
  becomes eligible once they succeed.
- **Delayed and cron jobs** — run-at-a-time and recurring schedules.

## What this is *not* (honest scope)

These are deliberate cuts, not omissions I'm hiding:

- **It implements Raft-style leader *election*, not Raft's replicated log.**
  Leadership is a renewable lease decided by terms + randomized timeouts +
  majority vote (the well-understood, testable half of consensus). The log-commit
  machinery — the subtly-broken-by-default part — is deliberately omitted:
  metadata replication is eager and best-effort, and we rely on idempotent,
  at-least-once execution. Exactly what can be lost on failover, and why that's
  acceptable here, is written up in
  [docs/DESIGN.md](docs/DESIGN.md#leadership-why-leases-not-raft). If you need
  linearizable, zero-loss failover, delegate the lease to etcd/Consul (as K8s
  does); the election is isolated in `internal/cluster` to make that swap small.
- **One scheduling mode, done well**, rather than seven half-finished ones.
- **No dashboard.** Observability is Prometheus metrics + structured logs + a
  `schedulerctl` CLI.
- **No published benchmarks** unless/until they're real and reproducible.
- The Kubernetes manifests (Milestone 8) are a **CI-validated reference
  architecture**, not a cluster that runs 24/7. The Docker Compose stack is the
  demo that actually runs.

## Architecture

```
                 ┌────────────── clients (schedulerctl / gRPC) ──────────────┐
                 │                         JobService                          │
                 ▼                                                             ▼
        ┌─────────────────┐   lease / replication (SchedulerService)  ┌─────────────────┐
        │  scheduler  A    │◀────────────────────────────────────────▶│  scheduler  B/C  │
        │  (LEADER)        │                                           │  (FOLLOWERS)     │
        │  • priority queue│                                           │  • warm replica  │
        │  • BoltDB store  │                                           │    of metadata   │
        └────────┬─────────┘                                           └─────────────────┘
                 │ WorkerService (register / heartbeat / poll / report)
        ┌────────┴───────────────┬───────────────────────┐
        ▼                        ▼                        ▼
   ┌─────────┐             ┌─────────┐              ┌─────────┐
   │ worker 1│             │ worker 2│              │ worker 3│
   └─────────┘             └─────────┘              └─────────┘
```

Three gRPC services define the system (see [`proto/distsched/v1`](proto/distsched/v1)):

| Service            | Plane                | Responsibility                                            |
| ------------------ | -------------------- | --------------------------------------------------------- |
| `JobService`       | client → scheduler   | submit / get / list / cancel jobs                         |
| `WorkerService`    | worker ↔ scheduler   | register, heartbeat, pull tasks, report success/failure   |
| `SchedulerService` | scheduler ↔ scheduler| cluster status, leader→follower metadata replication      |

Workers **pull** work rather than being pushed to, so no worker-side server or
inbound connectivity is required.

## Layout

```
proto/distsched/v1/   API definitions (.proto)
gen/go/distsched/v1/  generated gRPC + message code (committed)
cmd/scheduler/        scheduler node binary
cmd/worker/           worker node binary
cmd/schedulerctl/     operator CLI
internal/             implementation packages (cluster, scheduler, worker,
                      store, queue, retry, metrics, ctl, logging)
deploy/compose/       runnable Docker Compose cluster
deploy/k8s/           Kubernetes reference manifests (CI-validated)
scripts/chaos.sh      kill-the-leader failover demo
docs/DESIGN.md        design rationale and tradeoffs
docs/DEPLOY.md        what runs vs. what is a validated reference
```

## Quickstart

Requires Go ≥ 1.25. Regenerating protos additionally needs `protoc` plus the
pinned plugins (`make tools`).

```bash
make build              # compile scheduler, worker, schedulerctl into ./bin
make test               # go test -race ./...
make proto              # regenerate code from .proto (only if you edit protos)
```

Run a scheduler, a worker, and submit a job locally:

```bash
# terminal 1 — scheduler with a BoltDB store under ./data
./bin/scheduler --listen :7070 --data-dir ./data

# terminal 2 — a worker that follows the leader and executes tasks
./bin/worker --schedulers localhost:7070 --capacity 8

# terminal 3 — submit work and watch it run
./bin/schedulerctl --addr localhost:7070 submit --command echo --arg hello --priority 5
./bin/schedulerctl --addr localhost:7070 list
```

The scheduler persists every job to BoltDB, dispatches by priority, retries
failures with exponential backoff, and dead-letters jobs that exhaust their
retries; the worker pulls tasks and runs them as subprocesses.

Run a 3-node cluster with leader election and failover:

```bash
PEERS=node-0=localhost:7101,node-1=localhost:7102,node-2=localhost:7103
./bin/scheduler --node-id node-0 --listen :7101 --peers $PEERS --data-dir ./data/0 &
./bin/scheduler --node-id node-1 --listen :7102 --peers $PEERS --data-dir ./data/1 &
./bin/scheduler --node-id node-2 --listen :7103 --peers $PEERS --data-dir ./data/2 &

# the worker is given all three as seeds and follows whichever is leader
./bin/worker --schedulers localhost:7101,localhost:7102,localhost:7103
```

The nodes elect one leader; only the leader accepts writes and dispatches, and
followers redirect clients to it. Kill the leader and a follower takes over
within an election timeout, resuming from replicated state — the scripted
demonstration of this is Milestone 8's chaos test.

Drive the cluster with the CLI and scrape metrics:

```bash
schedulerctl --addr localhost:7101,localhost:7102,localhost:7103 status
schedulerctl --addr localhost:7101 submit --name hello --command echo --arg world --priority 5
schedulerctl --addr localhost:7101 submit --command echo --arg load --depends-on extract
schedulerctl --addr localhost:7101 submit --command echo --cron "@every 30s"
schedulerctl --addr localhost:7101 list
curl -s localhost:9090/metrics | grep distsched_   # Prometheus metrics
```

### The demo cluster + chaos test (Docker)

Bring up a real cluster (3 schedulers + 3 workers + Prometheus) and prove
failover by killing the leader under load:

```bash
docker compose -f deploy/compose/docker-compose.yml up --build   # start the cluster
./scripts/chaos.sh                                               # kill the leader; a follower takes over
```

The chaos script submits jobs, kills the leader container, waits for a new leader
at a higher term, submits more jobs, and confirms everything completes — then
restarts the killed node to show it rejoin as a follower. See
[docs/DEPLOY.md](docs/DEPLOY.md) for what runs versus what is a validated
reference.

## Roadmap

| #  | Milestone                                                            | Status |
| -- | ------------------------------------------------------------------- | ------ |
| 1  | Repo scaffold + proto definitions (Job / Worker / Scheduler)        | ✅ done |
| 2  | Single scheduler: BoltDB persistence, priority queue, heartbeats    | ✅ done |
| 3  | Multi-worker: assignment, complete/failed, backoff retry, dead-letter | ✅ done |
| 4  | Multi-scheduler: lease election, metadata replication, failover     | ✅ done |
| 5  | Failure recovery: missed-heartbeat requeue, idempotency, checkpoint | ✅ done |
| 6  | DAG dependencies + delayed/cron jobs                                | ✅ done |
| 7  | Observability: Prometheus metrics, structured logs, `schedulerctl`  | ✅ done |
| 8  | Docker Compose cluster + chaos/failover demo + reference K8s        | ✅ done |

## License

[MIT](LICENSE)
