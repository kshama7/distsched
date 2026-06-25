# distsched

A distributed job scheduler in Go, built to exercise the core problems of
infrastructure systems: concurrency, gRPC networking, leader election, state
replication, and recovery from process and node failure.

[![ci](https://github.com/kshama7/distsched/actions/workflows/ci.yml/badge.svg)](https://github.com/kshama7/distsched/actions/workflows/ci.yml)

> **Status: under active construction.** This repo is being built in 8
> milestones (see [Roadmap](#roadmap)). Each milestone leaves `main` in a
> runnable, tested state. **Milestones 1–2 are complete**: the scheduler runs as
> a live gRPC server with BoltDB persistence and a priority queue, accepts jobs,
> and tracks workers via registration + heartbeat. Task dispatch and execution
> arrive in Milestone 3.

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

- **It does not implement Raft.** Leadership uses a documented, lease-based
  election (single fenced lease row with a monotonic term). This is easier to
  reason about and verify than a hand-rolled consensus log, at the cost of a
  bounded weaker-consistency window. The tradeoff — including what can be lost
  on failover and why that's acceptable here — is written up in
  [docs/DESIGN.md](docs/DESIGN.md#leadership-why-leases-not-raft). If you need
  linearizable consensus, run this against an external store (etcd) instead.
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
internal/             implementation packages
docs/DESIGN.md        design rationale and tradeoffs
```

## Quickstart

Requires Go ≥ 1.25. Regenerating protos additionally needs `protoc` plus the
pinned plugins (`make tools`).

```bash
make build              # compile scheduler, worker, schedulerctl into ./bin
make test               # go test -race ./...
make proto              # regenerate code from .proto (only if you edit protos)
```

Run a scheduler and a worker locally:

```bash
# terminal 1 — scheduler with a BoltDB store under ./data
./bin/scheduler --listen :7070 --data-dir ./data --log-level debug

# terminal 2 — a worker that registers and heartbeats
./bin/worker --scheduler localhost:7070 --capacity 8 --heartbeat-interval 2s
```

The scheduler logs worker registration and heartbeats, persists every job to
BoltDB, and rebuilds its priority queue from that store on restart. Until the
`schedulerctl` CLI lands (Milestone 7), submit jobs via the `JobService` gRPC
API (exercised end-to-end in `internal/scheduler/server_test.go`).

## Roadmap

| #  | Milestone                                                            | Status |
| -- | ------------------------------------------------------------------- | ------ |
| 1  | Repo scaffold + proto definitions (Job / Worker / Scheduler)        | ✅ done |
| 2  | Single scheduler: BoltDB persistence, priority queue, heartbeats    | ✅ done |
| 3  | Multi-worker: assignment, complete/failed, backoff retry, dead-letter | ⏳   |
| 4  | Multi-scheduler: lease election, metadata replication, failover     | ⏳     |
| 5  | Failure recovery: missed-heartbeat requeue, idempotency, checkpoint | ⏳     |
| 6  | DAG dependencies + delayed/cron jobs                                | ⏳     |
| 7  | Observability: Prometheus metrics, structured logs, `schedulerctl`  | ⏳     |
| 8  | Docker Compose cluster + chaos/failover demo + reference K8s        | ⏳     |

## License

[MIT](LICENSE)
