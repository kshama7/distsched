# Deploying distsched

This document is deliberately explicit about **what actually runs** versus **what
is a validated reference**, because conflating the two is exactly the kind of
résumé-padding this project avoids.

| Artifact                              | Status                                                              |
| ------------------------------------- | ------------------------------------------------------------------- |
| `deploy/compose/docker-compose.yml`   | **Runs.** A real 3-scheduler + 3-worker + Prometheus cluster you start with one command, on any machine with Docker. The chaos script drives it. |
| `scripts/chaos.sh`                    | **Runs.** Kills the leader under load and asserts a follower takes over and jobs complete. |
| `deploy/k8s/*.yaml`                   | **Reference, CI-validated.** Production-shaped manifests, schema-checked by `kubeconform` in CI on every push. They are **not** deployed to a standing cluster — running them needs a registry to push the image to and a real cluster, which this project does not maintain. |

CI (`.github/workflows/deploy-validate.yml`) builds the Docker image, validates
the Compose file (`docker compose config`), and lints the Kubernetes manifests,
so all three stay correct even though only the first two run on demand.

## Docker Compose (the live cluster)

```bash
docker compose -f deploy/compose/docker-compose.yml up --build
```

This starts:

- **scheduler-a/b/c** — a 3-node cluster that elects one leader; gRPC on host
  ports 7071/7072/7073, metrics on 9091/9092/9093.
- **worker-1/2/3** — workers seeded with all three schedulers; they discover and
  follow the leader.
- **prometheus** — scrapes all three schedulers, UI at http://localhost:9099.

Drive it with the CLI. Run `schedulerctl` **inside the cluster network** (the
image bundles it) so it resolves the schedulers' internal DNS and can follow the
leader — `compose exec` into the always-on `worker-1` container:

```bash
SEEDS=scheduler-a:7070,scheduler-b:7070,scheduler-c:7070
dctl() { docker compose -f deploy/compose/docker-compose.yml exec -T worker-1 schedulerctl --addr "$SEEDS" "$@"; }

dctl status
dctl submit --command sh --arg -c --arg 'echo hello; sleep 1'
dctl list
```

> The schedulers advertise their **internal** addresses (e.g. `scheduler-a:7070`)
> for peer election, so leader redirection only resolves from inside the network.
> The host port mappings (7071–7073, 9091–9093) are for scraping `/metrics` and
> poking a specific node, not for leader-following clients.

Tear down (including volumes):

```bash
docker compose -f deploy/compose/docker-compose.yml down -v
```

## Chaos / failover demo

```bash
./scripts/chaos.sh
```

It brings the cluster up, submits a batch of jobs, **kills the current leader
container**, waits for a new leader to be elected at a higher term, submits more
jobs, and confirms everything completes on the surviving cluster — then restarts
the killed node to show it rejoins as a follower.

## Kubernetes (reference)

The manifests model a production deployment:

- Schedulers are a **StatefulSet** (`scheduler-0/1/2`) behind a **headless
  Service**, so each has a stable DNS name for lease-based peer election, and a
  per-replica **PersistentVolume** for its BoltDB store.
- Workers are a stateless **Deployment** seeded with the three scheduler DNS
  names, with a 35s termination grace period so in-flight tasks drain.

To actually run them you must first build and push the image to a registry your
cluster can pull from, then update `image:` accordingly:

```bash
# build + push to your registry, then:
kubectl apply -f deploy/k8s/
```

This is provided as a reference; the project does not keep a cluster running.
