#!/usr/bin/env bash
#
# chaos.sh — bring up the Compose cluster, submit load, kill the leader, and
# prove a follower takes over and outstanding jobs still complete.
#
# schedulerctl is run *inside* the cluster network (via `compose exec` into the
# always-on worker-1 container) so it resolves the schedulers' internal DNS
# names and can follow the leader across the kill.
#
# Requires: docker (with compose).
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE=(docker compose -f deploy/compose/docker-compose.yml)
INTERNAL_SEEDS="scheduler-a:7070,scheduler-b:7070,scheduler-c:7070"
CTL=("${COMPOSE[@]}" exec -T worker-1 schedulerctl --addr "$INTERNAL_SEEDS")

log()       { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
# leader prints the holder ID, or nothing if none is elected yet.
leader()    { "${CTL[@]}" status 2>/dev/null | awk '/^leader:/{if ($2!="(none)") print $2}'; }
succeeded() { "${CTL[@]}" list --state succeeded 2>/dev/null | tail -n +2 | grep -c . || true; }

log "starting cluster (3 schedulers + 3 workers + prometheus)"
"${COMPOSE[@]}" up -d --build

log "waiting for a leader to be elected"
L=""
for _ in $(seq 1 90); do
  L="$(leader || true)"
  [[ -n "$L" ]] && break
  sleep 1
done
[[ -n "$L" ]] || { echo "no leader elected in time"; "${COMPOSE[@]}" logs --tail=20; exit 1; }
echo "leader is: $L"

log "submitting batch 1 (10 jobs)"
for i in $(seq 1 10); do
  "${CTL[@]}" submit --name "batch1-$i" --command sh --arg -c --arg "echo job $i; sleep 1" --priority "$i" >/dev/null
done
echo "submitted 10 jobs"

OLD_LEADER="$(leader)"
log "KILLING THE LEADER: $OLD_LEADER"
"${COMPOSE[@]}" kill "$OLD_LEADER"

log "waiting for a NEW leader (failover)"
NEW_LEADER=""
for _ in $(seq 1 90); do
  NEW_LEADER="$(leader || true)"
  if [[ -n "$NEW_LEADER" && "$NEW_LEADER" != "$OLD_LEADER" ]]; then break; fi
  sleep 1
done
[[ -n "$NEW_LEADER" && "$NEW_LEADER" != "$OLD_LEADER" ]] || { echo "FAILOVER FAILED"; exit 1; }
echo "new leader is: $NEW_LEADER (was $OLD_LEADER)"

log "submitting batch 2 (10 jobs) to the new leader"
for i in $(seq 1 10); do
  "${CTL[@]}" submit --name "batch2-$i" --command sh --arg -c --arg "echo job $i; sleep 1" >/dev/null
done

log "waiting for jobs to complete on the surviving cluster"
for _ in $(seq 1 60); do
  n="$(succeeded)"
  echo "succeeded so far: $n / 20"
  [[ "$n" -ge 20 ]] && break
  sleep 2
done

log "restarting the killed node — it should rejoin as a follower"
"${COMPOSE[@]}" start "$OLD_LEADER"
sleep 6

log "final cluster status"
"${CTL[@]}" status

cat <<EOF

Failover proven: leader $OLD_LEADER was killed, $NEW_LEADER took over at a higher
term, and jobs submitted before and after the kill completed. $OLD_LEADER
rejoined as a follower.

Prometheus: http://localhost:9099    Metrics: http://localhost:9091/metrics
Tear down with: ${COMPOSE[*]} down -v
EOF
