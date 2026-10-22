#!/bin/sh
# The restore drill (CI runs it): fills a Command database, backs it up,
# restores the backup into a fresh database, and checks that the restored
# one holds the same and that its projections match their event log.
#
#   deploy/scripts/restore-drill.sh target/debug/agentux-command
#
# Needs docker (or ENGINE=podman) and curl. Uses ports 55432, 55433 and 18080.
set -eu

bin=${1:?usage: restore-drill.sh <agentux-command binary>}
engine=${ENGINE:-docker}
export ENGINE=$engine
here=$(dirname "$0")
work=$(mktemp -d)
cleanup() {
    [ -n "${server:-}" ] && kill "$server" 2>/dev/null || true
    "$engine" rm -f drill-pg-a drill-pg-b >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

for name in a b; do
    port=$([ "$name" = a ] && echo 55432 || echo 55433)
    "$engine" run -d --name "drill-pg-$name" -e POSTGRES_DB=agentux -e POSTGRES_USER=agentux \
        -e POSTGRES_PASSWORD=drill -p "127.0.0.1:$port:5432" docker.io/library/postgres:16 >/dev/null
done
for name in a b; do
    for _ in $(seq 60); do
        "$engine" exec "drill-pg-$name" pg_isready -U agentux -d agentux >/dev/null 2>&1 && break
        sleep 1
    done
done
url_a=postgres://agentux:drill@127.0.0.1:55432/agentux?sslmode=disable
url_b=postgres://agentux:drill@127.0.0.1:55433/agentux?sslmode=disable

# Some state: a published definition, a node and its events.
AGENTUX_COMMAND_DATABASE_URL=$url_a AGENTUX_COMMAND_ADDR=127.0.0.1:18080 \
    AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1 AGENTUX_COMMAND_INSECURE_OPEN_NODES=1 \
    "$bin" >"$work/server.log" 2>&1 &
server=$!
for _ in $(seq 60); do
    curl --silent --fail http://127.0.0.1:18080/readyz >/dev/null && break
    sleep 1
done
api() { curl --silent --show-error --fail -H 'authorization: Bearer demo-leader' -H 'content-type: application/json' "$@"; }
api -X POST http://127.0.0.1:18080/v1/organizations/acme/definitions -d '{
  "schema_version": "1.0", "organization": {"id": "acme", "name": "Acme"},
  "areas": [], "roles": [], "missions": [], "workflows": [], "gates": [], "agents": [], "nodes": []}' >/dev/null
api -X POST http://127.0.0.1:18080/v1/organizations/acme/definitions/1/publish >/dev/null
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)
base='"protocolVersion": "v1", "organizationId": "acme", "nodeId": "node-1", "occurredAt": "'$now'"'
api -X POST http://127.0.0.1:18080/v1/protocol/messages -d '{"jsonrpc": "2.0", "id": "h", "method": "node.hello", "params": {'"$base"', "messageId": "m1",
  "capabilities": ["harness.list"], "agentuxVersion": "0.6.0", "supportedProtocolVersions": ["v1"]}}' >/dev/null
api -X POST http://127.0.0.1:18080/v1/protocol/messages -d '{"jsonrpc": "2.0", "method": "run.started", "params": {'"$base"', "messageId": "m2",
  "eventId": "e1", "sequence": 1, "runId": "r1", "title": "drill"}}' >/dev/null
kill "$server"
wait "$server" 2>/dev/null || true
server=

before=$(AGENTUX_COMMAND_DATABASE_URL=$url_a "$bin" check)
PG_CONTAINER=drill-pg-a "$here/backup.sh" "$work/backups" >"$work/dump-path"
PG_CONTAINER=drill-pg-b "$here/restore.sh" --yes "$(cat "$work/dump-path")"
after=$(AGENTUX_COMMAND_DATABASE_URL=$url_b "$bin" check)
echo "before: $before"
echo "after:  $after"
[ "$before" = "$after" ] || { echo "the restored database differs" >&2; exit 1; }
case "$after" in
    *'"events":0'*|*'"definitions":0'*) echo "the drill wrote nothing to back up" >&2; exit 1 ;;
esac
echo "restore drill passed"
