#!/bin/sh
# Backs up AgentUX Command: its PostgreSQL database, which holds all of its
# state (event log, projections, definitions, runs, nodes, enrollment,
# tokens, sessions, outbox). Writes <dir>/agentux-command-<UTC time>.dump
# (pg_dump custom format) and its .sha256, and prints the dump's path.
#
#   deploy/scripts/backup.sh /var/lib/agentux/command/backups
#
# ENGINE (podman) and PG_CONTAINER (agentux-command-postgres) say where the
# database runs. The dump holds token hashes and tenant data: encrypt it
# before it leaves the host (deploy/README.md, Backups).
set -eu

dir=${1:?usage: backup.sh <directory>}
engine=${ENGINE:-podman}
container=${PG_CONTAINER:-agentux-command-postgres}
database=${PG_DATABASE:-agentux}
user=${PG_USER:-agentux}

mkdir -p "$dir"
umask 077
stamp=$(date -u +%Y%m%dT%H%M%SZ)
dump="$dir/agentux-command-$stamp.dump"
# A consistent snapshot while Command keeps running.
"$engine" exec "$container" pg_dump --username "$user" --dbname "$database" \
    --format custom --no-owner >"$dump.partial"
mv "$dump.partial" "$dump"
(cd "$dir" && sha256sum "$(basename "$dump")" >"$(basename "$dump").sha256")
echo "$dump"
