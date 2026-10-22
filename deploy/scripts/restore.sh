#!/bin/sh
# Restores a backup from deploy/scripts/backup.sh into Command's database,
# replacing what is there. Stop Command first (systemctl stop
# agentux-command), then start it and run `agentux-command check`.
#
#   deploy/scripts/restore.sh --yes /var/lib/agentux/command/backups/agentux-command-<time>.dump
#
# ENGINE, PG_CONTAINER, PG_DATABASE and PG_USER as for backup.sh.
set -eu

if [ "${1:-}" != "--yes" ] || [ -z "${2:-}" ]; then
    echo "usage: restore.sh --yes <dump>   (replaces the database's contents)" >&2
    exit 2
fi
dump=$2
engine=${ENGINE:-podman}
container=${PG_CONTAINER:-agentux-command-postgres}
database=${PG_DATABASE:-agentux}
user=${PG_USER:-agentux}

if [ -f "$dump.sha256" ]; then
    (cd "$(dirname "$dump")" && sha256sum --check --quiet "$(basename "$dump").sha256")
else
    echo "restore.sh: no $dump.sha256; the dump's integrity is not checked" >&2
fi
"$engine" exec -i "$container" pg_restore --username "$user" --dbname "$database" \
    --clean --if-exists --no-owner --exit-on-error --single-transaction <"$dump"
echo "restored $dump into $database; start Command and run: agentux-command check"
