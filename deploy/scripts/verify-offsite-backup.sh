#!/bin/sh
# Check Restic data, restore the latest dump into a fresh PostgreSQL
# container, and run the deployed Command image's database consistency check.
set -eu

here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$here/backup-lock.sh"
state_dir=${BACKUP_STATE_DIR:-/var/lib/agentux/command/backup-state}
work=
verify_db=
verify_network=
reported=0
finish() {
    rc=$?
    trap - EXIT HUP INT TERM
    if [ -n "$verify_db" ]; then
        "${ENGINE:-podman}" rm -f "$verify_db" >/dev/null 2>&1 || true
    fi
    if [ -n "$verify_network" ]; then
        "${ENGINE:-podman}" network rm "$verify_network" >/dev/null 2>&1 || true
    fi
    if [ "$reported" -eq 0 ]; then
        "$here/backup-status.sh" restore_verification failure || echo "verify-offsite-backup: could not publish failure metrics" >&2
    fi
    [ -z "$work" ] || rm -rf "$work"
    backup_lock_release
    exit "$rc"
}
trap finish EXIT
trap 'exit 1' HUP INT TERM

: "${RESTIC_REPOSITORY:?RESTIC_REPOSITORY is required}"
: "${RESTIC_PASSWORD_FILE:?RESTIC_PASSWORD_FILE is required}"
[ -s "$RESTIC_PASSWORD_FILE" ] || { echo "verify-offsite-backup: Restic password file is missing or empty" >&2; exit 1; }
[ -r "$RESTIC_PASSWORD_FILE" ] || { echo "verify-offsite-backup: Restic password file is unreadable" >&2; exit 1; }

engine=${ENGINE:-podman}
command_container=${COMMAND_CONTAINER:-agentux-command}
postgres_container=${PG_CONTAINER:-agentux-command-postgres}
mkdir -p "$state_dir"
lock_file=$state_dir/workflow.lock
backup_lock_acquire "$lock_file"

work=$(mktemp -d "${TMPDIR:-/tmp}/agentux-command-restore-check.XXXXXX")
restic check --read-data
restic restore latest --target "$work/restore"
dump=$(find "$work/restore" -type f -name 'agentux-command-*.dump' -print -quit)
[ -n "$dump" ] || { echo "verify-offsite-backup: latest snapshot contains no Command database dump" >&2; exit 1; }
[ -s "$dump.sha256" ] || { echo "verify-offsite-backup: restored dump has no checksum sidecar" >&2; exit 1; }
(cd "$(dirname "$dump")" && sha256sum --check --quiet "$(basename "$dump").sha256")
if [ -n "${BACKUP_EVENT_ARCHIVE_DIR:-}" ]; then
    [ -d "$(dirname "$dump")/event-archives" ] || {
        echo "verify-offsite-backup: configured event archives are missing from the snapshot" >&2
        exit 1
    }
fi

command_image=$("$engine" inspect --format '{{.Image}}' "$command_container")
postgres_image=$("$engine" inspect --format '{{.Image}}' "$postgres_container")
[ -n "$command_image" ] && [ -n "$postgres_image" ] || { echo "verify-offsite-backup: could not identify the deployed container images" >&2; exit 1; }
verify_db=agentux-command-backup-verify-$$
verify_network=agentux-command-backup-verify-$$
"$engine" network create --internal "$verify_network" >/dev/null
"$engine" run --detach --name "$verify_db" --network "$verify_network" \
    --env POSTGRES_DB=agentux --env POSTGRES_USER=agentux \
    --env POSTGRES_HOST_AUTH_METHOD=trust "$postgres_image" >/dev/null

ready=
for _ in $(seq "${BACKUP_VERIFY_DB_START_ATTEMPTS:-60}"); do
    if "$engine" exec "$verify_db" pg_isready -h 127.0.0.1 -U agentux -d agentux >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
[ -n "$ready" ] || { echo "verify-offsite-backup: temporary PostgreSQL did not become ready" >&2; exit 1; }

PG_CONTAINER="$verify_db" PG_DATABASE=agentux PG_USER=agentux ENGINE="$engine" \
    "$here/restore.sh" --yes "$dump"

"$engine" run --rm --network "$verify_network" \
    --entrypoint /usr/local/bin/agentux-command \
    --env "AGENTUX_COMMAND_DATABASE_URL=postgres://agentux@$verify_db:5432/agentux?sslmode=disable" \
    "$command_image" check >/dev/null

"$here/backup-status.sh" restore_verification success
reported=1
echo "AgentUX Command off-host restore verification passed"
