#!/bin/sh
# Create a local PostgreSQL dump, encrypt/store it in the dedicated Restic
# repository, and apply the configured snapshot retention policy.
set -eu

here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$here/backup-lock.sh"
state_dir=${BACKUP_STATE_DIR:-/var/lib/agentux/command/backup-state}
stage_dir=${BACKUP_STAGING_DIR:-$state_dir/staging}
reported=0
finish() {
    rc=$?
    trap - EXIT HUP INT TERM
    if [ "$reported" -eq 0 ]; then
        "$here/backup-status.sh" backup failure || echo "backup-offsite: could not publish failure metrics" >&2
    fi
    [ -d "$stage_dir" ] && rm -f "$stage_dir"/agentux-command-*
    backup_lock_release
    exit "$rc"
}
trap finish EXIT
trap 'exit 1' HUP INT TERM

: "${RESTIC_REPOSITORY:?RESTIC_REPOSITORY is required}"
: "${RESTIC_PASSWORD_FILE:?RESTIC_PASSWORD_FILE is required}"
[ -s "$RESTIC_PASSWORD_FILE" ] || { echo "backup-offsite: Restic password file is missing or empty" >&2; exit 1; }
[ -r "$RESTIC_PASSWORD_FILE" ] || { echo "backup-offsite: Restic password file is unreadable" >&2; exit 1; }

keep_daily=${BACKUP_KEEP_DAILY:-7}
keep_weekly=${BACKUP_KEEP_WEEKLY:-4}
keep_monthly=${BACKUP_KEEP_MONTHLY:-3}
for value in "$keep_daily" "$keep_weekly" "$keep_monthly"; do
    case "$value" in ''|*[!0-9]*) echo "backup-offsite: retention values must be non-negative integers" >&2; exit 2 ;; esac
done
case "$keep_daily:$keep_weekly:$keep_monthly" in
    *[1-9]*) ;;
    *) echo "backup-offsite: at least one retention period must keep a snapshot" >&2; exit 2 ;;
esac
mkdir -p "$state_dir"
lock_file=$state_dir/workflow.lock
backup_lock_acquire "$lock_file"

mkdir -p "$stage_dir"
rm -f "$stage_dir"/agentux-command-*
if [ -n "${BACKUP_EVENT_ARCHIVE_DIR:-}" ]; then
    case "$BACKUP_EVENT_ARCHIVE_DIR" in
        /*) ;;
        *) echo "backup-offsite: BACKUP_EVENT_ARCHIVE_DIR must be an absolute path inside the Command container" >&2; exit 2 ;;
    esac
    mkdir -p "$stage_dir/event-archives"
    "${ENGINE:-podman}" cp \
        "${COMMAND_CONTAINER:-agentux-command}:$BACKUP_EVENT_ARCHIVE_DIR/." \
        "$stage_dir/event-archives/"
fi
dump=$(PG_CONTAINER="${PG_CONTAINER:-agentux-command-postgres}" \
    PG_DATABASE="${PG_DATABASE:-agentux}" PG_USER="${PG_USER:-agentux}" \
    ENGINE="${ENGINE:-podman}" "$here/backup.sh" "$stage_dir")
[ -s "$dump" ] && [ -s "$dump.sha256" ] || { echo "backup-offsite: dump or checksum is missing" >&2; exit 1; }

restic backup --group-by host,tags --tag agentux-command "$stage_dir"
restic forget --group-by host,tags --tag agentux-command \
    --keep-daily "$keep_daily" --keep-weekly "$keep_weekly" \
    --keep-monthly "$keep_monthly" --prune

"$here/backup-status.sh" backup success
reported=1
echo "AgentUX Command off-host backup completed"
