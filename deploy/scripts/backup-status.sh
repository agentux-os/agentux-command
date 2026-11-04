#!/bin/sh
# Persist backup/verification health and atomically publish node_exporter
# textfile metrics. This file contains timestamps and counters only.
set -eu
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$here/backup-lock.sh"

if [ "$#" -ne 2 ]; then
    echo "usage: backup-status.sh <backup|restore_verification> <success|failure>" >&2
    exit 2
fi

operation=$1
result=$2
case "$operation:$result" in
    backup:success|backup:failure|restore_verification:success|restore_verification:failure) ;;
    *) echo "backup-status.sh: unsupported operation or result" >&2; exit 2 ;;
esac

state_dir=${BACKUP_STATE_DIR:-/var/lib/agentux/command/backup-state}
metrics_file=${BACKUP_METRICS_FILE:-/var/lib/node_exporter/textfile_collector/agentux-command-backup.prom}
mkdir -p "$state_dir" "$(dirname "$metrics_file")"
umask 022
backup_lock_acquire "$state_dir/status.lock"
trap backup_lock_release EXIT

read_number() {
    value=0
    if [ -r "$1" ]; then
        IFS= read -r value <"$1" || true
    fi
    case "$value" in
        ''|*[!0-9]*) value=0 ;;
    esac
    printf '%s' "$value"
}

write_number() {
    printf '%s\n' "$2" >"$1.tmp.$$"
    mv -f "$1.tmp.$$" "$1"
}

success_file=$state_dir/$operation-last-success
failures_file=$state_dir/$operation-failures
last_success=$(read_number "$success_file")
failures=$(read_number "$failures_file")

if [ "$result" = success ]; then
    last_success=$(date -u +%s)
    write_number "$success_file" "$last_success"
else
    failures=$((failures + 1))
    write_number "$failures_file" "$failures"
fi

tmp=$metrics_file.tmp.$$
cleanup() {
    rm -f "$tmp"
    backup_lock_release
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
{
    printf '# HELP agentux_command_backup_last_success_timestamp_seconds Unix timestamp of the last successful off-host backup.\n'
    printf '# TYPE agentux_command_backup_last_success_timestamp_seconds gauge\n'
    printf 'agentux_command_backup_last_success_timestamp_seconds %s\n' "$(read_number "$state_dir/backup-last-success")"
    printf '# HELP agentux_command_backup_failures_total Failed off-host backup attempts.\n'
    printf '# TYPE agentux_command_backup_failures_total counter\n'
    printf 'agentux_command_backup_failures_total %s\n' "$(read_number "$state_dir/backup-failures")"
    printf '# HELP agentux_command_restore_verification_last_success_timestamp_seconds Unix timestamp of the last successful restore verification.\n'
    printf '# TYPE agentux_command_restore_verification_last_success_timestamp_seconds gauge\n'
    printf 'agentux_command_restore_verification_last_success_timestamp_seconds %s\n' "$(read_number "$state_dir/restore_verification-last-success")"
    printf '# HELP agentux_command_restore_verification_failures_total Failed restore verification attempts.\n'
    printf '# TYPE agentux_command_restore_verification_failures_total counter\n'
    printf 'agentux_command_restore_verification_failures_total %s\n' "$(read_number "$state_dir/restore_verification-failures")"
} >"$tmp"
chmod 0644 "$tmp"
mv -f "$tmp" "$metrics_file"
