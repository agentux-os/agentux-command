#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
export BACKUP_STATE_DIR="$work/state"
export BACKUP_METRICS_FILE="$work/metrics/agentux-command-backup.prom"
export BACKUP_STAGING_DIR="$work/staging"
export FAKE_RESTIC_SNAPSHOT="$work/repository"
export FAKE_COMMAND_LOG="$work/commands.log"
export FAKE_ARCHIVE_SOURCE="$work/archive-source"
export BACKUP_EVENT_ARCHIVE_DIR=/var/lib/agentux-command/archive
export RESTIC_REPOSITORY=s3:https://backup.example.invalid/agentux-command
export RESTIC_PASSWORD_FILE="$work/restic-password"
export ENGINE=podman
export PATH="$work/bin:$PATH"

mkdir -p "$work/bin" "$FAKE_RESTIC_SNAPSHOT" "$FAKE_ARCHIVE_SOURCE"
printf 'archived event fixture\n' >"$FAKE_ARCHIVE_SOURCE/events.jsonl"
printf 'test-only-password\n' >"$RESTIC_PASSWORD_FILE"

cat >"$work/bin/restic" <<'MOCK_RESTIC'
#!/bin/sh
set -eu
command=$1
shift
case "$command" in
    backup)
        [ "${FAKE_RESTIC_BACKUP_FAIL:-0}" = 0 ] || exit 1
        rm -rf "$FAKE_RESTIC_SNAPSHOT/latest"
        mkdir -p "$FAKE_RESTIC_SNAPSHOT/latest"
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --tag|--group-by) shift 2 ;;
                --*) shift ;;
                *)
                    [ "$1" = "$BACKUP_STAGING_DIR" ] || { echo "backup source path changed" >&2; exit 1; }
                    cp -R "$1" "$FAKE_RESTIC_SNAPSHOT/latest/"
                    shift
                    ;;
            esac
        done
        ;;
    forget)
        [ "${FAKE_RESTIC_FORGET_FAIL:-0}" = 0 ] || exit 1
        ;;
    check)
        [ "${FAKE_RESTIC_CHECK_FAIL:-0}" = 0 ] || exit 1
        ;;
    restore)
        target=
        while [ "$#" -gt 0 ]; do
            if [ "$1" = --target ]; then target=$2; shift 2; else shift; fi
        done
        mkdir -p "$target/restored"
        cp -R "$FAKE_RESTIC_SNAPSHOT/latest"/* "$target/restored/"
        ;;
    *) echo "unexpected fake Restic command" >&2; exit 2 ;;
esac
MOCK_RESTIC

cat >"$work/bin/podman" <<'MOCK_PODMAN'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$FAKE_COMMAND_LOG"
command=$1
shift
case "$command" in
    exec)
        [ "${1:-}" = -i ] && shift
        shift # container name
        case "${1:-}" in
            pg_dump) printf 'fake PostgreSQL custom dump\n' ;;
            pg_restore) [ "${FAKE_PG_RESTORE_FAIL:-0}" = 0 ] || exit 1; cat >/dev/null ;;
            pg_isready) exit 0 ;;
            *) echo "unexpected fake podman exec" >&2; exit 2 ;;
        esac
        ;;
    inspect)
        container=$3
        case "$container" in
            agentux-command) echo command-test-image ;;
            agentux-command-postgres) echo postgres-test-image ;;
            *) echo "unexpected fake container inspect" >&2; exit 2 ;;
        esac
        ;;
    run)
        case " $* " in
            *" --detach "*) echo temporary database started ;;
            *" --rm "*) [ "${FAKE_COMMAND_CHECK_FAIL:-0}" = 0 ] || exit 1 ;;
            *) echo "unexpected fake podman run" >&2; exit 2 ;;
        esac
        ;;
    network)
        case "$1" in create|rm) exit 0 ;; esac
        ;;
    rm) exit 0 ;;
    cp)
        archive_source=${FAKE_ARCHIVE_SOURCE:?}
        mkdir -p "$2"
        cp -R "$archive_source"/. "$2"
        ;;
    *) echo "unexpected fake podman command" >&2; exit 2 ;;
esac
MOCK_PODMAN

chmod +x "$work/bin/restic" "$work/bin/podman"

fail() { echo "backup workflow test: $*" >&2; exit 1; }
metric() { grep -E "^$1 " "$BACKUP_METRICS_FILE" | awk '{print $2}'; }

sh "$root/deploy/scripts/backup-status.sh" backup success
backup_first=$(metric agentux_command_backup_last_success_timestamp_seconds)
[ "$backup_first" -gt 0 ] || fail "backup success timestamp was not written"
[ "$(metric agentux_command_backup_failures_total)" = 0 ] || fail "backup failure counter should start at zero"

sh "$root/deploy/scripts/backup-status.sh" backup failure
[ "$(metric agentux_command_backup_last_success_timestamp_seconds)" = "$backup_first" ] || fail "backup failure changed its last-success timestamp"
[ "$(metric agentux_command_backup_failures_total)" = 1 ] || fail "backup failure was not counted"

sh "$root/deploy/scripts/backup-status.sh" restore_verification success
verify_first=$(metric agentux_command_restore_verification_last_success_timestamp_seconds)
[ "$verify_first" -gt 0 ] || fail "restore verification success timestamp was not written"
[ "$(metric agentux_command_restore_verification_failures_total)" = 0 ] || fail "restore failure counter should start at zero"

sh "$root/deploy/scripts/backup-status.sh" restore_verification failure
[ "$(metric agentux_command_restore_verification_last_success_timestamp_seconds)" = "$verify_first" ] || fail "restore failure changed its last-success timestamp"
[ "$(metric agentux_command_restore_verification_failures_total)" = 1 ] || fail "restore failure was not counted"

if grep -E 'organization|node_id|repository|password|secret' "$BACKUP_METRICS_FILE"; then
    fail "metrics contain an unsafe label or secret-like field"
fi

# Start workflow-level assertions with clean counters after testing the status
# helper's increment behavior above.
rm -rf "$BACKUP_STATE_DIR" "$work/metrics"
mkdir -p "$work/metrics"

BACKUP_KEEP_DAILY=0
BACKUP_KEEP_WEEKLY=0
BACKUP_KEEP_MONTHLY=0
export BACKUP_KEEP_DAILY BACKUP_KEEP_WEEKLY BACKUP_KEEP_MONTHLY
if sh "$root/deploy/scripts/backup-offsite.sh" >"$work/retention-config-failure.log" 2>&1; then
    fail "zero retention periods must not allow every snapshot to be pruned"
fi
unset BACKUP_KEEP_DAILY BACKUP_KEEP_WEEKLY BACKUP_KEEP_MONTHLY
[ "$(metric agentux_command_backup_last_success_timestamp_seconds)" = 0 ] || fail "invalid retention config recorded backup success"
[ "$(metric agentux_command_backup_failures_total)" = 1 ] || fail "invalid retention config was not counted"
rm -rf "$BACKUP_STATE_DIR" "$work/metrics"
mkdir -p "$work/metrics"

sh "$root/deploy/scripts/backup-offsite.sh" >"$work/backup.log"
backup_success=$(metric agentux_command_backup_last_success_timestamp_seconds)
[ "$backup_success" -gt 0 ] || fail "off-site backup did not record success"
[ "$(metric agentux_command_backup_failures_total)" = 0 ] || fail "successful backup was counted as a failure"
[ "$(find "$FAKE_RESTIC_SNAPSHOT/latest" -type f -name 'agentux-command-*.dump' | wc -l)" -eq 1 ] || fail "Restic did not receive the database dump"
[ "$(cat "$FAKE_RESTIC_SNAPSHOT/latest/staging/event-archives/events.jsonl")" = 'archived event fixture' ] || fail "Restic did not receive the configured event archive"

FAKE_RESTIC_BACKUP_FAIL=1
export FAKE_RESTIC_BACKUP_FAIL
if sh "$root/deploy/scripts/backup-offsite.sh" >"$work/upload-failure.log" 2>&1; then
    fail "upload failure should fail the scheduled backup"
fi
unset FAKE_RESTIC_BACKUP_FAIL
[ "$(metric agentux_command_backup_last_success_timestamp_seconds)" = "$backup_success" ] || fail "upload failure changed last-success time"
[ "$(metric agentux_command_backup_failures_total)" = 1 ] || fail "upload failure was not counted"

FAKE_RESTIC_FORGET_FAIL=1
export FAKE_RESTIC_FORGET_FAIL
if sh "$root/deploy/scripts/backup-offsite.sh" >"$work/backup-failure.log" 2>&1; then
    fail "retention failure should fail the scheduled backup"
fi
unset FAKE_RESTIC_FORGET_FAIL
[ "$(metric agentux_command_backup_last_success_timestamp_seconds)" = "$backup_success" ] || fail "failed backup changed last-success time"
[ "$(metric agentux_command_backup_failures_total)" = 2 ] || fail "retention failure was not counted"
[ -z "$(find "$BACKUP_STAGING_DIR" -type f -name 'agentux-command-*' -print -quit)" ] || fail "local staging files were left behind"
if grep -F 'test-only-password' "$work/backup.log" "$work/upload-failure.log" "$work/backup-failure.log"; then
    fail "backup logs exposed the Restic password"
fi

sh "$root/deploy/scripts/verify-offsite-backup.sh" >"$work/verify.log"
verify_success=$(metric agentux_command_restore_verification_last_success_timestamp_seconds)
[ "$verify_success" -gt 0 ] || fail "restore verification did not record success"
[ -s "$FAKE_RESTIC_SNAPSHOT/latest/staging/event-archives/events.jsonl" ] || fail "event archive fixture was not preserved for restore verification"
[ "$(metric agentux_command_restore_verification_failures_total)" = 0 ] || fail "successful verification was counted as a failure"
grep -q 'rm -f agentux-command-backup-verify-' "$FAKE_COMMAND_LOG" || fail "temporary verification database was not removed"

FAKE_RESTIC_CHECK_FAIL=1
export FAKE_RESTIC_CHECK_FAIL
if sh "$root/deploy/scripts/verify-offsite-backup.sh" >"$work/repository-check-failure.log" 2>&1; then
    fail "repository check failure should fail restore verification"
fi
unset FAKE_RESTIC_CHECK_FAIL
[ "$(metric agentux_command_restore_verification_last_success_timestamp_seconds)" = "$verify_success" ] || fail "repository failure changed verification last-success time"
[ "$(metric agentux_command_restore_verification_failures_total)" = 1 ] || fail "repository failure was not counted"

dump=$(find "$FAKE_RESTIC_SNAPSHOT/latest" -type f -name 'agentux-command-*.dump' -print -quit)
printf 'corrupt dump\n' >"$dump"
if sh "$root/deploy/scripts/verify-offsite-backup.sh" >"$work/corrupt.log" 2>&1; then
    fail "checksum mismatch should fail restore verification"
fi
[ "$(metric agentux_command_restore_verification_last_success_timestamp_seconds)" = "$verify_success" ] || fail "checksum failure changed verification last-success time"
[ "$(metric agentux_command_restore_verification_failures_total)" = 2 ] || fail "checksum failure was not counted"

echo 'valid restored dump' >"$dump"
sha256sum "$dump" >"$dump.sha256"
FAKE_PG_RESTORE_FAIL=1
export FAKE_PG_RESTORE_FAIL
if sh "$root/deploy/scripts/verify-offsite-backup.sh" >"$work/restore-failure.log" 2>&1; then
    fail "database restore failure should fail restore verification"
fi
unset FAKE_PG_RESTORE_FAIL
[ "$(metric agentux_command_restore_verification_last_success_timestamp_seconds)" = "$verify_success" ] || fail "database restore failure changed verification last-success time"
[ "$(metric agentux_command_restore_verification_failures_total)" = 3 ] || fail "database restore failure was not counted"

echo 'restore' >"$dump"
sha256sum "$dump" >"$dump.sha256"
FAKE_COMMAND_CHECK_FAIL=1
export FAKE_COMMAND_CHECK_FAIL
if sh "$root/deploy/scripts/verify-offsite-backup.sh" >"$work/check-failure.log" 2>&1; then
    fail "Command check failure should fail restore verification"
fi
unset FAKE_COMMAND_CHECK_FAIL
[ "$(metric agentux_command_restore_verification_last_success_timestamp_seconds)" = "$verify_success" ] || fail "Command check failure changed verification last-success time"
[ "$(metric agentux_command_restore_verification_failures_total)" = 4 ] || fail "Command check failure was not counted"
grep -q 'rm -f agentux-command-backup-verify-' "$FAKE_COMMAND_LOG" || fail "failed verification did not clean up its temporary database"
grep -q 'network rm agentux-command-backup-verify-' "$FAKE_COMMAND_LOG" || fail "failed verification did not clean up its private network"

echo "backup workflow tests passed"
