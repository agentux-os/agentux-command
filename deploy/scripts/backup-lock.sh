#!/bin/sh
# POSIX directory lock shared by backup and restore-verification jobs.
# Including the kernel boot ID lets a later boot remove an abandoned lock.

backup_lock_acquire() {
    BACKUP_LOCK_PATH=$1
    wait_seconds=${BACKUP_LOCK_WAIT_SECONDS:-3600}
    case "$wait_seconds" in ''|*[!0-9]*) echo "backup lock: wait timeout must be a non-negative integer" >&2; return 2 ;; esac
    boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || printf unknown)
    attempt=0
    while [ "$attempt" -lt "$wait_seconds" ]; do
        if mkdir "$BACKUP_LOCK_PATH" 2>/dev/null; then
            if ! printf '%s %s\n' "$$" "$boot_id" >"$BACKUP_LOCK_PATH/owner"; then
                rmdir "$BACKUP_LOCK_PATH" 2>/dev/null || true
                return 1
            fi
            return 0
        fi
        if [ -r "$BACKUP_LOCK_PATH/owner" ]; then
            IFS=' ' read -r owner_pid owner_boot <"$BACKUP_LOCK_PATH/owner" || true
            if [ "$owner_boot" != "$boot_id" ] || ! kill -0 "$owner_pid" 2>/dev/null; then
                rm -rf "$BACKUP_LOCK_PATH" 2>/dev/null || true
                continue
            fi
        elif [ "$attempt" -ge 2 ]; then
            rmdir "$BACKUP_LOCK_PATH" 2>/dev/null || true
            continue
        fi
        sleep 1
        attempt=$((attempt + 1))
    done
    echo "backup lock: timed out waiting for another backup operation" >&2
    return 1
}

backup_lock_release() {
    [ -n "${BACKUP_LOCK_PATH:-}" ] || return 0
    if [ -r "$BACKUP_LOCK_PATH/owner" ]; then
        IFS=' ' read -r owner_pid owner_boot <"$BACKUP_LOCK_PATH/owner" || true
        boot_id=$(cat /proc/sys/kernel/random/boot_id 2>/dev/null || printf unknown)
        if [ "$owner_pid" = "$$" ] && [ "$owner_boot" = "$boot_id" ]; then
            rm -rf "$BACKUP_LOCK_PATH"
        fi
    fi
}
