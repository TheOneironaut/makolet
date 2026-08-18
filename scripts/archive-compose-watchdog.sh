#!/usr/bin/env bash

# Shared bounded supervision for raw-archive Docker Compose commands. This file
# is sourced by the three archive entry points after they resolve the repository.

_archive_validate_seconds() {
  local name=$1
  local value=$2
  local maximum=$3
  local number_pattern='^([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$'
  if [[ ! "$value" =~ $number_pattern ]] ||
    ! awk -v value="$value" -v maximum="$maximum" \
      'BEGIN { exit !(value > 0 && value <= maximum) }'; then
    echo "$name must be positive and at most $maximum" >&2
    return 1
  fi
}

_archive_ceil_seconds() {
  awk -v value="$1" 'BEGIN { rounded = int(value); print rounded < value ? rounded + 1 : rounded }'
}

archive_backup_lock_name() {
  local project_name=$1
  if [[ ! "$project_name" =~ ^(makolet|makolet-smoke-[a-z0-9][a-z0-9_-]{0,40}-[0-9]{1,10})$ ]]; then
    echo "archive backup cannot form a safe project lock name" >&2
    return 1
  fi
  printf 'makolet_archive_backup_lock_%s\n' "$project_name"
}

archive_backup_lock_owner() {
  local timeout_seconds=$1
  local lock_name=$2
  if [[ ! "$lock_name" =~ ^makolet_archive_backup_lock_[a-z0-9_-]{1,80}$ ]]; then
    echo "refusing to inspect an unsafe archive backup lock" >&2
    return 1
  fi
  run_with_bounded_watchdog \
    "$timeout_seconds" \
    docker volume inspect \
    --format '{{ index .Labels "com.makolet.archive-backup-lock.owner" }}' \
    "$lock_name"
}

acquire_archive_backup_lock() {
  local timeout_seconds=$1
  local lock_name=$2
  local owner=$3
  if [[ ! "$owner" =~ ^makolet-archive-backup-[a-zA-Z0-9_.-]{1,100}$ ]]; then
    echo "refusing an unsafe archive backup lock owner" >&2
    return 1
  fi
  local created_name
  created_name="$(
    run_with_bounded_watchdog \
      "$timeout_seconds" \
      docker volume create \
      --label "com.makolet.archive-backup-lock.owner=$owner" \
      "$lock_name"
  )"
  if [[ "$created_name" != "$lock_name" ]]; then
    echo "archive backup lock creation returned an unexpected target" >&2
    return 1
  fi
  local observed_owner
  observed_owner="$(archive_backup_lock_owner "$timeout_seconds" "$lock_name")"
  if [[ "$observed_owner" != "$owner" ]]; then
    echo "archive backup lock is already owned by another operation: $lock_name" >&2
    return 1
  fi
}

release_archive_backup_lock() {
  local timeout_seconds=$1
  local lock_name=$2
  local owner=$3
  local observed_owner
  observed_owner="$(archive_backup_lock_owner "$timeout_seconds" "$lock_name")"
  if [[ "$observed_owner" != "$owner" ]]; then
    echo "archive backup lock ownership changed before release: $lock_name" >&2
    return 1
  fi
  run_with_bounded_watchdog \
    "$timeout_seconds" \
    docker volume rm "$lock_name" >/dev/null
}

archive_operation_watchdog_seconds() {
  local operation_seconds=${MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS:-3600.0}
  local cleanup_seconds=${MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS:-30.0}
  _archive_validate_seconds \
    MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS "$operation_seconds" 86400
  _archive_validate_seconds \
    MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS "$cleanup_seconds" 300
  awk -v work="$operation_seconds" -v cleanup="$cleanup_seconds" '
    BEGIN {
      total = work + cleanup
      rounded = int(total)
      if (rounded < total) {
        rounded += 1
      }
      print rounded
    }
  '
}

archive_cleanup_watchdog_seconds() {
  local cleanup_seconds=${MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS:-30.0}
  _archive_validate_seconds \
    MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS "$cleanup_seconds" 300
  _archive_ceil_seconds "$cleanup_seconds"
}

new_archive_container_name() {
  local operation=$1
  if [[ ! "$operation" =~ ^(backup|verify|restore)$ ]]; then
    echo "archive operation cannot form a safe container name" >&2
    return 1
  fi
  local name="makolet-archive-$operation-$$-$RANDOM-$RANDOM-$RANDOM"
  if [[ ! "$name" =~ ^makolet-archive-(backup|verify|restore)-[0-9]{1,10}(-[0-9]{1,5}){3}$ ]]; then
    echo "generated archive container name is unsafe" >&2
    return 1
  fi
  printf '%s\n' "$name"
}

run_with_bounded_watchdog() {
  local timeout_seconds=$1
  shift
  if [[ ! "$timeout_seconds" =~ ^[0-9]{1,6}$ ]] ||
    ((10#$timeout_seconds < 1 || 10#$timeout_seconds > 86700)); then
    echo "archive Compose watchdog timeout is outside the allowed range" >&2
    return 1
  fi

  "$@" &
  local command_pid=$!
  (
    local elapsed_ticks=0
    local timeout_ticks=$((10#$timeout_seconds * 10))
    while ((elapsed_ticks < timeout_ticks)); do
      sleep 0.1
      kill -0 "$command_pid" 2>/dev/null || exit 0
      ((elapsed_ticks += 1))
    done
    kill -TERM "$command_pid" 2>/dev/null || exit 0
    local termination_grace_ticks=0
    while ((termination_grace_ticks < 20)); do
      sleep 0.1
      kill -0 "$command_pid" 2>/dev/null || exit 124
      ((termination_grace_ticks += 1))
    done
    kill -KILL "$command_pid" 2>/dev/null || true
    exit 124
  ) &
  local watchdog_pid=$!
  local command_status=0
  local watchdog_status=0
  wait "$command_pid" || command_status=$?
  wait "$watchdog_pid" || watchdog_status=$?
  if ((watchdog_status == 124)); then
    echo "archive Compose operation exceeded its bounded watchdog" >&2
    return 124
  fi
  return "$command_status"
}

remove_archive_container_with_watchdog() {
  local timeout_seconds=$1
  local container_name=$2
  if [[ ! "$container_name" =~ ^makolet-archive-(backup|verify|restore)-[a-zA-Z0-9_.-]{1,100}$ ]]; then
    echo "refusing to remove an unsafe archive container name" >&2
    return 1
  fi
  run_with_bounded_watchdog \
    "$timeout_seconds" \
    docker container rm --force "$container_name" >/dev/null
}
