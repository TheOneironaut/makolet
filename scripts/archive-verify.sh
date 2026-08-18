#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: scripts/archive-verify.sh BACKUP_DIRECTORY" >&2
  exit 2
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repository_root"
watchdog_helper="$repository_root/scripts/archive-compose-watchdog.sh"
if [[ ! -f "$watchdog_helper" || -L "$watchdog_helper" ]]; then
  echo "archive Compose watchdog helper must be a regular non-symlink file" >&2
  exit 1
fi
# shellcheck source=scripts/archive-compose-watchdog.sh
source "$watchdog_helper"
archive_watchdog_timeout_seconds="$(archive_operation_watchdog_seconds)"
archive_cleanup_timeout_seconds="$(archive_cleanup_watchdog_seconds)"
archive_container_name="$(new_archive_container_name verify)"
compose_project_name=makolet
if [[ -v COMPOSE_PROJECT_NAME ]]; then
  if [[ ! "$COMPOSE_PROJECT_NAME" =~ ^makolet-smoke-[a-z0-9][a-z0-9_-]{0,40}-[0-9]{1,10}$ ]] || \
    [[ "${MAKOLET_COMPOSE_ENV_FILE:-}" != ".env.example" ]] || \
    [[ "${MAKOLET_ENVIRONMENT:-}" != "development" ]] || \
    [[ "${POSTGRES_DB:-}" != "makolet_test_coverage" ]]; then
    echo "refusing ambient Compose or Docker target selector: COMPOSE_PROJECT_NAME" >&2
    exit 1
  fi
  compose_project_name="$COMPOSE_PROJECT_NAME"
  unset COMPOSE_PROJECT_NAME
fi
for selector in \
  COMPOSE_FILE COMPOSE_ENV_FILES COMPOSE_PATH_SEPARATOR COMPOSE_PROFILES \
  COMPOSE_DISABLE_ENV_FILE DOCKER_CONFIG DOCKER_CONTEXT DOCKER_HOST; do
  if [[ -v $selector ]]; then
    echo "refusing ambient Compose or Docker target selector: $selector" >&2
    exit 1
  fi
done
compose_file="$repository_root/compose.yaml"
if [[ ! -f "$compose_file" || -L "$compose_file" ]]; then
  echo "repository compose.yaml must be a regular non-symlink file" >&2
  exit 1
fi
compose=(docker compose --file "$compose_file" --project-directory "$repository_root" --project-name "$compose_project_name")
if [[ -n "${MAKOLET_COMPOSE_ENV_FILE:-}" ]]; then
  compose_env_input="$MAKOLET_COMPOSE_ENV_FILE"
  if [[ ! -f "$compose_env_input" || -L "$compose_env_input" ]]; then
    echo "MAKOLET_COMPOSE_ENV_FILE must name a regular non-symlink file" >&2
    exit 1
  fi
  compose_env_directory="$(cd "$(dirname "$compose_env_input")" && pwd -P)"
  compose_env_file="$compose_env_directory/$(basename "$compose_env_input")"
  compose+=(--env-file "$compose_env_file")
fi
destination="$(cd "$1" && pwd -P)"
authentication_key_input="${MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE:-}"
if [[ -z "$authentication_key_input" || ! -f "$authentication_key_input" || -L "$authentication_key_input" ]]; then
  echo "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE must name a regular non-symlink file" >&2
  exit 1
fi
authentication_key_directory="$(cd "$(dirname "$authentication_key_input")" && pwd -P)"
authentication_key="$authentication_key_directory/$(basename "$authentication_key_input")"
case "$authentication_key" in
  "$destination"|"$destination"/*)
    echo "archive backup authentication key must be outside the backup tree" >&2
    exit 1
    ;;
esac
container_authentication_key=/run/secrets/makolet-archive-backup-auth.key
host_user_id="$(id -u)"
host_group_id="$(id -g)"
if [[ ! "$host_user_id" =~ ^[0-9]{1,10}$ || ! "$host_group_id" =~ ^[0-9]{1,10}$ ]]; then
  echo "could not determine a safe non-root invoking POSIX user and group" >&2
  exit 1
fi
if (( 10#$host_user_id < 1 || 10#$host_user_id > 2147483647 || 10#$host_group_id < 1 || 10#$host_group_id > 2147483647 )); then
  echo "could not determine a safe non-root invoking POSIX user and group" >&2
  exit 1
fi

backup_limit_arguments=()
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES=$MAKOLET_ARCHIVE_BACKUP_MAXIMUM_BYTES")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES=$MAKOLET_ARCHIVE_BACKUP_MINIMUM_FREE_BYTES")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES=$MAKOLET_ARCHIVE_BACKUP_MAX_LIST_PAGES")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES=$MAKOLET_ARCHIVE_BACKUP_MAX_NO_PROGRESS_PAGES")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS=$MAKOLET_ARCHIVE_BACKUP_LIST_TIMEOUT_SECONDS")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS=$MAKOLET_ARCHIVE_BACKUP_OPERATION_TIMEOUT_SECONDS")
fi
if [[ -n "${MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS+x}" ]]; then
  backup_limit_arguments+=(--env "MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS=$MAKOLET_ARCHIVE_BACKUP_CLEANUP_TIMEOUT_SECONDS")
fi
archive_container_cleanup_required=true
finish_archive_verify() {
  local operation_status=$?
  trap - EXIT
  if [[ "$archive_container_cleanup_required" == true ]] &&
    ! remove_archive_container_with_watchdog \
      "$archive_cleanup_timeout_seconds" "$archive_container_name"; then
    echo \
      "exact archive container $archive_container_name cleanup failed or exceeded its bounded watchdog" \
      >&2
    operation_status=1
  fi
  exit "$operation_status"
}
trap finish_archive_verify EXIT
run_with_bounded_watchdog \
  "$archive_watchdog_timeout_seconds" \
  "${compose[@]}" --profile operations run --rm --no-deps \
  --name "$archive_container_name" \
  --user "$host_user_id:$host_group_id" \
  --volume "$destination:/backup:ro" \
  --volume "$authentication_key:$container_authentication_key:ro" \
  --env "MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE=$container_authentication_key" \
  "${backup_limit_arguments[@]}" \
  archive-tool verify /backup
archive_container_cleanup_required=false
