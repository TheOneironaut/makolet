#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: scripts/database-restore.sh BACKUP.dump CONFIRM_DATABASE_NAME" >&2
  exit 2
fi

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repository_root"
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

backup="$(cd "$(dirname "$1")" && pwd -P)/$(basename "$1")"
checksum_file="$backup.sha256"
authentication_file="$backup.hmac-sha256"
confirmation=$2
if [[ ! -f "$backup" || -L "$backup" || ! -f "$checksum_file" || -L "$checksum_file" || \
  ! -f "$authentication_file" || -L "$authentication_file" ]]; then
  echo "backup, .sha256, and .hmac-sha256 must all be regular files" >&2
  exit 1
fi

expected_digest="$(uv run python -m makolet.interfaces.database_backup_auth \
  read-checksum "$checksum_file")"
if [[ ! "$expected_digest" =~ ^[0-9a-f]{64}$ ]]; then
  echo "database backup checksum file is invalid" >&2
  exit 1
fi

verified_directory="$(mktemp -d "${TMPDIR:-/tmp}/makolet-database-restore.XXXXXX")"
verified_root="$(cd "$(dirname "$verified_directory")" && pwd -P)"
verified_backup="$verified_directory/authenticated.dump"
cleanup_verified_backup() {
  rm -f -- "$verified_backup"
  rmdir -- "$verified_directory" >/dev/null 2>&1 || true
}
trap cleanup_verified_backup EXIT

uv run python -m makolet.interfaces.database_backup_auth \
  verify-copy "$backup" "$authentication_file" "$verified_backup" "$verified_root"

if command -v sha256sum >/dev/null 2>&1; then
  actual_digest="$(sha256sum "$verified_backup" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
  actual_digest="$(shasum -a 256 "$verified_backup" | awk '{print $1}')"
else
  echo "sha256sum or shasum is required" >&2
  exit 1
fi
if [[ "$expected_digest" != "$actual_digest" ]]; then
  echo "database backup checksum verification failed" >&2
  exit 1
fi

"${compose[@]}" exec -T postgres pg_restore --list <"$verified_backup" >/dev/null
database="$("${compose[@]}" exec -T postgres sh -eu -c 'printf %s "$POSTGRES_DB"' | tr -d '\r')"
if [[ ! "$database" =~ ^[A-Za-z][A-Za-z0-9_]{0,62}$ ]]; then
  echo "configured database name is outside the restore script's safe identifier subset" >&2
  exit 1
fi
if [[ "$confirmation" != "$database" ]]; then
  echo "confirmation does not exactly match the configured database: $database" >&2
  exit 1
fi

suffix="$(date -u +%Y%m%d%H%M%S)_$$"
staging="makolet_restore_$suffix"
previous="makolet_previous_$suffix"
api_was_running=false
worker_was_running=false
staging_created=false
swapped=false

recover_on_error() {
  if [[ "$swapped" == false && "$staging_created" == true ]]; then
    "${compose[@]}" exec -T -e RESTORE_DB="$staging" postgres sh -eu -c \
      'dropdb --username="$POSTGRES_USER" --if-exists "$RESTORE_DB"' >/dev/null 2>&1 || true
  fi
  if [[ "$api_was_running" == true ]]; then
    "${compose[@]}" up -d api >/dev/null 2>&1 || true
  fi
  if [[ "$worker_was_running" == true ]]; then
    "${compose[@]}" up -d worker >/dev/null 2>&1 || true
  fi
  cleanup_verified_backup
}
trap recover_on_error EXIT

"${compose[@]}" exec -T -e RESTORE_DB="$staging" postgres sh -eu -c \
  'createdb --username="$POSTGRES_USER" "$RESTORE_DB"'
staging_created=true
if ! "${compose[@]}" exec -T -e RESTORE_DB="$staging" postgres sh -eu -c \
  'exec pg_restore --username="$POSTGRES_USER" --dbname="$RESTORE_DB" --exit-on-error --no-owner --no-acl' \
  <"$verified_backup"; then
  echo "database restore into the staging database failed; the active database is unchanged" >&2
  exit 1
fi

if ! revision="$("${compose[@]}" --progress quiet run --rm --no-deps \
  -e MAKOLET_RESTORE_STAGING_DATABASE="$staging" \
  migrate python -m makolet.interfaces.database_restore | tr -d '\r\n')"; then
  echo "staging migration or exact-head verification failed; the active database is unchanged" >&2
  exit 1
fi
if [[ ! "$revision" =~ ^[A-Za-z0-9_-]+(,[A-Za-z0-9_-]+)*$ ]]; then
  echo "staging migration returned an invalid revision; the active database is unchanged" >&2
  exit 1
fi

running_services="$("${compose[@]}" ps --status running --services)"
grep -qx api <<<"$running_services" && api_was_running=true
grep -qx worker <<<"$running_services" && worker_was_running=true
"${compose[@]}" stop api worker >/dev/null 2>&1 || true

swap_sql="SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$database' AND pid <> pg_backend_pid();
ALTER DATABASE \"$database\" RENAME TO \"$previous\";
ALTER DATABASE \"$staging\" RENAME TO \"$database\";"
printf '%s\n' "$swap_sql" | "${compose[@]}" exec -T postgres sh -eu -c \
  'exec psql --username="$POSTGRES_USER" --dbname=postgres --no-psqlrc --single-transaction --set=ON_ERROR_STOP=1'
swapped=true

if [[ "$api_was_running" == true ]]; then
  "${compose[@]}" up -d api >/dev/null
fi
if [[ "$worker_was_running" == true ]]; then
  "${compose[@]}" up -d worker >/dev/null
fi

cleanup_verified_backup
trap - EXIT
printf '{"status":"restored","database":"%s","previous_database":"%s","migration_revision":"%s"}\n' \
  "$database" "$previous" "$revision"
