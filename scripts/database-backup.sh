#!/usr/bin/env bash
set -euo pipefail

umask 077

if [[ $# -ne 1 ]]; then
  echo "usage: scripts/database-backup.sh BACKUP.dump" >&2
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

requested=$1
parent="$(dirname "$requested")"
filename="$(basename "$requested")"
mkdir -p "$parent"
parent="$(cd "$parent" && pwd -P)"
destination="$parent/$filename"
checksum_destination="$destination.sha256"
authentication_destination="$destination.hmac-sha256"
if [[ -e "$destination" || -e "$checksum_destination" || -e "$authentication_destination" ]]; then
  echo "refusing to overwrite an existing database backup" >&2
  exit 1
fi

temporary_directory="$(mktemp -d "$parent/.makolet-database-backup.XXXXXX")"
temporary="$temporary_directory/$filename"
temporary_checksum="$temporary.sha256"
temporary_authentication="$temporary.hmac-sha256"
cleanup() {
  rm -f -- "$temporary" "$temporary_checksum" "$temporary_authentication"
  rmdir -- "$temporary_directory" 2>/dev/null || true
}
trap cleanup EXIT

uv run python -m makolet.interfaces.database_backup_auth \
  capture-command "$temporary" "$parent" -- \
  "${compose[@]}" exec -T postgres sh -eu -c \
  'exec pg_dump --username="$POSTGRES_USER" --dbname="$POSTGRES_DB" --format=custom --compress=6 --no-owner --no-acl'

uv run python -m makolet.interfaces.database_backup_auth \
  validate-command "$temporary" -- \
  "${compose[@]}" exec -T postgres pg_restore --list

digest="$(uv run python -m makolet.interfaces.database_backup_auth \
  write-sidecars "$temporary" "$temporary_checksum" "$temporary_authentication" \
  "$filename" "$parent")"
chmod 600 "$temporary" "$temporary_checksum" "$temporary_authentication"
mv -- "$temporary" "$destination"
mv -- "$temporary_checksum" "$checksum_destination"
mv -- "$temporary_authentication" "$authentication_destination"
rmdir -- "$temporary_directory"
trap - EXIT

printf '{"status":"backed_up","path":"%s","sha256":"%s","authentication":"hmac-sha256-v1"}\n' \
  "$destination" "$digest"
