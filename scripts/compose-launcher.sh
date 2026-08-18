#!/usr/bin/env bash

# Shared fail-closed Docker Compose target selection for repository wrappers.
# This file is sourced after each wrapper has resolved repository_root.

makolet_initialize_compose() {
  if [[ $# -ne 1 ]]; then
    echo "internal error: makolet_initialize_compose requires the repository root" >&2
    return 2
  fi
  local selected_repository_root=$1
  local compose_project_name=makolet
  local selector

  if [[ -v COMPOSE_PROJECT_NAME ]]; then
    if [[ ! "$COMPOSE_PROJECT_NAME" =~ ^makolet-smoke-[a-z0-9][a-z0-9_-]{0,40}-[0-9]{1,10}$ ]] || \
      [[ "${MAKOLET_COMPOSE_ENV_FILE:-}" != ".env.example" ]] || \
      [[ "${MAKOLET_ENVIRONMENT:-}" != "development" ]] || \
      [[ "${POSTGRES_DB:-}" != "makolet_test_coverage" ]]; then
      echo "refusing ambient Compose or Docker target selector: COMPOSE_PROJECT_NAME" >&2
      return 1
    fi
    compose_project_name=$COMPOSE_PROJECT_NAME
    unset COMPOSE_PROJECT_NAME
  fi

  for selector in \
    COMPOSE_FILE COMPOSE_ENV_FILES COMPOSE_PATH_SEPARATOR COMPOSE_PROFILES \
    COMPOSE_DISABLE_ENV_FILE DOCKER_CONFIG DOCKER_CONTEXT DOCKER_HOST; do
    if [[ -v $selector ]]; then
      echo "refusing ambient Compose or Docker target selector: $selector" >&2
      return 1
    fi
  done

  local compose_file="$selected_repository_root/compose.yaml"
  if [[ ! -f "$compose_file" || -L "$compose_file" ]]; then
    echo "repository compose.yaml must be a regular non-symlink file" >&2
    return 1
  fi
  compose=(
    docker compose
    --file "$compose_file"
    --project-directory "$selected_repository_root"
    --project-name "$compose_project_name"
  )

  if [[ -n "${MAKOLET_COMPOSE_ENV_FILE:-}" ]]; then
    local compose_env_input=$MAKOLET_COMPOSE_ENV_FILE
    if [[ ! -f "$compose_env_input" || -L "$compose_env_input" ]]; then
      echo "MAKOLET_COMPOSE_ENV_FILE must name a regular non-symlink file" >&2
      return 1
    fi
    local compose_env_directory
    compose_env_directory="$(cd "$(dirname "$compose_env_input")" && pwd -P)"
    local compose_env_file="$compose_env_directory/$(basename "$compose_env_input")"
    compose+=(--env-file "$compose_env_file")
  fi
}
