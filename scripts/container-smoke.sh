#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repository_root"

unset COMPOSE_FILE COMPOSE_ENV_FILES
export COMPOSE_PROJECT_NAME="makolet-smoke-${GITHUB_RUN_ID:-local}-$$"
export MAKOLET_COMPOSE_ENV_FILE=".env.example"
export MAKOLET_ENVIRONMENT=development
export POSTGRES_DB=makolet_test_coverage
export MAKOLET_DATABASE_URL=postgresql://makolet:makolet-development-only-change-me@postgres:5432/makolet_test_coverage
export MAKOLET_POSTGRES_PORT=0
export MAKOLET_S3_PORT=0
export MAKOLET_API_PORT=0
export MAKOLET_PROMETHEUS_PORT=0
export MAKOLET_ENABLED_SOURCES='[]'
export MAKOLET_S3_ENDPOINT=http://seaweedfs:8333
export MAKOLET_S3_ALLOW_INSECURE_LOCAL=true
export MAKOLET_S3_BUCKET=makolet-raw
export MAKOLET_S3_REGION=us-east-1
export MAKOLET_S3_ACCESS_KEY=makolet-development
export MAKOLET_S3_SECRET_KEY=makolet-development-only-change-me
export MAKOLET_S3_KEY_PREFIX=raw
export MAKOLET_S3_PATH_STYLE=true

is_windows_posix_shell() {
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) return 0 ;;
    *) return 1 ;;
  esac
}

run_archive_operation() {
  local operation="$1"
  local backup_directory="$2"
  local confirmation="${3:-}"
  case "$operation" in
    archive-backup|archive-verify) ;;
    archive-restore)
      if [[ -z "$confirmation" ]]; then
        echo "archive-restore requires an exact bucket confirmation" >&2
        return 2
      fi
      ;;
    *)
      echo "unsupported archive operation: $operation" >&2
      return 2
      ;;
  esac

  if is_windows_posix_shell; then
    if ! command -v pwsh.exe >/dev/null 2>&1; then
      echo "container smoke on Windows requires PowerShell 7 (pwsh.exe)" >&2
      return 2
    fi
    local operations_script_windows
    local backup_directory_windows
    local authentication_key_windows
    operations_script_windows="$(cygpath -w "$repository_root/scripts/operations.ps1")"
    backup_directory_windows="$(cygpath -w "$backup_directory")"
    authentication_key_windows="$(cygpath -w "$MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE")"
    local arguments=(
      -NoProfile -NonInteractive -ExecutionPolicy Bypass
      -File "$operations_script_windows" "$operation" "$backup_directory_windows"
    )
    if [[ -n "$confirmation" ]]; then
      arguments+=("$confirmation")
    fi
    MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE="$authentication_key_windows" \
      pwsh.exe "${arguments[@]}"
    return
  fi

  case "$operation" in
    archive-backup) bash scripts/archive-backup.sh "$backup_directory" ;;
    archive-verify) bash scripts/archive-verify.sh "$backup_directory" ;;
    archive-restore) bash scripts/archive-restore.sh "$backup_directory" "$confirmation" ;;
  esac
}

compose=(
  docker compose
  --file "$repository_root/compose.yaml"
  --env-file "$MAKOLET_COMPOSE_ENV_FILE"
)
temporary_root="$(mktemp -d "${TMPDIR:-/tmp}/makolet-smoke.XXXXXX")"
cleanup() {
  "${compose[@]}" --profile monitoring down --volumes --remove-orphans >/dev/null 2>&1 || true
  case "$temporary_root" in
    "${TMPDIR:-/tmp}"/makolet-smoke.*) rm -rf -- "$temporary_root" ;;
    *) echo "refusing to remove unexpected smoke-test path: $temporary_root" >&2 ;;
  esac
}
trap cleanup EXIT

"${compose[@]}" config --quiet
uv run python scripts/check_container_images.py
"${compose[@]}" build --pull api
"${compose[@]}" up -d --wait postgres seaweedfs
"${compose[@]}" run --rm migrate
"${compose[@]}" --profile demo run --rm demo-seed
"${compose[@]}" up -d --wait api worker
"${compose[@]}" --profile monitoring up -d --wait prometheus

api_binding="$("${compose[@]}" port api 8000 | tail -n 1)"
api_port="${api_binding##*:}"
if [[ ! "$api_port" =~ ^[0-9]{1,5}$ ]]; then
  echo "could not determine the loopback API port" >&2
  exit 1
fi
api_url="http://127.0.0.1:$api_port"
curl --fail --silent --show-error --max-time 5 "$api_url/healthz" >/dev/null
curl --fail --silent --show-error --max-time 5 "$api_url/readyz" >/dev/null
curl --fail --silent --show-error --max-time 5 \
  "$api_url/api/v1/products/search?query=%D7%98%D7%97%D7%99%D7%A0%D7%94&limit=10" \
  >"$temporary_root/products.json"
uv run python - "$temporary_root/products.json" <<'PY'
import json
import sys
from pathlib import Path

payload = json.loads(Path(sys.argv[1]).read_bytes())
if not payload.get("items"):
    raise SystemExit("demo product was not queryable through the container API")
PY

prometheus_binding="$("${compose[@]}" port prometheus 9090 | tail -n 1)"
prometheus_port="${prometheus_binding##*:}"
if [[ ! "$prometheus_port" =~ ^[0-9]{1,5}$ ]]; then
  echo "could not determine the loopback Prometheus port" >&2
  exit 1
fi
prometheus_url="http://127.0.0.1:$prometheus_port"
curl --fail --silent --show-error --max-time 5 "$prometheus_url/-/healthy" >/dev/null
uv run python - "$prometheus_url/api/v1/targets" <<'PY'
import json
import sys
import time
import urllib.request

target_url = sys.argv[1]
expected_jobs = {"makolet-api", "makolet-worker", "seaweedfs"}
deadline = time.monotonic() + 60
last_health: dict[str, str] = {}
while time.monotonic() < deadline:
    with urllib.request.urlopen(target_url, timeout=5) as response:
        payload = json.load(response)
    active_targets = payload.get("data", {}).get("activeTargets", [])
    last_health = {
        str(target.get("labels", {}).get("job")): str(target.get("health"))
        for target in active_targets
    }
    if all(last_health.get(job) == "up" for job in expected_jobs):
        break
    time.sleep(1)
else:
    raise SystemExit(f"Prometheus targets did not become healthy: {last_health!r}")
PY

for application_service in api worker; do
  application_id="$("${compose[@]}" ps -q "$application_service")"
  if [[ ! "$application_id" =~ ^[0-9a-f]{64}$ ]]; then
    echo "could not resolve the $application_service container" >&2
    exit 1
  fi
  runtime_security="$(docker inspect --format '{{.Config.User}} {{.HostConfig.ReadonlyRootfs}}' "$application_id")"
  if [[ "$runtime_security" != "10001:10001 true" ]]; then
    echo "$application_service container is not non-root with a read-only root filesystem" >&2
    exit 1
  fi
done

# The full stack has now been observed healthy. Stop every long-lived database
# client before the host coverage suite resets shared tables with TRUNCATE.
"${compose[@]}" --profile monitoring stop prometheus
"${compose[@]}" stop api worker

# Capture the known-clean deterministic demo before the coverage fixtures reset and
# repopulate the shared database. Restoring this snapshot after coverage both proves
# the authenticated database recovery path and gives the archive inventory comparison
# an exact database/S3 baseline rather than test-order-dependent fixture residue.
mkdir -p "$temporary_root/database" "$temporary_root/archive" "$temporary_root/protected"
export MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE="$temporary_root/protected/database-backup-auth.key"
export MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE="$temporary_root/protected/archive-backup-auth.key"
uv run python -m makolet.interfaces.database_backup_auth \
  generate-key "$MAKOLET_DATABASE_BACKUP_AUTH_KEY_FILE"
uv run python -m makolet.interfaces.database_backup_auth \
  generate-key "$MAKOLET_ARCHIVE_BACKUP_AUTH_KEY_FILE"
bash scripts/database-backup.sh "$temporary_root/database/makolet.dump"

postgres_binding="$("${compose[@]}" port postgres 5432 | tail -n 1)"
postgres_port="${postgres_binding##*:}"
s3_binding="$("${compose[@]}" port seaweedfs 8333 | tail -n 1)"
s3_port="${s3_binding##*:}"
(
  # The Compose stack intentionally exports local application settings. The host
  # coverage process must start from a clean Makolet namespace so configuration
  # default tests and service fixtures observe only their explicit TEST_* inputs.
  unset "${!MAKOLET_@}"
  COVERAGE_FILE="$temporary_root/.coverage" \
  MAKOLET_TEST_DATABASE_URL="postgresql://makolet:makolet-development-only-change-me@127.0.0.1:$postgres_port/makolet_test_coverage" \
  MAKOLET_TEST_DATABASE_CONFIRM=makolet_test_coverage \
  MAKOLET_TEST_S3_ENDPOINT="http://127.0.0.1:$s3_port" \
  MAKOLET_TEST_S3_BUCKET=makolet-raw \
  MAKOLET_TEST_S3_ACCESS_KEY=makolet-development \
  MAKOLET_TEST_S3_SECRET_KEY=makolet-development-only-change-me \
    uv run pytest --cov=makolet --cov-branch --cov-report=term-missing \
      --cov-fail-under=85 -m "not live and not benchmark"
)

# The real-database fixtures intentionally leave their final test state in the
# dedicated schema. Restore the pre-coverage deterministic demo so PostgreSQL's
# authoritative raw-object inventory exactly describes the archive prefix.
bash scripts/database-restore.sh "$temporary_root/database/makolet.dump" makolet_test_coverage
bash scripts/database-status.sh >/dev/null
"${compose[@]}" --profile demo run --rm demo-seed

run_archive_operation archive-backup "$temporary_root/archive"
run_archive_operation archive-verify "$temporary_root/archive"
restore_bucket="makolet-restore-verification-${GITHUB_RUN_ID:-local}-$$"
MAKOLET_S3_BUCKET="$restore_bucket" "${compose[@]}" --profile operations run --rm \
  --no-deps --entrypoint python archive-tool -c \
  'import boto3, os; from botocore.config import Config; boto3.client("s3", endpoint_url=os.environ["MAKOLET_S3_ENDPOINT"], region_name=os.environ["MAKOLET_S3_REGION"], aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"], aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"], config=Config(s3={"addressing_style":"path"})).create_bucket(Bucket=os.environ["MAKOLET_S3_BUCKET"])'
MAKOLET_S3_BUCKET="$restore_bucket" \
  run_archive_operation archive-restore "$temporary_root/archive" "$restore_bucket" >/dev/null
# The target bucket was created immediately above and therefore started empty.
# Re-verifying its complete inventory is stronger than trusting the restore summary.
MAKOLET_S3_BUCKET="$restore_bucket" \
  run_archive_operation archive-verify "$temporary_root/archive" >/dev/null
"${compose[@]}" up -d --wait api worker
"${compose[@]}" --profile monitoring up -d --wait prometheus
api_binding="$("${compose[@]}" port api 8000 | tail -n 1)"
api_port="${api_binding##*:}"
if [[ ! "$api_port" =~ ^[0-9]{1,5}$ ]]; then
  echo "could not determine the restored loopback API port" >&2
  exit 1
fi
api_url="http://127.0.0.1:$api_port"
curl --fail --silent --show-error --max-time 5 \
  "$api_url/api/v1/barcodes/7290000000015" \
  >"$temporary_root/restored-product.json"
curl --fail --silent --show-error --max-time 5 \
  "$api_url/api/v1/products/77777777-7777-7777-7777-777777777777/prices?limit=1" \
  >"$temporary_root/restored-prices.json"
uv run python - "$temporary_root/restored-product.json" "$temporary_root/restored-prices.json" <<'PY'
import json
import sys
from decimal import Decimal
from pathlib import Path

product = json.loads(Path(sys.argv[1]).read_bytes()).get("data", {})
prices = json.loads(Path(sys.argv[2]).read_bytes()).get("items", [])
if product.get("id") != "77777777-7777-7777-7777-777777777777":
    raise SystemExit("database restore did not preserve the deterministic demo product")
if len(prices) != 1 or Decimal(str(prices[0].get("item_price"))) != Decimal("18.90"):
    raise SystemExit("database restore did not preserve the deterministic demo current price")
PY

printf '{"status":"passed","api_url":"%s"}\n' "$api_url"
