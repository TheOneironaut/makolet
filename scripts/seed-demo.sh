#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$repository_root"
source "$repository_root/scripts/compose-launcher.sh"
makolet_initialize_compose "$repository_root"
"${compose[@]}" --profile demo run --rm demo-seed
