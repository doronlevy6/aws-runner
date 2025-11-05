#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/rr_opensearch_finops.sh us-east-1,eu-west-1 steam-fi tea-fi
#   scripts/rr_opensearch_finops.sh us-east-1 tea-fi

regions="${1:-us-east-1}"
shift || true

if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <regions_csv> <profiles...>" >&2
  exit 2
fi

scripts/run_review.sh opensearch_finops --regions "$regions" --profiles "$@"
