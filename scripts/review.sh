#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/review.sh <review-name> <regions-csv> <profile...>
# Examples:
#   scripts/review.sh rds_rightsizing us-east-1,us-east-2 steam-fi tea-fi
#   scripts/review.sh ec2_idle_instances eu-west-1 my-prod my-dev

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <review-name> <regions-csv> <profile...>" >&2
  exit 2
fi

REVIEW="$1"; shift
REGIONS="$1"; shift
PROFILES=("$@")

# Activate venv if exists (non-fatal)
if [[ -d ".venv" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate || true
fi

# Delegate to the generic review runner
exec scripts/run_review.sh "${REVIEW}" --regions "${REGIONS}" --profiles "${PROFILES[@]}"
