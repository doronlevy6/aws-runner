#!/usr/bin/env bash
set -euo pipefail

regions="${1:-all}"
shift || true
profiles=("$@")

if [[ ${#profiles[@]} -eq 0 ]]; then
  echo "Usage: scripts/rr_s3_cost_analysis.sh <regions|all> <profile1> [profile2...]" >&2
  exit 2
fi

scripts/run_review.sh s3_cost_analysis --regions "$regions" --profiles "${profiles[@]}"
