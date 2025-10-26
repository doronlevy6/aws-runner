#!/usr/bin/env bash
set -euo pipefail
regions="${1:-us-east-1,eu-west-1}"
shift || true
profiles=("$@")
if [[ ${#profiles[@]} -eq 0 ]]; then
  echo "Usage: scripts/rr_ec2_utilization.sh <regions> <profile1> [profile2...]" >&2
  exit 2
fi
scripts/run_review.sh ec2_utilization --regions "$regions" --profiles "${profiles[@]}"
---