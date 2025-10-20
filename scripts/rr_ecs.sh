#!/usr/bin/env bash
set -euo pipefail
regions="${1:-us-east-1}"
shift || true
profiles="$@"
scripts/run_review.sh ecs_rightsizing --regions "${regions}" --profiles ${profiles}
