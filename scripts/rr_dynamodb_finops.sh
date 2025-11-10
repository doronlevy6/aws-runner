#!/usr/bin/env bash
# Part of aws-runner: convenience wrapper for DynamoDB FinOps review.
set -euo pipefail

REGION=${1:-}
PROFILE=${2:-}

if [[ -z "$REGION" || -z "$PROFILE" ]]; then
  echo "Usage: $0 <region> <profile>" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

PY=python
command -v python3 >/dev/null 2>&1 && PY=python3

cd "$REPO_DIR"
exec "$PY" scripts/reviews/dynamodb_finops/run.py --region "$REGION" --profile "$PROFILE"
