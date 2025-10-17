#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/run_review.sh rds_rightsizing --profiles steam-fi tea-fi --regions all
#   scripts/run_review.sh rds_rightsizing --profiles tea-fi --regions eu-west-1,us-east-1

REVIEW="${1:-}"
if [[ -z "$REVIEW" ]]; then
  echo "Usage: $0 <review-name> [args...]" >&2
  exit 2
fi
shift

# Activate venv if exists (non-fatal if missing)
if [[ -d ".venv" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate || true
fi

# Resolve repo root (this file is <repo>/scripts/run_review.sh)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Ensure Python sees the repo root as a package root
export PYTHONPATH="${REPO_DIR}:${PYTHONPATH:-}"

PY=python
command -v python3 >/dev/null 2>&1 && PY=python3

case "$REVIEW" in
  rds_rightsizing)
    # Run as a module from the repo root so 'scripts.*' imports work
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.rds_rightsizing.run "$@"
    ;;
  *)
    echo "Unknown review: $REVIEW" >&2
    exit 2
    ;;
esac
