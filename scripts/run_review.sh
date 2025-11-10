#scripts/run_review.sh
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
  rds_storage_audit)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.rds_storage_audit.run "$@"
    ;;
  # תוספת ל-scripts/run_review.sh (בלוק case)
  ecs_rightsizing)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.ecs_rightsizing.run "$@"
    ;;
  amazon_mq_finops)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.amazon_mq_finops.run "$@"
    ;;
  ec2_utilization)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.ec2_utilization.run "$@"
    ;;
  s3_cost_analysis)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.s3_cost_analysis.run "$@"
    ;;
  cloudfront_distributions_config)
    cd "$REPO_DIR"
    exec "$PY" -m scripts.reviews.cloudfront_distributions_config.run "$@"
    ;;

  dynamodb_finops)
    region_arg=""
    profiles=()

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --regions)
          if [[ $# -lt 2 ]]; then
            echo "--regions requires an argument" >&2
            exit 2
          fi
          region_arg="$2"
          shift 2
          ;;
        --profiles)
          shift
          while [[ $# -gt 0 && $1 != --* ]]; do
            profiles+=("$1")
            shift
          done
          ;;
        *)
          shift
          ;;
      esac
    done

    if [[ -z "$region_arg" ]]; then
      echo "dynamodb_finops requires --regions" >&2
      exit 2
    fi
    if [[ ${#profiles[@]} -eq 0 ]]; then
      echo "dynamodb_finops requires at least one --profiles entry" >&2
      exit 2
    fi

    IFS=',' read -r -a region_list <<< "$region_arg"
    for profile in "${profiles[@]}"; do
      for region in "${region_list[@]}"; do
        region_trimmed="${region//[[:space:]]/}"
        if [[ -z "$region_trimmed" ]]; then
          continue
        fi
        "${SCRIPT_DIR}/rr_dynamodb_finops.sh" "$region_trimmed" "$profile"
      done
    done
    exit 0
    ;;



  *)
    echo "Unknown review: $REVIEW" >&2
    exit 2
    ;;
esac
