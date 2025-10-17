# scripts/rr.sh  (נשאר כמו שהצעתי — מתאים בדיוק למודל "אתה נותן אזורים")
#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   scripts/rr.sh us-east-1,us-east-2 steam-fi tea-fi
#   scripts/rr.sh eu-west-1 tea-fi

if [[ $# < 2 ]]; then
  echo "Usage: $0 <regions-csv> <profile...>" >&2
  echo "Example: $0 us-east-1,us-east-2 steam-fi tea-fi" >&2
  exit 2
fi

REGIONS="$1"; shift
PROFILES=("$@")

if [[ -d ".venv" ]]; then
  source .venv/bin/activate || true
fi

exec scripts/run_review.sh rds_rightsizing --regions "${REGIONS}" --profiles "${PROFILES[@]}"
