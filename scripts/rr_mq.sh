#scripts/rr_mq.sh
#!/usr/bin/env bash
set -euo pipefail
# שימוש:
#   scripts/rr_mq.sh us-east-1,eu-west-1 steam-fi tea-fi
regions="${1:-us-east-1}"
shift || true
if [[ $# -eq 0 ]]; then
  echo "Usage: $0 <regions_csv> <profiles...>" >&2
  exit 2
fi
scripts/run_review.sh amazon_mq_finops --regions "$regions" --profiles "$@"
