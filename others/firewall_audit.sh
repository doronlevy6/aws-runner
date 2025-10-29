#!/usr/bin/env bash
# others/firewall_audit.sh
# One-click audit for AWS Network Firewall: endpoints, CloudTrail (who/when), CE costs, and route usage.

set -euo pipefail
trap 'echo "[ERROR] בשורה ${LINENO}: פקודה נכשלה (קוד $?)"' ERR

# ===== CONFIG =====
REGION="eu-central-1"
FIREWALL_NAME="network-firewall-dev-NAT"

# חלון זמן לאירועי CloudTrail (שנה לערכים לפי הצורך)
CT_START="2025-10-01T00:00:00Z"
CT_END="2025-10-31T23:59:59Z"

# חלון זמן ל-Cost Explorer (DAILY)
CE_START="2025-10-01"
CE_END="2025-10-31"

# ===== PATHS =====
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
TS="$(date +%Y%m%d%H%M)"
OUT_DIR="$ROOT_DIR/outputs/firewall_audit_$TS"
mkdir -p "$OUT_DIR"

echo "==> OUTPUT DIR: $OUT_DIR"

# ===== PRECHECKS =====
if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: 'jq' is required. Install with: brew install jq"
  exit 1
fi

if ! aws sts get-caller-identity >/dev/null 2>&1; then
  echo "ERROR: AWS CLI not authenticated. Run: source scripts/use-aws <profile>"
  exit 1
fi

# ===== 1) CURRENT FIREWALL + ENDPOINTS =====
echo "==> Describe firewall and endpoints (current state)..."
FW_JSON="$OUT_DIR/firewall_describe_raw.json"
aws network-firewall describe-firewall \
  --region "$REGION" \
  --firewall-name "$FIREWALL_NAME" \
  > "$FW_JSON"

# endpoints_now.csv: AZ,EndpointId,Status
jq -r '
  .FirewallStatus.SyncStates
  | to_entries
  | map({
      AZ: .key,
      EndpointId: (.value.Attachment.EndpointId // ""),
      Status: (.value.Attachment.Status // "")
    })
  | (["AZ","EndpointId","Status"] , (.[] | [.AZ,.EndpointId,.Status]))
  | @csv
' "$FW_JSON" > "$OUT_DIR/endpoints_now.csv"

# נחלץ גם metadata קצר (VpcId, SubnetMappings, PolicyArn)
jq -r '
  {
    FirewallName: .FirewallName,
    FirewallArn: .FirewallArn,
    VpcId: .VpcId,
    SubnetCount: (.SubnetMappings | length),
    PolicyArn: .FirewallPolicyArn
  } | to_entries
  | .[] | "\(.key): \(.value|tostring)"
' "$FW_JSON" > "$OUT_DIR/firewall_meta.txt"

# רשימת EndpointIds לשימוש בהמשך (תואם macOS Bash 3.2 – בלי mapfile)
EP_IDS=()
while IFS= read -r _ep; do
  [[ -n "$_ep" ]] && EP_IDS+=("$_ep")
done < <(jq -r '.FirewallStatus.SyncStates | to_entries | .[].value.Attachment.EndpointId // empty' "$FW_JSON")

# ===== 2) ROUTE TABLES → DO ROUTES POINT TO THESE ENDPOINTS? =====
echo "==> Resolve route-tables that point to each endpoint..."
RT_CSV="$OUT_DIR/routes_by_endpoint.csv"
echo "EndpointId,RouteTableId,DestinationCidr,State" > "$RT_CSV"
for ep in "${EP_IDS[@]:-}"; do
  if [[ -n "$ep" ]]; then
    aws ec2 describe-route-tables --region "$REGION" \
      --filters "Name=route.vpc-endpoint-id,Values=$ep" \
      --query 'RouteTables[].{Id:RouteTableId,Routes:Routes[?VpcEndpointId==`'"$ep"'`]} ' \
      | jq -r --arg ep "$ep" '
        .[] as $rt
        | ($rt.Routes[]? | [$ep, $rt.Id, (.DestinationCidrBlock // .DestinationPrefixListId // ""), (.State // "")] | @csv)
      ' >> "$RT_CSV" || true
  fi
done

# ===== 3) CLOUDTRAIL: WHO DID WHAT & WHEN (Firewall create/associate/disassociate/delete) =====
echo "==> Pull CloudTrail events (who/when/what)..."
CT_RAW="$OUT_DIR/cloudtrail_raw.json"
aws cloudtrail lookup-events \
  --region "$REGION" \
  --lookup-attributes AttributeKey=EventSource,AttributeValue=network-firewall.amazonaws.com \
  --start-time "$CT_START" \
  --end-time "$CT_END" \
  > "$CT_RAW"

# סיכום-CSV ממוקד
CT_CSV="$OUT_DIR/cloudtrail_events.csv"
{
  echo "EventTime,Username,EventName,FirewallName,SubnetIdsAdded,SubnetIdsRemoved"
  jq -r '
    .Events[]?
    | . as $e
    | (try (.CloudTrailEvent | fromjson) catch {}) as $c
    | [
        ($e.EventTime // ""),
        ($e.Username // ""),
        ($e.EventName // ""),
        ($c.requestParameters.firewallName // $c.responseElements.firewall.firewallName // ""),
        ([
          ($c.requestParameters.subnetMappings[]?.subnetId // empty),
          ($c.responseElements.firewall.subnetMappings[]?.subnetId // empty)
        ] | unique | join(";")),
        ([
          ($c.requestParameters.subnetMappings[]?.subnetId // empty)
        ] | unique | join(";"))
      ] | @csv
  ' "$CT_RAW"
} > "$CT_CSV"

# ===== 4) COST EXPLORER: DAILY COST + USAGE (Endpoint-Hour) =====
echo "==> Fetch Cost Explorer daily (UNBLENDED_COST, USAGE_QUANTITY) grouped by USAGE_TYPE..."
CE_RAW="$OUT_DIR/ce_raw.json"
read -r -d '' CE_FILTER <<'JSON'
{
  "And": [
    { "Dimensions": { "Key": "SERVICE", "Values": ["AWS Network Firewall"] } },
    { "Not": { "Dimensions": { "Key": "RECORD_TYPE", "Values": ["Tax"] } } }
  ]
}
JSON

aws ce get-cost-and-usage \
  --time-period Start="$CE_START",End="$CE_END" \
  --granularity DAILY \
  --metrics UNBLENDED_COST USAGE_QUANTITY \
  --group-by Type=DIMENSION,Key=USAGE_TYPE \
  --filter "$CE_FILTER" \
  > "$CE_RAW"

CE_CSV="$OUT_DIR/cost_by_usage_type.csv"
{
  echo "date,usage_type,unblended_cost_usd,usage_quantity"
  jq -r '
    .ResultsByTime[] as $r
    | if ($r.Groups|length) > 0 then
        $r.Groups[]
        | [
            $r.TimePeriod.Start,
            (.Keys[0] // "UNKNOWN"),
            (.Metrics.UnblendedCost.Amount // "0"),
            (.Metrics.UsageQuantity.Amount // "0")
          ]
      else
        [
          $r.TimePeriod.Start,
          "TOTAL",
          ($r.Total.UnblendedCost.Amount // "0"),
          ($r.Total.UsageQuantity.Amount // "0")
        ]
      end
    | @csv
  ' "$CE_RAW"
} > "$CE_CSV"

# ===== 5) MINI REPORT =====
echo "==> Compose mini summary..."
SUMMARY="$OUT_DIR/_SUMMARY.txt"
{
  echo "AWS Network Firewall Audit"
  echo "Region: $REGION"
  echo "Firewall: $FIREWALL_NAME"
  echo
  echo "[Endpoints now]"
  jq -r '.FirewallStatus.SyncStates | to_entries | .[] | "\(.key): \(.value.Attachment.Status) - \(.value.Attachment.EndpointId//"")"' "$FW_JSON"
  echo
  echo "[CloudTrail window]"
  echo "From: $CT_START  To: $CT_END"
  echo "Key events to look for: CreateFirewall, AssociateSubnets, DisassociateSubnets, DeleteFirewall"
  echo
  echo "[Cost Explorer window]"
  echo "From: $CE_START  To: $CE_END"
  echo "CSV: cost_by_usage_type.csv (look for *Endpoint-Hour)"
} > "$SUMMARY"

echo
echo "✅ Done!"
echo "Files created under: $OUT_DIR"
ls -1 "$OUT_DIR"