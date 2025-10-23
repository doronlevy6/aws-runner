# scripts/reviews/amazonmq_cost_analysis/run.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/rr_mq.sh --regions us-east-1  --profiles  tea-fi --days 30 --period 300






Amazon MQ FinOps — Data Collector (DATA ONLY)
Collects minimal-but-sufficient signals to drive the MQ checklist:
- Idle brokers: connections/messages near zero
- Over-provisioned instance type: CPU/Memory low vs host type
- Multi-AZ vs Single-AZ
- Storage/Snapshots indicators
- Auto-recovery / auto minor upgrade flags
- Throughput vs limits (approx via messages/conn)
Outputs: CSV per profile + merged CSV (mq_all_profiles.csv)
"""

import argparse
import os
import sys
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.csvio import write_csv
from scripts.common.mq import mq_brokers_exist_in_region, list_brokers_full, list_snapshots_for_broker
from scripts.common.cloudwatch import get_metric_series, summarize, window

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
AMQ_NS = "AWS/AmazonMQ"

# ---------- Safe metric helpers ----------
def safe_series(cw, metric: str, broker_id: str, start, end, period: int, stat: str = "Average"):
    dims = [{"Name": "Broker", "Value": broker_id}]
    try:
        return get_metric_series(cw, AMQ_NS, metric, dims, start, end, period, stat=stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric:{metric}] skip ({code})", file=sys.stderr)
        return []
    except Exception as e:
        print(f"    [metric:{metric}] skip ({e})", file=sys.stderr)
        return []

def summarize_avg(series) -> Optional[float]:
    avg, _, _ = summarize(series)
    return avg

def summarize_p95(series) -> Optional[float]:
    _, p95, _ = summarize(series)
    return p95

def summarize_sum(series) -> Optional[float]:
    # get_metric_series returns datapoints with 'Value' already aggregated per period
    # summarize() returns (avg,p95,max). For sum we just multiply avg by count if available,
    # but here we’ll compute a simple sum directly.
    total = 0.0
    got = False
    for t, v in series:
        try:
            total += float(v)
            got = True
        except Exception:
            pass
    return total if got else None

def min_period_for_days(days: int) -> int:
    total_seconds = days * 86400
    raw = (total_seconds + 1440 - 1) // 1440
    return ((raw + 59) // 60) * 60 if raw % 60 != 0 else raw

def effective_period(days: int, requested: int) -> int:
    return max(requested, min_period_for_days(days))

# ---------- Heuristics (tunable) ----------
IDLE_CONN_THRESHOLD = 0.1     # avg connections ~0
IDLE_MSG_THRESHOLD  = 5.0     # ~no messages over window
LOW_CPU_P95         = 20.0    # over-provisioned if CPU p95 is very low
LOW_MEM_USAGE_FLAG  = 20.0    # if MemoryUsage avg <20% (if metric exists)

def assess_idle(conn_avg: Optional[float], msg_pub_sum: Optional[float], msg_cons_sum: Optional[float]) -> bool:
    is_idle_conn = (conn_avg is not None and conn_avg <= IDLE_CONN_THRESHOLD)
    msgs = (msg_pub_sum or 0.0) + (msg_cons_sum or 0.0)
    is_idle_msg = msgs <= IDLE_MSG_THRESHOLD
    return bool(is_idle_conn and is_idle_msg)

def assess_over_provisioned(cpu_p95: Optional[float], mem_avg_pct: Optional[float]) -> bool:
    cpu_ok = (cpu_p95 is not None and cpu_p95 <= LOW_CPU_P95)
    mem_ok = (mem_avg_pct is None) or (mem_avg_pct <= LOW_MEM_USAGE_FLAG)
    return bool(cpu_ok and mem_ok)

# ---------- Main collect ----------
def collect_for_broker(cw, broker: Dict, start, end, period: int) -> Dict:
    bid   = broker.get("BrokerId")
    name  = broker.get("BrokerName")
    eng   = broker.get("EngineType")         # ACTIVEMQ / RABBITMQ
    host  = broker.get("HostInstanceType")   # e.g., mq.m5.large
    dep   = broker.get("DeploymentMode")     # SINGLE_INSTANCE / ACTIVE_STANDBY_MULTI_AZ / CLUSTER_MULTI_AZ
    pub   = broker.get("PubliclyAccessible")
    auto_minor = broker.get("AutoMinorVersionUpgrade")
    users = (broker.get("Users") or [])
    logs  = broker.get("Logs") or {}
    cfgs  = broker.get("Configurations") or {}

    # Metrics (best-effort; skip if not available)
    cpu_avg = summarize_avg(safe_series(cw, "CPUUtilization",       bid, start, end, period, stat="Average"))
    cpu_p95 = summarize_p95(safe_series(cw, "CPUUtilization",       bid, start, end, period, stat="Average"))
    mem_avg = summarize_avg(safe_series(cw, "MemoryUsage",          bid, start, end, period, stat="Average"))  # percentage if exposed
    conn_avg= summarize_avg(safe_series(cw, "CurrentConnections",   bid, start, end, period, stat="Average"))
    msg_pub = summarize_sum(safe_series(cw, "MessagesPublished",    bid, start, end, period, stat="Sum"))
    msg_con = summarize_sum(safe_series(cw, "MessagesConsumed",     bid, start, end, period, stat="Sum"))

    idle = assess_idle(conn_avg, msg_pub, msg_con)
    over = assess_over_provisioned(cpu_p95, mem_avg)

    row = {
        # identity
        "broker_id": bid,
        "broker_name": name,
        "engine_type": eng,
        "host_instance_type": host,
        "deployment_mode": dep,
        "publicly_accessible": pub,
        "auto_minor_version_upgrade": auto_minor,

        # config hints
        "users_count": len(users),
        "logs_audit": bool(logs.get("Audit", False)),
        "logs_general": bool(logs.get("General", False)),

        # metrics
        "cpu_avg_pct": cpu_avg,
        "cpu_p95_pct": cpu_p95,
        "memory_usage_avg_pct": mem_avg,
        "connections_avg": conn_avg,
        "messages_published_sum": msg_pub,
        "messages_consumed_sum": msg_con,

        # heuristics / flags
        "candidate_idle": idle,
        "candidate_over_provisioned": over,

        # financial levers (derived hints)
        "candidate_single_az_possible": (dep == "ACTIVE_STANDBY_MULTI_AZ") and idle,
        "candidate_rightsize_hint": host,  # keep original for later mapping (e.g., mq.m5.large -> mq.t3.small)
    }
    return row

def collect_profile(profile: str, regions: List[str], days: int, period: int) -> List[Dict]:
    from scripts.common.aws_common import session_for_profile, sts_whoami
    rows: List[Dict] = []
    sess = session_for_profile(profile)
    acct_id, _ = sts_whoami(sess)
    start, end = window(days)

    for region in regions:
        cw = sess.client("cloudwatch", region_name=region, config=CFG)

        if not mq_brokers_exist_in_region(sess, region):
            print(f"[{profile}/{region}] no MQ brokers", file=sys.stderr)
            continue

        for br in list_brokers_full(sess, region):
            try:
                r = collect_for_broker(cw, br, start, end, period)
                r.update({
                    "profile": profile,
                    "account_id": acct_id,
                    "region": region,
                    # snapshots count (storage governance signal)
                    "snapshots_count": len(list_snapshots_for_broker(sess, region, br["BrokerId"])),
                })
                rows.append(r)
            except Exception as e:
                print(f"[{profile}/{region}] broker {br.get('BrokerId')}: skip ({e})", file=sys.stderr)
                continue
    return rows

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Amazon MQ FinOps — Data Collector (DATA ONLY)")
    p.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g., steam-fi tea-fi)")
    p.add_argument("--regions", required=True, help="Comma-separated regions, e.g. 'us-east-1,eu-west-1' or 'all'")
    p.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    p.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (>=60; default 300)")
    p.add_argument("--outdir", default=None, help="Output dir (default: outputs/amazonmq_cost_analysis_<timestamp>)")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        regions = parse_regions_arg(args.regions)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    eff_period = effective_period(args.days, args.period)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"amazonmq_cost_analysis_{ts}")
    os.makedirs(outdir, exist_ok=True)

    print("== Amazon MQ FinOps — Data Collector (DATA ONLY) ==", file=sys.stderr)
    print(f"  regions: {', '.join(regions)}", file=sys.stderr)
    print(f"  days={args.days}, period={eff_period}s", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

    all_rows: List[Dict] = []
    for prof in args.profiles:
        print(f"\n[profile: {prof}]", file=sys.stderr)
        try:
            sess = session_for_profile(prof)
        except ProfileNotFound:
            print(f"  ! profile '{prof}' not found in ~/.aws/config", file=sys.stderr)
            continue

        try:
            acct, arn = sts_whoami(sess)
            print(f"  account: {acct}", file=sys.stderr)
            print(f"  caller : {arn}", file=sys.stderr)
        except ClientError as e:
            print(f"  ! STS failed: {e}", file=sys.stderr)
            continue

        # collect
        rows = collect_profile(prof, regions, args.days, eff_period)
        if rows:
            all_rows.extend(rows)
            write_csv(os.path.join(outdir, f"amazonmq_{prof}.csv"), rows, rows[0].keys())
            print(f"  -> wrote {len(rows)} rows to {os.path.join(outdir, f'amazonmq_{prof}.csv')}", file=sys.stderr)
        else:
            print("  -> no data collected for this profile.", file=sys.stderr)

    if all_rows:
        write_csv(os.path.join(outdir, "mq_all_profiles.csv"), all_rows, all_rows[0].keys())
        print(f"\nALL DONE -> {os.path.join(outdir, 'mq_all_profiles.csv')}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
