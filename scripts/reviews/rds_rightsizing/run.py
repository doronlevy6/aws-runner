#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RDS/Aurora Rightsizing Collector — Minimal (DATA ONLY)
אוסף רק נתונים הכרחיים להחלטה עתידית על ריסייזינג.
אין חישובי שעות/תמחור/Latency/Throughput/המלצות.
פלט: CSV פר-פרופיל + CSV מאוחד (rds_all_profiles.csv)
"""

import argparse
import os
import sys
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone

from botocore.exceptions import ClientError, ProfileNotFound
from botocore.config import Config as BotoConfig

# מודולים משותפים מהפרויקט שלך
from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.rds import rds_instances_exist_in_region
from scripts.common.cloudwatch import (
    RDS_NS, rds_dim, get_metric_series, summarize, window
)
from scripts.common.csvio import write_csv

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

# ---------- Helpers ----------
def is_aurora(engine: Optional[str]) -> bool:
    return (engine or "").lower().startswith("aurora")

def iops_capacity_for_instance(inst: Dict) -> Tuple[Optional[int], Optional[str], Optional[int], Optional[str]]:
    """
    מחזיר (provisioned_iops, storage_type, allocated_storage_gib, note)
    gp3: baseline 3000 אם לא הוגדר; io1/io2: לפי Iops; gp2: baseline≈max(100, 3*GiB); Aurora: storage שיתופי (אין cap instance).
    """
    stype = (inst.get("StorageType") or "").lower()
    alloc_gib = inst.get("AllocatedStorage")
    iops = inst.get("Iops")

    if is_aurora(inst.get("Engine")):
        return (None, None, None, "aurora-shared-storage")

    if stype == "gp3":
        prov = iops if iops else 3000
        return (prov, "gp3", alloc_gib, "gp3 baseline if not set")

    if stype in ("io1", "io2"):
        prov = iops if iops else None
        return (prov, stype, alloc_gib, "provisioned")

    if stype == "gp2" and alloc_gib:
        baseline = max(100, int(3 * alloc_gib))
        return (baseline, "gp2", alloc_gib, "baseline≈3*GiB (min 100)")

    return (None, stype or None, alloc_gib, None)

def gib(bytes_val: Optional[float]) -> Optional[float]:
    if bytes_val is None:
        return None
    try:
        return float(bytes_val) / (1024 ** 3)
    except Exception:
        return None

# ---------- CloudWatch ----------
def safe_series(cw, namespace, metric, dimensions, start, end, period, stat="Average"):
    try:
        return get_metric_series(cw, namespace, metric, dimensions, start, end, period, stat=stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric:{metric}] skip ({code})", file=sys.stderr)
        return []

def min_period_for_days(days: int) -> int:
    total_seconds = days * 86400
    raw = (total_seconds + 1440 - 1) // 1440
    return ((raw + 59) // 60) * 60 if raw % 60 != 0 else raw

def effective_period(days: int, requested: int) -> int:
    return max(requested, min_period_for_days(days))

# ---------- Collect ----------
def collect_for_instance(cw, inst_id: str, start, end, period: int) -> Dict[str, Optional[float]]:
    """
    מינימום מטריקות ברמת instance לצורך החלטה עתידית:
    CPU (avg,p95), Connections (avg), FreeableMemory (avg GiB), Read/Write IOPS (p95).
    """
    out: Dict[str, Optional[float]] = {}

    # CPU
    cpu = safe_series(cw, RDS_NS, "CPUUtilization", rds_dim(inst_id), start, end, period, stat="Average")
    out["cpu_avg_pct"], out["cpu_p95_pct"], _ = summarize(cpu)

    # Connections (מדד שימוש כללי)
    con = safe_series(cw, RDS_NS, "DatabaseConnections", rds_dim(inst_id), start, end, period, stat="Average")
    out["connections_avg"], _, _ = summarize(con)

    # Memory
    mem = safe_series(cw, RDS_NS, "FreeableMemory", rds_dim(inst_id), start, end, period, stat="Average")
    mem_avg, _, _ = summarize(mem)
    out["freeable_mem_avg_gib"] = gib(mem_avg)

    # IOPS p95 (לזיהוי צוואר בקבוק)
    r_iops = safe_series(cw, RDS_NS, "ReadIOPS", rds_dim(inst_id), start, end, period, stat="Average")
    w_iops = safe_series(cw, RDS_NS, "WriteIOPS", rds_dim(inst_id), start, end, period, stat="Average")
    _, out["read_iops_p95"], _  = summarize(r_iops)
    _, out["write_iops_p95"], _ = summarize(w_iops)

    return out

def collect_profile(profile: str, regions: List[str], days: int, period: int) -> List[Dict]:
    rows: List[Dict] = []
    sess = session_for_profile(profile)
    acct_id, _ = sts_whoami(sess)
    start, end = window(days)

    for region in regions:
        rds = sess.client("rds", region_name=region, config=CFG)
        cw  = sess.client("cloudwatch", region_name=region, config=CFG)

        try:
            paginator = rds.get_paginator("describe_db_instances")
            for page in paginator.paginate():
                for inst in page.get("DBInstances", []):
                    inst_id = inst["DBInstanceIdentifier"]
                    engine  = inst.get("Engine")
                    iclass  = inst.get("DBInstanceClass")
                    az      = inst.get("AvailabilityZone")
                    vpc     = inst.get("DBSubnetGroup", {}).get("VpcId")
                    cluster = inst.get("DBClusterIdentifier")
                    multi_az = bool(inst.get("MultiAZ"))

                    prov_iops, storage_type, alloc_gib, cap_note = iops_capacity_for_instance(inst)

                    met = collect_for_instance(cw, inst_id, start, end, period)

                    # IOPS utilization מול cap (gp3/io1/io2 בלבד)
                    iops_util_pct = None
                    read_p95 = met.get("read_iops_p95")
                    write_p95 = met.get("write_iops_p95")
                    if prov_iops and (read_p95 is not None or write_p95 is not None):
                        peak = max(read_p95 or 0, write_p95 or 0)
                        if prov_iops > 0:
                            iops_util_pct = (peak / prov_iops) * 100.0

                    row = {
                        # מזהים
                        "profile": profile,
                        "account_id": acct_id,
                        "region": region,
                        "db_instance_id": inst_id,
                        "engine": engine,
                        "db_instance_class": iclass,
                        "multi_az": multi_az,
                        "availability_zone": az,
                        "vpc_id": vpc,
                        "aurora_cluster_id": cluster,  # אינדיקציה בלבד

                        # דיסק (תצורה בלבד, לא שימוש)
                        "storage_type": storage_type or inst.get("StorageType"),
                        "allocated_storage_gib": alloc_gib,
                        "provisioned_iops": prov_iops,
                        "iops_cap_note": cap_note,

                        # מדדים קריטיים בלבד
                        "cpu_avg_pct":  met.get("cpu_avg_pct"),
                        "cpu_p95_pct":  met.get("cpu_p95_pct"),
                        "freeable_mem_avg_gib": met.get("freeable_mem_avg_gib"),
                        "connections_avg": met.get("connections_avg"),
                        "read_iops_p95":  met.get("read_iops_p95"),
                        "write_iops_p95": met.get("write_iops_p95"),
                        "iops_util_pct":  iops_util_pct,
                    }

                    rows.append(row)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"[{profile}/{region}] skipping due to error: {code}", file=sys.stderr)
            continue

    return rows

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="RDS/Aurora Rightsizing Collector — Minimal (DATA ONLY)")
    p.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g., steam-fi tea-fi)")
    p.add_argument("--regions", required=True, help="Comma-separated regions, e.g. 'us-east-1,eu-west-1'")
    p.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    p.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (>=60; default 300)")
    p.add_argument("--outdir", default=None, help="Output dir (default: outputs/rds_rightsize_min_<timestamp>)")
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
    outdir = args.outdir or os.path.join("outputs", f"rds_rightsize_min_{ts}")
    os.makedirs(outdir, exist_ok=True)

    all_rows: List[Dict] = []

    print("== RDS/Aurora Rightsizing Collector — Minimal (DATA ONLY) ==", file=sys.stderr)
    print(f"  regions: {', '.join(regions)}", file=sys.stderr)
    print(f"  days={args.days}, period={eff_period}s", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

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

        active_regions = [r for r in regions if rds_instances_exist_in_region(sess, r)]
        if not active_regions:
            print("  (no RDS instances in selected regions)", file=sys.stderr)
            continue

        rows = collect_profile(prof, active_regions, args.days, eff_period)
        if rows:
            all_rows.extend(rows)
            write_csv(os.path.join(outdir, f"rds_{prof}.csv"), rows, rows[0].keys())
            print(f"  -> wrote {len(rows)} rows to {os.path.join(outdir, f'rds_{prof}.csv')}", file=sys.stderr)
        else:
            print("  -> no data collected for this profile.", file=sys.stderr)

    if all_rows:
        write_csv(os.path.join(outdir, "rds_all_profiles.csv"), all_rows, all_rows[0].keys())
        print(f"\nALL DONE -> {os.path.join(outdir, 'rds_all_profiles.csv')}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())
