#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
RDS Rightsizing Review - Metrics + CSV
מקבל --profiles --regions וגם --days/--period/--outdir.
עובר על כל מופעי RDS בכל אזור, מוריד מטריקות CloudWatch,
מסכם avg/p95/max, מחשב ניצולת IOPS משוערת, וכותב CSV פר-פרופיל + מאוחד.

כולל:
- טיפול נכון ב-Aurora: FreeLocalStorage ברמת DBClusterIdentifier (לא Instance).
- מעטפת בטוחה למטריקות (safe_series) כדי לא ליפול על InvalidParameter*.
- התאמה אוטומטית ל-period לפי מגבלת 1440 נקודות של CloudWatch.
"""

import argparse
import os
import sys
from typing import List, Dict, Tuple, Optional

from botocore.exceptions import ClientError, ProfileNotFound
from botocore.config import Config as BotoConfig
from datetime import datetime, timezone

# מודולים כלליים (Common/Core)
from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.rds import rds_instances_exist_in_region
from scripts.common.cloudwatch import (
    RDS_NS, rds_dim, get_metric_series, summarize, window
)
from scripts.common.csvio import write_csv

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

# ---------- עזרי חישוב מקומיים ----------

def is_aurora(engine: Optional[str]) -> bool:
    return (engine or "").lower().startswith("aurora")

def iops_capacity_for_instance(inst: Dict) -> Tuple[Optional[int], Optional[str], Optional[int], Optional[str]]:
    """
    החזרת (provisioned_iops, storage_type, allocated_storage_gib, note)
    - gp3: baseline 3000 אם לא הוגדר אחר; אם הוגדר Iops -> נשתמש בו.
    - io1/io2: לפי Iops מהתצורה.
    - gp2: baseline מקורב ≈ max(100, 3*GiB). (burst לא מחושב כאן)
    - Aurora: אין IOPS פר-מופע (storage משותף לקלסטר).
    """
    stype = (inst.get("StorageType") or "").lower()
    alloc_gib = inst.get("AllocatedStorage")  # עלול להיות None באורורה
    iops = inst.get("Iops")  # io1/io2/gp3 מותאם

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

def rightsizing_hint(cpu_avg, cpu_p95, free_mem_gib, iops_util_pct) -> str:
    notes = []
    if cpu_avg is not None and cpu_p95 is not None and cpu_avg < 10 and cpu_p95 < 40:
        if free_mem_gib is not None and free_mem_gib > 1:
            notes.append("Consider smaller instance (low CPU & memory headroom)")
    if cpu_p95 is not None and cpu_p95 > 80:
        notes.append("CPU pressure (p95>80%)")
    if free_mem_gib is not None and free_mem_gib < 1:
        notes.append("Low free memory (<1 GiB)")
    if iops_util_pct is not None and iops_util_pct > 80:
        notes.append("IOPS near capacity")
    return "; ".join(notes)

# ---------- CloudWatch wrappers בטוחים ----------

def safe_series(cw, namespace, metric, dimensions, start, end, period, stat="Average") -> List[float]:
    """מגן מפני InvalidParameter* ע״י החזרת סדרה ריקה במקום להפיל את כל האיסוף."""
    try:
        return get_metric_series(cw, namespace, metric, dimensions, start, end, period, stat=stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # נפוצים: InvalidParameterCombination / InvalidParameterValue / AccessDenied
        print(f"    [metric:{metric}] skip ({code})", file=sys.stderr)
        return []

# ---------- התאמת period אוטומטית (CloudWatch 1440 נק׳) ----------

def min_period_for_days(days: int) -> int:
    """
    CloudWatch GetMetricStatistics מגביל ל-1440 נקודות.
    period_min = ceil(days*86400 / 1440), מעוגל למכפלה של 60.
    דוגמה: 14 ימים -> 1209600/1440=840 -> מעגל ל-900.
    """
    total_seconds = days * 86400
    # ceiling division
    raw = (total_seconds + 1440 - 1) // 1440
    # עיגול כלפי מעלה למכפלה של 60
    return ((raw + 59) // 60) * 60 if raw % 60 != 0 else raw

def effective_period(days: int, requested: int) -> int:
    return max(requested, min_period_for_days(days))

# ---------- שלב איסוף ----------

def collect_for_instance(
    cw,
    inst_id: str,
    engine: Optional[str],
    cluster_id: Optional[str],
    start,
    end,
    period: int,
) -> Dict[str, Optional[float]]:
    """
    מוריד מטריקות RDS למופע בודד ומחזיר dict עם סיכומים avg/p95/max.
    התאמות:
      - Aurora: FreeLocalStorage בממד DBClusterIdentifier; אין FreeStorageSpace.
      - Non-Aurora: FreeStorageSpace בממד DBInstanceIdentifier; אין FreeLocalStorage.
    """
    out: Dict[str, Optional[float]] = {}

    aur = is_aurora(engine)

    # מטריקות instance-level שנאסוף תמיד
    instance_metrics = [
        "CPUUtilization",
        "DatabaseConnections",
        "ReadIOPS", "WriteIOPS",
        "ReadThroughput", "WriteThroughput",
        "FreeableMemory",
        "ReadLatency", "WriteLatency",
    ]

    for m in instance_metrics:
        series = safe_series(cw, RDS_NS, m, rds_dim(inst_id), start, end, period, stat="Average")
        avg, p95, mx = summarize(series)
        keypfx = m.lower()
        out[f"{keypfx}_avg"] = avg
        out[f"{keypfx}_p95"] = p95
        out[f"{keypfx}_max"] = mx

    # Storage metrics – שונים בין Aurora/Non-Aurora
    if aur:
        if cluster_id:
            dim = [{"Name": "DBClusterIdentifier", "Value": cluster_id}]
            series = safe_series(cw, RDS_NS, "FreeLocalStorage", dim, start, end, period, stat="Average")
            avg, p95, mx = summarize(series)
            out["freelocalstorage_avg"] = avg
            out["freelocalstorage_p95"] = p95
            out["freelocalstorage_max"] = mx
    else:
        series = safe_series(cw, RDS_NS, "FreeStorageSpace", rds_dim(inst_id), start, end, period, stat="Average")
        avg, p95, mx = summarize(series)
        out["freestoragespace_avg"] = avg
        out["freestoragespace_p95"] = p95
        out["freestoragespace_max"] = mx

    # המרות נוחות
    out["freeable_mem_avg_gib"] = gib(out.get("freeablememory_avg"))

    return out

def collect_profile(profile: str, regions: List[str], days: int, period: int) -> List[Dict]:
    """
    איסוף לכל הפרופיל בכל האזורים הנתונים:
    - זיהוי מופעי RDS
    - שליפת מטריקות CloudWatch וסיכומים
    - חישובי ניצולת IOPS (אם יש קאפ ידוע)
    """
    rows: List[Dict] = []
    sess = session_for_profile(profile)
    sts_acct, _ = sts_whoami(sess)

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

                    prov_iops, storage_type, alloc_gib, cap_note = iops_capacity_for_instance(inst)

                    # מטריקות
                    met = collect_for_instance(cw, inst_id, engine, cluster, start, end, period)

                    # IOPS ניצולת מול קאפ (אם יש)
                    iops_util_pct = None
                    read_p95 = met.get("readiops_p95")
                    write_p95 = met.get("writeiops_p95")
                    if prov_iops and (read_p95 is not None or write_p95 is not None):
                        peak = max(read_p95 or 0, write_p95 or 0)
                        if prov_iops > 0:
                            iops_util_pct = (peak / prov_iops) * 100.0

                    row = {
                        "profile": profile,
                        "account_id": sts_acct,
                        "region": region,
                        "db_instance_id": inst_id,
                        "engine": engine,
                        "db_instance_class": iclass,
                        "availability_zone": az,
                        "vpc_id": vpc,
                        "aurora_cluster_id": cluster,

                        "storage_type": storage_type or inst.get("StorageType"),
                        "allocated_storage_gib": alloc_gib,
                        "provisioned_iops": prov_iops,
                        "iops_cap_note": cap_note,

                        # CPU
                        "cpu_avg_pct":  met.get("cpuutilization_avg"),
                        "cpu_p95_pct":  met.get("cpuutilization_p95"),
                        "cpu_max_pct":  met.get("cpuutilization_max"),

                        # Connections
                        "connections_avg": met.get("databaseconnections_avg"),
                        "connections_p95": met.get("databaseconnections_p95"),
                        "connections_max": met.get("databaseconnections_max"),

                        # IOPS
                        "read_iops_avg":  met.get("readiops_avg"),
                        "read_iops_p95":  met.get("readiops_p95"),
                        "read_iops_max":  met.get("readiops_max"),
                        "write_iops_avg": met.get("writeiops_avg"),
                        "write_iops_p95": met.get("writeiops_p95"),
                        "write_iops_max": met.get("writeiops_max"),
                        "iops_util_pct":  iops_util_pct,

                        # Throughput (Bytes/sec)
                        "read_throughput_avg_Bps":  met.get("readthroughput_avg"),
                        "read_throughput_p95_Bps":  met.get("readthroughput_p95"),
                        "read_throughput_max_Bps":  met.get("readthroughput_max"),
                        "write_throughput_avg_Bps": met.get("writethroughput_avg"),
                        "write_throughput_p95_Bps": met.get("writethroughput_p95"),
                        "write_throughput_max_Bps": met.get("writethroughput_max"),

                        # Memory / Storage
                        "freeable_mem_avg_gib":          met.get("freeable_mem_avg_gib"),
                        "free_storage_space_avg_bytes":  met.get("freestoragespace_avg"),
                        "free_local_storage_avg_bytes":  met.get("freelocalstorage_avg"),

                        # Latency (seconds)
                        "read_latency_avg_sec":  met.get("readlatency_avg"),
                        "read_latency_p95_sec":  met.get("readlatency_p95"),
                        "read_latency_max_sec":  met.get("readlatency_max"),
                        "write_latency_avg_sec": met.get("writelatency_avg"),
                        "write_latency_p95_sec": met.get("writelatency_p95"),
                        "write_latency_max_sec": met.get("writelatency_max"),
                    }

                    row["rightsizing_note"] = rightsizing_hint(
                        row["cpu_avg_pct"], row["cpu_p95_pct"],
                        row["freeable_mem_avg_gib"], row["iops_util_pct"]
                    )

                    rows.append(row)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            print(f"[{profile}/{region}] skipping due to error: {code}", file=sys.stderr)
            continue

    return rows

# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser(description="RDS Rightsizing - Metrics + CSV")
    p.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g., steam-fi tea-fi)")
    p.add_argument("--regions", required=True, help="Comma-separated regions, e.g. 'us-east-1,eu-west-1'")
    p.add_argument("--days", type=int, default=14, help="Lookback window in days (default 14)")
    p.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (60 or multiple; default 300)")
    p.add_argument("--outdir", default=None, help="Output dir (default: outputs/rds_rightsizing_<timestamp>)")
    return p.parse_args()

def main():
    args = parse_args()

    try:
        regions = parse_regions_arg(args.regions)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    # period אפקטיבי (נמנע מ-InvalidParameterCombination)
    eff_period = effective_period(args.days, args.period)

    # נתיב פלט
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"rds_rightsizing_{ts}")
    os.makedirs(outdir, exist_ok=True)

    all_rows: List[Dict] = []

    print("== RDS Rightsizing: metrics & CSV ==", file=sys.stderr)
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

        # וולידציה קלה: האם באזור בכלל יש RDS
        active_regions = [r for r in regions if rds_instances_exist_in_region(sess, r)]
        if not active_regions:
            print("  (no RDS instances in selected regions)", file=sys.stderr)
            continue

        # איסוף
        rows = collect_profile(prof, active_regions, args.days, eff_period)
        if rows:
            all_rows.extend(rows)
            # כתיבה פר-פרופיל
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
