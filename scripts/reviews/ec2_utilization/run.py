#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EC2 Utilization Audit — 14 days @ 5-min CPU, daily network sums
Outputs:
  - <outdir>/ec2_<profile>.csv
  - <outdir>/ec2_all_profiles.csv
  - <outdir>/category_summary.csv
Columns:
  account_id, account_name, region, instance_id, name, type,
  cpu_avg_pct, cpu_p95_pct, net_mb_per_day, cpu_credit_balance, category, note
"""

import argparse
import csv
import os
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})


# ---------- time & math helpers ----------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def chunk_windows(start: datetime, end: datetime, max_days: int) -> List[Tuple[datetime, datetime]]:
    """Split [start, end] into <= max_days windows (for CW 1440-point limit)."""
    windows = []
    cur = start
    step = timedelta(days=max_days)
    while cur < end:
        nxt = min(cur + step, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows

def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0

def p95(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    idx = int(round(0.95 * (len(ys) - 1)))
    return ys[idx]


# ---------- data shaping ----------

FIELD_ORDER = [
    "account_id", "account_name", "region", "instance_id", "name", "type",
    "cpu_avg_pct", "cpu_p95_pct", "net_mb_per_day", "cpu_credit_balance", "category", "note"
]

def categorize(cpu_avg: float, cpu_p95_: float, net_mb_per_day: float) -> Tuple[str, str]:
    """
    Categories (priority order):
      1) High Utilization – CPU avg ≥ 40% or p95 ≥ 70%
      2) Network-Heavy – CPU < 10% and traffic > 5 GB/day
      3) Idle / Over-Provisioned – CPU avg < 5% and traffic < 200 MB/day
      4) Moderate – otherwise
    """
    if cpu_avg >= 40.0 or cpu_p95_ >= 70.0:
        return ("High Utilization", "Consider scale-up/RI/SP alignment if persistent.")
    if cpu_avg < 10.0 and net_mb_per_day > (5 * 1024):
        return ("Network-Heavy", "CPU low but traffic high; check data transfer or placement.")
    if cpu_avg < 5.0 and net_mb_per_day < 200.0:
        return ("Idle / Over-Provisioned", "Very low CPU & traffic; candidate for stop/resize.")
    return ("Moderate", "Balanced utilization; monitor.")


# ---------- AWS helpers ----------

def session_for_profile(profile: str):
    try:
        return boto3.Session(profile_name=profile)
    except ProfileNotFound:
        print(f"[{profile}] AWS profile not found", file=sys.stderr)
        raise

def sts_whoami(sess) -> Tuple[str, str]:
    sts = sess.client("sts", config=CFG)
    ident = sts.get_caller_identity()
    return ident["Account"], ident["Arn"]

def list_regions(sess, regions_arg: str) -> List[str]:
    if regions_arg.lower() == "all":
        ec2 = sess.client("ec2", region_name="us-east-1", config=CFG)
        resp = ec2.describe_regions(AllRegions=False)
        return sorted([r["RegionName"] for r in resp.get("Regions", [])])
    return [r.strip() for r in regions_arg.split(",") if r.strip()]

def list_running_instances(sess, region: str) -> List[Dict]:
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    instances = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}],
        PaginationConfig={"PageSize": 1000}
    ):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                tags = {t["Key"]: t.get("Value", "") for t in inst.get("Tags", []) or []}
                instances.append({
                    "InstanceId": inst["InstanceId"],
                    "InstanceType": inst["InstanceType"],
                    "LaunchTime": inst.get("LaunchTime"),
                    "Tags": tags,
                    "Name": tags.get("Name", "")
                })
    return instances

def is_t_family(instance_type: str) -> bool:
    return instance_type.startswith(("t2.", "t3.", "t3a.", "t4g."))


# ---------- CloudWatch collectors ----------

def cw_get_metric_data(cw, queries: List[Dict], start: datetime, end: datetime):
    return cw.get_metric_data(MetricDataQueries=queries, StartTime=start, EndTime=end, ScanBy="TimestampAscending")

def fetch_cpu_points_5m(cw, instance_id: str, start: datetime, end: datetime) -> List[float]:
    """CPUUtilization 5-min averages, chunked for CW limits."""
    points: List[float] = []
    for s, e in chunk_windows(start, end, max_days=5):
        resp = cw_get_metric_data(cw, [{
            "Id": "cpu",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]
                },
                "Period": 300,
                "Stat": "Average"
            },
            "ReturnData": True
        }], s, e)
        for r in resp.get("MetricDataResults", []):
            points.extend(r.get("Values", []))
    return points

def fetch_network_daily_mb(cw, instance_id: str, start: datetime, end: datetime) -> float:
    """Average (NetworkIn+NetworkOut) MB/day."""
    resp = cw_get_metric_data(cw, [
        {"Id": "in_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2","MetricName": "NetworkIn",
         "Dimensions": [{"Name": "InstanceId","Value": instance_id}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True},
        {"Id": "out_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2","MetricName": "NetworkOut",
         "Dimensions": [{"Name": "InstanceId","Value": instance_id}]}, "Period": 86400, "Stat": "Sum"}, "ReturnData": True}
    ], start, end)

    in_vals = out_vals = []
    for r in resp.get("MetricDataResults", []):
        if r["Id"] == "in_sum": in_vals = r.get("Values", [])
        elif r["Id"] == "out_sum": out_vals = r.get("Values", [])
    days = min(len(in_vals), len(out_vals))
    if days == 0:
        return 0.0
    total_bytes = sum(in_vals[:days]) + sum(out_vals[:days])
    return (total_bytes / (1024 * 1024)) / days

def fetch_cpu_credit_min(cw, instance_id: str, start: datetime, end: datetime) -> Optional[float]:
    """Return minimum CPUCreditBalance over the window."""
    mins: List[float] = []
    for s, e in chunk_windows(start, end, max_days=5):
        resp = cw_get_metric_data(cw, [{
            "Id": "credit",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUCreditBalance",
                    "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]
                },
                "Period": 300,
                "Stat": "Minimum"
            },
            "ReturnData": True
        }], s, e)
        for r in resp.get("MetricDataResults", []):
            vals = r.get("Values", [])
            if vals:
                mins.append(min(vals))
    return min(mins) if mins else None


# ---------- IO ----------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(path: str, rows: List[Dict], field_order: List[str]):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=field_order)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in field_order})


# ---------- MAIN ----------

def parse_args():
    p = argparse.ArgumentParser(description="EC2 Utilization Audit (14d window @ 5m CPU, daily network sums)")
    p.add_argument("--profiles", nargs="+", required=True)
    p.add_argument("--regions", required=True)
    p.add_argument("--days", type=int, default=14)
    p.add_argument("--outdir", default=None)
    return p.parse_args()

def main():
    args = parse_args()
    end = utc_now()
    start = end - timedelta(days=args.days)
    ts = end.strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"ec2_utilization_{ts}")
    ensure_dir(outdir)

    all_rows: List[Dict] = []
    cat_counter = Counter()

    for profile in args.profiles:
        sess = session_for_profile(profile)
        account_id, _ = sts_whoami(sess)
        account_name = sess.profile_name
        regions = list_regions(sess, args.regions)
        profile_rows: List[Dict] = []

        for region in regions:
            cw = sess.client("cloudwatch", region_name=region, config=CFG)
            try:
                instances = list_running_instances(sess, region)
            except ClientError as e:
                print(f"[{profile}/{region}] describe_instances failed: {e}", file=sys.stderr)
                continue

            for inst in instances:
                iid = inst["InstanceId"]
                itype = inst["InstanceType"]
                name = inst.get("Name", "")

                # CPU
                cpu_points = []
                try:
                    cpu_points = fetch_cpu_points_5m(cw, iid, start, end)
                except ClientError as e:
                    print(f"[{profile}/{region}/{iid}] CPUUtilization error: {e}", file=sys.stderr)
                cpu_avg = mean(cpu_points)
                cpu_p95_ = p95(cpu_points)

                # Network
                net_mb_day = 0.0
                try:
                    net_mb_day = fetch_network_daily_mb(cw, iid, start, end)
                except ClientError as e:
                    print(f"[{profile}/{region}/{iid}] NetworkIn/Out error: {e}", file=sys.stderr)

                # Credits
                credit_min = None
                if is_t_family(itype):
                    try:
                        credit_min = fetch_cpu_credit_min(cw, iid, start, end)
                    except ClientError as e:
                        print(f"[{profile}/{region}/{iid}] CPUCreditBalance error: {e}", file=sys.stderr)

                category, note = categorize(cpu_avg, cpu_p95_, net_mb_day)
                row = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "region": region,
                    "instance_id": iid,
                    "name": name,
                    "type": itype,
                    "cpu_avg_pct": round(cpu_avg, 2),
                    "cpu_p95_pct": round(cpu_p95_, 2),
                    "net_mb_per_day": round(net_mb_day, 2),
                    "cpu_credit_balance": "" if credit_min is None else round(credit_min, 2),
                    "category": category,
                    "note": note
                }
                profile_rows.append(row)
                all_rows.append(row)
                cat_counter[category] += 1

        write_csv(os.path.join(outdir, f"ec2_{profile}.csv"), profile_rows, FIELD_ORDER)

    write_csv(os.path.join(outdir, "ec2_all_profiles.csv"), all_rows, FIELD_ORDER)
    write_csv(os.path.join(outdir, "category_summary.csv"),
              [{"category": k, "count": v} for k, v in sorted(cat_counter.items())],
              ["category", "count"])

    print(f"✅ Done. {len(all_rows)} rows → {outdir}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
