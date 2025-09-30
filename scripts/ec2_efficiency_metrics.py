#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EC2 Efficiency Metrics – usage + inventory (FinOps-ready)
=========================================================
שולף *מדדים כמותיים* ל-Rightsizing/יעילות לכל מופעי EC2 ומצרף אינבנטורי בסיסי.
מייצא CSV/JSON אחד לכל חשבון/רשימת אזורים.

מה כלול (ל־N הימים האחרונים, ברירת מחדל 30):
- CPUUtilization: ממוצע, p95, מקסימום
- NetworkIn/Out: סיכום ב-GB (סה״כ תעבורה)
- **NetworkPacketsIn/Out: סיכום (count) – חדש**
- CPUCreditBalance (ל-T* בלבד): מינימום
- **CPUCreditUsage (ל-T* בלבד): סיכום – חדש**
- StatusCheckFailed: מקסימום (אם >0 – בעיה)
- MemoryUsed% (אם מותקן CloudWatch Agent): ממוצע, p95, מקסימום  [best-effort]
- Compute Optimizer (אופציונלי): המלצה, סוג מומלץ, PerformanceRisk, EstimatedMonthlySavings

תלויות:
  pip install boto3 python-dateutil

הרשאות דרושות:
  ec2:DescribeInstances
  ec2:DescribeRegions
  cloudwatch:GetMetricData
  compute-optimizer:GetEC2InstanceRecommendations        (אם --with-co)
"""
import os
import sys
import csv
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import boto3
import botocore
from dateutil import tz


# ---------- helpers ----------
def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def pct(x) -> str:
    return "" if x is None else f"{x:.2f}"


def num(x) -> str:
    return "" if x is None else f"{x:.2f}"


def gb(bytes_total: Optional[float]) -> Optional[float]:
    return None if bytes_total is None else (bytes_total / (1024 ** 3))


def is_t_family(instance_type: str) -> bool:
    return instance_type.lower().startswith("t")


# ---------- regions ----------
def get_regions(session: boto3.Session, arg_regions: Optional[List[str]]) -> List[str]:
    if arg_regions and arg_regions != ["all"]:
        return arg_regions
    ec2 = session.client("ec2", region_name=session.region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    regions = ec2.describe_regions(AllRegions=True)["Regions"]
    names = [r["RegionName"] for r in regions if r.get("OptInStatus") in ("opt-in-not-required", "opted-in")]
    if arg_regions == ["all"]:
        return names
    # default: single region = session/default
    return [session.region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")]


# ---------- CW metric queries ----------
def cw_batch_get(cloudwatch, queries: List[Dict[str, Any]], start: datetime, end: datetime) -> Dict[str, float]:
    """
    מריץ GetMetricData במנות, ומחזיר dict של {Id: aggregated_value}.
    חשוב: לא לשלוח NextToken=None – שולחים רק אם יש ערך.
    """
    results: Dict[str, float] = {}
    next_token = None

    while True:
        params = {
            "MetricDataQueries": queries,
            "StartTime": start,
            "EndTime": end,
            "ScanBy": "TimestampAscending",
        }
        if next_token:
            params["NextToken"] = next_token

        resp = cloudwatch.get_metric_data(**params)

        for r in resp.get("MetricDataResults", []):
            id_ = r.get("Id")
            vals = r.get("Values", [])
            if not vals:
                continue
            # אגרגציה לפי סיומות Id
            if id_.endswith("_avg") or id_.endswith("_memavg"):
                results[id_] = sum(vals) / len(vals)
            elif id_.endswith("_p95") or id_.endswith("_memp95"):
                results[id_] = max(vals)
            elif id_.endswith("_max") or id_.endswith("_memmax"):
                results[id_] = max(vals)
            elif id_.endswith("_sum"):
                results[id_] = sum(vals)
            elif id_.endswith("_min"):
                results[id_] = min(vals)
            elif id_.endswith("_statusmax"):
                results[id_] = max(vals)
            else:
                results[id_] = sum(vals) / len(vals)

        next_token = resp.get("NextToken")
        if not next_token:
            break

    return results


def build_queries_for_instance(instance_id: str, period: int) -> List[Dict[str, Any]]:
    dims = [{"Name": "InstanceId", "Value": instance_id}]
    q = [
        # CPU
        {"Id": "cpu_avg", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization",
                                                    "Dimensions": dims}, "Period": period, "Stat": "Average"}},
        {"Id": "cpu_p95", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization",
                                                    "Dimensions": dims}, "Period": period, "Stat": "p95"}},
        {"Id": "cpu_max", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUUtilization",
                                                    "Dimensions": dims}, "Period": period, "Stat": "Maximum"}},
        # Network (bytes) – נסכם את כל התקופה
        {"Id": "netin_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkIn",
                                                      "Dimensions": dims}, "Period": period, "Stat": "Sum"}},
        {"Id": "netout_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkOut",
                                                       "Dimensions": dims}, "Period": period, "Stat": "Sum"}},
        # **Network packets (count) – חדש**
        {"Id": "pktin_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkPacketsIn",
                                                      "Dimensions": dims}, "Period": period, "Stat": "Sum"}},
        {"Id": "pktout_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "NetworkPacketsOut",
                                                       "Dimensions": dims}, "Period": period, "Stat": "Sum"}},
        # Status checks
        {"Id": "status_statusmax", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "StatusCheckFailed",
                                                             "Dimensions": dims}, "Period": period, "Stat": "Maximum"}},
    ]
    return q


def maybe_add_t_credits(queries: List[Dict[str, Any]], instance_type: str, instance_id: str, period: int):
    if is_t_family(instance_type):
        dims = [{"Name": "InstanceId", "Value": instance_id}]
        # Balance (min)
        queries.append(
            {"Id": "cpucred_min", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUCreditBalance",
                                                            "Dimensions": dims}, "Period": period, "Stat": "Minimum"}}
        )
        # **Usage (sum) – חדש**
        queries.append(
            {"Id": "cpucredusage_sum", "MetricStat": {"Metric": {"Namespace": "AWS/EC2", "MetricName": "CPUCreditUsage",
                                                                 "Dimensions": dims}, "Period": period, "Stat": "Sum"}}
        )


def add_memory_queries_if_available(queries: List[Dict[str, Any]], instance_id: str, period: int, cwagent_ns: str = "CWAgent"):
    """
    מנסה להביא mem_used_percent אם קיים CWAgent; אם אין – פשוט יישאר ריק.
    """
    dims = [{"Name": "InstanceId", "Value": instance_id}]
    queries.extend([
        {"Id": "mem_memavg", "MetricStat": {"Metric": {"Namespace": cwagent_ns, "MetricName": "mem_used_percent",
                                                       "Dimensions": dims}, "Period": period, "Stat": "Average"}},
        {"Id": "mem_memp95", "MetricStat": {"Metric": {"Namespace": cwagent_ns, "MetricName": "mem_used_percent",
                                                       "Dimensions": dims}, "Period": period, "Stat": "p95"}},
        {"Id": "mem_memmax", "MetricStat": {"Metric": {"Namespace": cwagent_ns, "MetricName": "mem_used_percent",
                                                       "Dimensions": dims}, "Period": period, "Stat": "Maximum"}},
    ])


# ---------- Compute Optimizer ----------
def get_co_map(session: boto3.Session, region: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    co = session.client("compute-optimizer", region_name=region)
    next_token = None
    while True:
        params = {}
        if next_token:
            params["nextToken"] = next_token
        resp = co.get_ec2_instance_recommendations(**params)
        for rec in resp.get("instanceRecommendations", []):
            inst_id = rec.get("instanceId", "")
            finding = rec.get("finding", "")
            risk = rec.get("performanceRisk")
            recs = rec.get("recommendationOptions") or []
            if recs:
                best = recs[0]
                rec_type = best.get("instanceType", "")
                est_sav = (best.get("savingsOpportunity", {}) or {}).get("estimatedMonthlySavings", {})
                savings = est_sav.get("value")
                currency = est_sav.get("currency", "USD")
            else:
                rec_type = ""
                savings = None
                currency = "USD"
            out[inst_id] = {"CO_Finding": finding, "CO_RecType": rec_type, "CO_PerfRisk": risk,
                            "CO_EstMonthlySavings": savings, "CO_Currency": currency}
        next_token = resp.get("nextToken")
        if not next_token:
            break
    return out


# ---------- main ----------
def main():
    ap = argparse.ArgumentParser(description="EC2 efficiency metrics (usage + inventory) – FinOps")
    ap.add_argument("--profile", default=os.getenv("AWS_PROFILE"), help="AWS profile (or use AWS_PROFILE)")
    ap.add_argument("--regions", default=None,
                    help='Comma-separated list (e.g. "eu-west-1,us-east-1") or "all". Default: current region.')
    ap.add_argument("--days", type=int, default=30, help="Lookback days for metrics (default 30)")
    ap.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (default 300 = 5m)")
    ap.add_argument("--with-co", action="store_true", help="Include Compute Optimizer recommendations (if enabled)")
    ap.add_argument("--format", choices=["csv", "json"], default="csv")
    ap.add_argument("--outdir", default="out")
    args = ap.parse_args()

    # session
    try:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    except Exception as e:
        print(f"ERROR: cannot create session for profile '{args.profile}': {e}", file=sys.stderr)
        sys.exit(2)

    # whoami
    sts = session.client("sts")
    try:
        ident = sts.get_caller_identity()
    except botocore.exceptions.NoCredentialsError:
        print("ERROR: No credentials. Run: aws sso login --profile <name>", file=sys.stderr)
        sys.exit(2)
    account = ident["Account"]
    caller = ident["Arn"]

    # regions
    regions = get_regions(session, [r.strip() for r in args.regions.split(",")] if args.regions else None)

    # optional CO cache per region
    co_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}

    headers = [
        "Account", "Region", "InstanceId", "Name", "InstanceType", "State", "AZ", "LaunchTime",
        "CPU_Avg%", "CPU_p95%", "CPU_Max%",
        "Mem_Avg%","Mem_p95%","Mem_Max%",
        "NetIn_GB","NetOut_GB",
        "PacketsIn_Count","PacketsOut_Count",                # חדש
        "CPUCreditBalance_Min","CPUCreditUsage_Sum",         # חדש (T*)
        "StatusCheckFailed_Max",
        "Lifecycle","Tenancy","VpcId","SubnetId","PrivateIp","PublicIp",
        "CO_Finding","CO_RecType","CO_PerfRisk","CO_EstMonthlySavings","CO_Currency",
        "Tags"
    ]
    rows: List[List[str]] = []

    # time window
    end = datetime.now(tz=tz.tzlocal()).astimezone(timezone.utc)
    start = end - timedelta(days=args.days)
    period = args.period

    for region in regions:
        # list instances
        try:
            ec2 = session.client("ec2", region_name=region)
            paginator = ec2.get_paginator("describe_instances")
            instances: List[Dict[str, Any]] = []
            for page in paginator.paginate():
                for res in page.get("Reservations", []) or []:
                    for inst in res.get("Instances", []) or []:
                        instances.append(inst)
        except botocore.exceptions.ClientError as e:
            print(f"# ERR | Account {account} | Region {region} | {e.response.get('Error',{}).get('Code','ClientError')}: {e}", file=sys.stderr)
            continue

        # optional CO
        if args.with_co and region not in co_cache:
            try:
                co_cache[region] = get_co_map(session, region)
            except botocore.exceptions.ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                print(f"# WARN | CO in {region} unavailable ({code}) – skipping.", file=sys.stderr)
                co_cache[region] = {}

        # CloudWatch
        cw = session.client("cloudwatch", region_name=region)
        for inst in instances:
            iid = inst.get("InstanceId")
            itype = inst.get("InstanceType", "")
            # build queries
            q = build_queries_for_instance(iid, period)
            maybe_add_t_credits(q, itype, iid, period)
            add_memory_queries_if_available(q, iid, period)

            # fetch
            res = cw_batch_get(cw, q, start, end)

            # extract
            cpu_avg = res.get("cpu_avg"); cpu_p95 = res.get("cpu_p95"); cpu_max = res.get("cpu_max")
            mem_avg = res.get("mem_memavg"); mem_p95 = res.get("mem_memp95"); mem_max = res.get("mem_memmax")
            netin_gb = gb(res.get("netin_sum")); netout_gb = gb(res.get("netout_sum"))
            pktin = res.get("pktin_sum"); pktout = res.get("pktout_sum")
            cred_min = res.get("cpucred_min")
            cred_usage = res.get("cpucredusage_sum")
            status_max = res.get("status_statusmax")

            state = (inst.get("State") or {}).get("Name", "")
            az = (inst.get("Placement") or {}).get("AvailabilityZone", "")
            launch = inst.get("LaunchTime")
            lifecycle = inst.get("InstanceLifecycle", "OnDemand")
            tenancy = (inst.get("Placement") or {}).get("Tenancy", "")
            vpc = inst.get("VpcId", ""); subnet = inst.get("SubnetId","")
            priv_ip = inst.get("PrivateIpAddress",""); pub_ip = inst.get("PublicIpAddress","")
            name = ""
            tags_list = inst.get("Tags", []) or []
            for t in tags_list:
                if t.get("Key") == "Name":
                    name = t.get("Value", "")
                    break
            tag_str = ";".join([f"{t.get('Key')}={t.get('Value')}" for t in tags_list])

            co = (co_cache.get(region) or {}).get(iid, {})
            rows.append([
                account, region, iid, name, itype, state, az, iso_utc(launch) if launch else "",
                pct(cpu_avg), pct(cpu_p95), pct(cpu_max),
                pct(mem_avg), pct(mem_p95), pct(mem_max),
                num(netin_gb), num(netout_gb),
                num(pktin), num(pktout),
                num(cred_min), num(cred_usage),
                num(status_max),
                lifecycle, tenancy, vpc, subnet, priv_ip, pub_ip,
                co.get("CO_Finding",""), co.get("CO_RecType",""),
                num(co.get("CO_PerfRisk")), num(co.get("CO_EstMonthlySavings")), co.get("CO_Currency",""),
                tag_str
            ])

        print(f"# OK  | Account {account} | Region {region} | Instances {len(instances)}")

    # write file
    os.makedirs(args.outdir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    region_tag = "multi" if len(regions) > 1 else regions[0]
    profile_tag = args.profile or (session.profile_name or "default")
    outfile = os.path.join(args.outdir, f"ec2_efficiency_{profile_tag}_{region_tag}_{ts}.{args.format}")

    if args.format == "csv":
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            w.writerows(rows)
    else:
        with open(outfile, "w", encoding="utf-8") as f:
            data = [dict(zip(headers, r)) for r in rows]
            json.dump(data, f, ensure_ascii=False, indent=2)

    print("\n# SUMMARY")
    print(f"# Caller:  {caller}")
    print(f"# Account: {account}")
    print(f"# Regions: {', '.join(regions)}")
    print(f"# Days:    {args.days} | Period: {args.period}s")
    print(f"# Rows:    {len(rows)}")
    print(f"# File:    {outfile}")


if __name__ == "__main__":
    main()
