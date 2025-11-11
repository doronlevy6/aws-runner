#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DynamoDB FinOps review — Cleaned & Improved
- Uses Sum for Consumed* metrics (per AWS guidance)
- Uses Read/WriteThrottleEvents (table + GSI)
- Computes avg/sec from totals, stability, burstiness, headroom
- Fixes BillingMode default and PITR status
- Emits a concise CSV with actionable recommendations

Usage:
    python3 scripts/reviews/dynamodb_finops/run.py --region us-east-1 --profile prod
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

# These helpers are assumed to exist in your project (unchanged)
from scripts.common.cloudwatch import get_metric_statistics_multi, window
from scripts.common.csvio import write_csv

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
DDB_NAMESPACE = "AWS/DynamoDB"

# Final CSV columns (lean & actionable)
CSV_FIELDS: Sequence[str] = (
    "TableName",
    "BillingMode",
    "Region",
    "TableSizeMB",
    "ItemCount",
    "TableClass",
    "PITR",
    "GSI_Count",

    # Core 30d totals & avg/sec
    "total_30d_read",
    "total_30d_write",
    "avg_30d_read_per_sec",
    "avg_30d_write_per_sec",

    # Peaks & statistics
    "peak_30d_read",
    "peak_30d_write",
    "p95_7d_read",
    "p95_7d_write",
    "stability_7d_read",      # peak/avg (7d)
    "stability_7d_write",

    # Coverage & throttles
    "samples_30d_read",
    "samples_30d_write",
    "expected_samples_30d",
    "coverage_ok",
    "throttle_read_7d_sum",
    "throttle_write_7d_sum",
    "throttle_read_30d_sum",
    "throttle_write_30d_sum",

    # Derived indicators
    "avg_ops_sec",            # read+write
    "read_spike_ratio",       # peak/avg (30d)
    "write_spike_ratio",
    "headroom_r",             # p95/peak (7d vs 30d peak)
    "headroom_w",

    # GSI hot-spot signals (optional but very useful)
    "gsi_top_name_by_read_peak",
    "gsi_top_read_peak_30d",
    "gsi_top_name_by_throttle",
    "gsi_top_throttle_30d_sum",

    # Final decision string
    "recommendation",
)

# Time windows: (days, period seconds)
TIME_WINDOWS = {
    "7d": (7,   900),
    "30d": (30, 3600),
}

SEC_30D = 30 * 24 * 3600


@dataclass
class MetricAggregate:
    # we now store arrays of sums/max for later aggregation
    sums: List[float]
    maxs: List[float]
    p95s: List[float]
    samples_count: int

    @property
    def total_sum(self) -> float:
        return float(sum(self.sums)) if self.sums else 0.0

    @property
    def avg_of_max(self) -> float:
        return float(sum(self.maxs) / len(self.maxs)) if self.maxs else 0.0

    @property
    def peak(self) -> float:
        return float(max(self.maxs)) if self.maxs else 0.0

    @property
    def p95(self) -> float:
        if not self.p95s:
            return 0.0
        ordered = sorted(self.p95s)
        k = (len(ordered) - 1) * 0.95
        f = math.floor(k)
        c = min(f + 1, len(ordered) - 1)
        if f == c:
            return float(ordered[f])
        d0 = ordered[f] * (c - k)
        d1 = ordered[c] * (k - f)
        return float(d0 + d1)


@dataclass
class MetricBundle:
    # table-level
    read_7d: MetricAggregate
    write_7d: MetricAggregate
    read_30d: MetricAggregate
    write_30d: MetricAggregate
    thr_read_7d_sum: float
    thr_write_7d_sum: float
    thr_read_30d_sum: float
    thr_write_30d_sum: float

    # GSI-level (top signals)
    gsi_top_name_by_read_peak: str
    gsi_top_read_peak_30d: float
    gsi_top_name_by_throttle: str
    gsi_top_throttle_30d_sum: float


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run the DynamoDB FinOps review")
    p.add_argument("--region", required=True, help="AWS region (single or CSV list)")
    p.add_argument("--profile", required=True, help="AWS CLI profile name")
    p.add_argument(
        "--output",
        default=None,
        help="Path to CSV (default: outputs/dynamodb_finops_<ts>/dynamodb_finops_summary.csv)",
    )
    return p.parse_args(argv)


def gather_agg(
    cw,
    metric_name: str,
    dimensions: List[Dict[str, str]],
    days: int,
    period: int,
    want_p95: bool = True,
) -> MetricAggregate:
    """Fetch datapoints and aggregate arrays of Sum/Maximum/p95."""
    stats = ["Sum", "Maximum"]
    ext = ["p95"] if want_p95 else []
    dps = get_metric_statistics_multi(
        cw,
        namespace=DDB_NAMESPACE,
        metric_name=metric_name,
        dimensions=dimensions,
        start=window(days)[0],
        end=window(days)[1],
        period=period,
        statistics=stats,
        extended_statistics=ext,
    )
    sums, maxs, p95s = [], [], []
    samples = 0
    for dp in dps:
        if "Sum" in dp:
            try:
                sums.append(float(dp["Sum"]))
            except Exception:
                pass
        if "Maximum" in dp:
            try:
                maxs.append(float(dp["Maximum"]))
            except Exception:
                pass
        if isinstance(dp.get("ExtendedStatistics"), dict) and "p95" in dp["ExtendedStatistics"]:
            try:
                p95s.append(float(dp["ExtendedStatistics"]["p95"]))
            except Exception:
                pass
        # count a sample if either stat exists
        if ("Sum" in dp) or ("Maximum" in dp):
            samples += 1
    return MetricAggregate(sums=sums, maxs=maxs, p95s=p95s, samples_count=samples)


def fetch_gsi_signals(
    cw,
    table_name: str,
    gsi_names: List[str],
) -> Tuple[str, float, str, float]:
    """Return top GSI by read peak and top GSI by throttle sum (30d)."""
    best_read_name, best_read_peak = "", 0.0
    best_thr_name, best_thr_sum = "", 0.0
    for gsi in gsi_names:
        dims = [{"Name": "TableName", "Value": table_name},
                {"Name": "GlobalSecondaryIndexName", "Value": gsi}]
        # Read peak (30d)
        read_30d = gather_agg(
            cw, "ConsumedReadCapacityUnits", dims, days=30, period=3600, want_p95=False
        )
        # Throttle sum (30d)
        thr_r_30d = gather_agg(
            cw, "ReadThrottleEvents", dims, days=30, period=3600, want_p95=False
        ).total_sum
        thr_w_30d = gather_agg(
            cw, "WriteThrottleEvents", dims, days=30, period=3600, want_p95=False
        ).total_sum

        if read_30d.peak > best_read_peak:
            best_read_peak = read_30d.peak
            best_read_name = gsi
        thr_sum = thr_r_30d + thr_w_30d
        if thr_sum > best_thr_sum:
            best_thr_sum = thr_sum
            best_thr_name = gsi

    return best_read_name, best_read_peak, best_thr_name, best_thr_sum


def safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d else default


def stability_ratio(peak: float, avg: float) -> float:
    """peak/avg with guardrails."""
    if avg <= 0:
        return 0.0
    return peak / avg


def collect_table(
    session,
    cw,
    region: str,
    table_detail: Dict,
) -> Dict:
    # --- Metadata ---
    table_name = table_detail.get("TableName")
    billing_summary = table_detail.get("BillingModeSummary") or {}
    # If missing -> it's PROVISIONED by default per AWS
    billing_mode = billing_summary.get("BillingMode") or "PROVISIONED"

    tclass_summary = table_detail.get("TableClassSummary") or {}
    pitr_desc = table_detail.get("PointInTimeRecoveryDescription") or {}
    pitr_status = (pitr_desc.get("PointInTimeRecoveryStatus") or "OFF").upper()

    gsi_list = table_detail.get("GlobalSecondaryIndexes") or []
    gsi_names = [g.get("IndexName") for g in gsi_list if g.get("IndexName")]

    row = {
        "TableName": table_name,
        "BillingMode": billing_mode,
        "Region": region,
        "TableSizeMB": round((table_detail.get("TableSizeBytes", 0) or 0) / (1024 * 1024), 2),
        "ItemCount": table_detail.get("ItemCount", 0),
        "TableClass": tclass_summary.get("TableClass"),
        "PITR": pitr_status,
        "GSI_Count": len(gsi_names),
    }

    # --- Table-level metrics (7d/30d) ---
    dims_table = [{"Name": "TableName", "Value": table_name}]

    read_7d = gather_agg(cw, "ConsumedReadCapacityUnits", dims_table, 7, 900, True)
    write_7d = gather_agg(cw, "ConsumedWriteCapacityUnits", dims_table, 7, 900, True)
    read_30d = gather_agg(cw, "ConsumedReadCapacityUnits", dims_table, 30, 3600, True)
    write_30d = gather_agg(cw, "ConsumedWriteCapacityUnits", dims_table, 30, 3600, True)

    thr_r_7d = gather_agg(cw, "ReadThrottleEvents", dims_table, 7, 900, False).total_sum
    thr_w_7d = gather_agg(cw, "WriteThrottleEvents", dims_table, 7, 900, False).total_sum
    thr_r_30d = gather_agg(cw, "ReadThrottleEvents", dims_table, 30, 3600, False).total_sum
    thr_w_30d = gather_agg(cw, "WriteThrottleEvents", dims_table, 30, 3600, False).total_sum

    # --- Derived: totals & avg/sec (30d) ---
    total_30d_read = read_30d.total_sum
    total_30d_write = write_30d.total_sum
    avg_30d_read_per_sec = safe_div(total_30d_read, SEC_30D)
    avg_30d_write_per_sec = safe_div(total_30d_write, SEC_30D)

    # peak 30d from Maximum
    peak_30d_read = read_30d.peak
    peak_30d_write = write_30d.peak

    # p95 (7d)
    p95_7d_read = read_7d.p95
    p95_7d_write = write_7d.p95

    # stability 7d (peak/avg where avg from Sum/period)
    # compute avg/sec for 7d to make stability meaningful
    # Derive avg/sec from sums: (sum of all Sum datapoints) / (seconds in 7d)
    SEC_7D = 7 * 24 * 3600
    avg_7d_read_per_sec = safe_div(read_7d.total_sum, SEC_7D)
    avg_7d_write_per_sec = safe_div(write_7d.total_sum, SEC_7D)
    stability_7d_read = stability_ratio(read_7d.peak, avg_7d_read_per_sec)
    stability_7d_write = stability_ratio(write_7d.peak, avg_7d_write_per_sec)

    # Coverage stats from 30d
    # expected samples = 30d / period (period=3600)
    expected_samples_30d = math.floor(SEC_30D / 3600)
    samples_30d_read = read_30d.samples_count
    samples_30d_write = write_30d.samples_count
    coverage_ok = (samples_30d_read >= expected_samples_30d * 0.95) and \
                  (samples_30d_write >= expected_samples_30d * 0.95)

    # spike ratios (30d): peak/avg_per_sec
    avg_30d_total_per_sec_read = avg_30d_read_per_sec
    avg_30d_total_per_sec_write = avg_30d_write_per_sec
    read_spike_ratio = stability_ratio(peak_30d_read, avg_30d_total_per_sec_read)
    write_spike_ratio = stability_ratio(peak_30d_write, avg_30d_total_per_sec_write)

    # p95 headroom vs 30d peak (if peak==0 -> 0)
    headroom_r = safe_div(p95_7d_read, peak_30d_read)
    headroom_w = safe_div(p95_7d_write, peak_30d_write)

    # overall avg ops/sec
    avg_ops_sec = avg_30d_read_per_sec + avg_30d_write_per_sec

    # --- GSI signals (top) ---
    gsi_top_name_by_read_peak, gsi_top_read_peak_30d, gsi_top_name_by_throttle, gsi_top_throttle_30d_sum = ("", 0.0, "", 0.0)
    if gsi_names:
        gsi_top_name_by_read_peak, gsi_top_read_peak_30d, gsi_top_name_by_throttle, gsi_top_throttle_30d_sum = \
            fetch_gsi_signals(cw, table_name, gsi_names)

    # --- Decision rules (keep it transparent) ---
    # base guards
    if not coverage_ok:
        recommendation = "NEED_MORE_DATA"
    elif (thr_r_7d + thr_w_7d + thr_r_30d + thr_w_30d) > 0:
        recommendation = "ON_DEMAND | throttles observed"
    else:
        # Stable & sustained => Provisioned + AS
        stable = (stability_7d_read <= 2.0) and (stability_7d_write <= 2.0)
        semi_stable = (stability_7d_read <= 3.0) and (stability_7d_write <= 3.0)
        low_burst = (read_spike_ratio <= 2.0) and (write_spike_ratio <= 2.0)
        med_burst = (read_spike_ratio <= 4.0) and (write_spike_ratio <= 4.0)
        sustained = (avg_ops_sec >= 10.0)  # default threshold; tune per account

        if stable and low_burst and sustained and (headroom_r <= 0.6) and (headroom_w <= 0.6):
            recommendation = "PROVISIONED_STRONG (AS target~70%)"
        elif sustained and (semi_stable or med_burst):
            recommendation = "PROVISIONED_CAUTIOUS (AS target~70% + alarms)"
        else:
            recommendation = "ON_DEMAND"

    # --- Build row ---
    row.update({
        "total_30d_read": round(total_30d_read, 2),
        "total_30d_write": round(total_30d_write, 2),
        "avg_30d_read_per_sec": round(avg_30d_read_per_sec, 6),
        "avg_30d_write_per_sec": round(avg_30d_write_per_sec, 6),
        "peak_30d_read": round(peak_30d_read, 6),
        "peak_30d_write": round(peak_30d_write, 6),
        "p95_7d_read": round(p95_7d_read, 6),
        "p95_7d_write": round(p95_7d_write, 6),
        "stability_7d_read": round(stability_7d_read, 4),
        "stability_7d_write": round(stability_7d_write, 4),

        "samples_30d_read": samples_30d_read,
        "samples_30d_write": samples_30d_write,
        "expected_samples_30d": expected_samples_30d,
        "coverage_ok": coverage_ok,

        "throttle_read_7d_sum": int(thr_r_7d),
        "throttle_write_7d_sum": int(thr_w_7d),
        "throttle_read_30d_sum": int(thr_r_30d),
        "throttle_write_30d_sum": int(thr_w_30d),

        "avg_ops_sec": round(avg_ops_sec, 6),
        "read_spike_ratio": round(read_spike_ratio, 3),
        "write_spike_ratio": round(write_spike_ratio, 3),
        "headroom_r": round(headroom_r, 3),
        "headroom_w": round(headroom_w, 3),

        "gsi_top_name_by_read_peak": gsi_top_name_by_read_peak,
        "gsi_top_read_peak_30d": round(gsi_top_read_peak_30d, 6),
        "gsi_top_name_by_throttle": gsi_top_name_by_throttle,
        "gsi_top_throttle_30d_sum": int(gsi_top_throttle_30d_sum),

        "recommendation": recommendation,
    })
    return row


def collect_region(session, region: str) -> List[Dict]:
    dynamodb = session.client("dynamodb", region_name=region, config=CFG)
    cw = session.client("cloudwatch", region_name=region, config=CFG)

    paginator = dynamodb.get_paginator("list_tables")
    table_names: List[str] = []
    for page in paginator.paginate():
        table_names.extend(page.get("TableNames", []))

    rows: List[Dict] = []
    for table_name in sorted(table_names):
        try:
            detail = dynamodb.describe_table(TableName=table_name)["Table"]
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            print(f"[{region}] describe_table {table_name} -> {code}", file=sys.stderr)
            continue

        try:
            row = collect_table(session, cw, region, detail)
            rows.append(row)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            print(f"[{region}] metrics {table_name} -> {code}", file=sys.stderr)
            continue
    return rows


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    default_outdir = os.path.join("outputs", f"dynamodb_finops_{ts}")
    output_path = args.output or os.path.join(default_outdir, "dynamodb_finops_summary.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    regions = [r.strip() for r in args.region.split(",") if r.strip()]
    if not regions:
        print("No regions provided", file=sys.stderr)
        return 2

    try:
        session = boto3.Session(profile_name=args.profile)
    except ProfileNotFound:
        print(f"Profile '{args.profile}' not found", file=sys.stderr)
        return 2

    all_rows: List[Dict] = []
    for region in regions:
        all_rows.extend(collect_region(session, region))

    write_csv(output_path, all_rows, CSV_FIELDS)

    print("=== DynamoDB FinOps Review (Clean) ===")
    print(f"Profile: {args.profile}")
    print(f"Regions: {', '.join(regions)}")
    print(f"Tables scanned: {len(all_rows)}")
    print(f"CSV written to: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
