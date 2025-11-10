#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DynamoDB FinOps review.

Usage:
    python3 scripts/reviews/dynamodb_finops/run.py --region us-east-1 --profile prod

Collects table configuration + CloudWatch consumption metrics to highlight
idle tables and tables that might benefit from provisioned capacity.
"""

from __future__ import annotations

import argparse
import math
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

from scripts.common.cloudwatch import (
    get_metric_statistics_multi,
    window,
)
from scripts.common.csvio import write_csv

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
DDB_NAMESPACE = "AWS/DynamoDB"

CSV_FIELDS: Sequence[str] = (
    "TableName",
    "BillingMode",
    "Region",
    "TableSizeMB",
    "ItemCount",
    "TableClass",
    "PITR",
    "GSI_Count",
    "avg_1d_read",
    "peak_1d_read",
    "p95_1d_read",
    "avg_1d_write",
    "peak_1d_write",
    "p95_1d_write",
    "avg_1d_throttle",
    "peak_1d_throttle",
    "p95_1d_throttle",
    "avgmax_1d_read",
    "avgmax_1d_write",
    "avgmax_1d_throttle",
    "avg_7d_read",
    "peak_7d_read",
    "p95_7d_read",
    "avg_7d_write",
    "peak_7d_write",
    "p95_7d_write",
    "avg_7d_throttle",
    "peak_7d_throttle",
    "p95_7d_throttle",
    "avgmax_7d_read",
    "avgmax_7d_write",
    "avgmax_7d_throttle",
    "avg_30d_read",
    "peak_30d_read",
    "p95_30d_read",
    "avg_30d_write",
    "peak_30d_write",
    "p95_30d_write",
    "avg_30d_throttle",
    "peak_30d_throttle",
    "p95_30d_throttle",
    "avgmax_30d_read",
    "avgmax_30d_write",
    "avgmax_30d_throttle",
    "stability_7d_read",
    "stability_7d_write",
    "stability_7d_p95_ratio",
    "spike_ratio_7d",
    "throttle_indicator",
    "confidence_score",
    "recommendation",
    "Tags",
)

TIME_WINDOWS = {
    "1d": (1, 300),
    "7d": (7, 900),
    "30d": (30, 3600),
}


@dataclass
class MetricAggregate:
    avg: float = 0.0
    peak: float = 0.0
    p95: float = 0.0
    spike_count: int = 0
    samples_count: int = 0
    avg_max: float = 0.0


@dataclass
class MetricBundle:
    read: Dict[str, MetricAggregate]
    write: Dict[str, MetricAggregate]
    throttle: Dict[str, MetricAggregate]


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the DynamoDB FinOps review")
    parser.add_argument("--region", required=True, help="AWS region (single or CSV list)")
    parser.add_argument("--profile", required=True, help="AWS CLI profile name")
    parser.add_argument(
        "--output",
        default="outputs/dynamodb_finops_summary.csv",
        help="Path to the summary CSV (default: outputs/dynamodb_finops_summary.csv)",
    )
    return parser.parse_args(argv)


def percentile(values: Sequence[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    k = (len(ordered) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return float(ordered[f])
    d0 = ordered[f] * (c - k)
    d1 = ordered[c] * (k - f)
    return float(d0 + d1)


def summarize_datapoints(datapoints: Iterable[Dict]) -> MetricAggregate:
    avg_vals: List[float] = []
    max_vals: List[float] = []
    p95_vals: List[float] = []
    spike_count = 0
    samples_count = 0

    for dp in datapoints:
        avg_val = None
        max_val = None
        if "Average" in dp:
            try:
                avg_val = float(dp["Average"])
            except (TypeError, ValueError):
                avg_val = None
            if avg_val is not None:
                avg_vals.append(avg_val)
        if "Maximum" in dp:
            try:
                max_val = float(dp["Maximum"])
            except (TypeError, ValueError):
                max_val = None
            if max_val is not None:
                max_vals.append(max_val)
        if avg_val is not None or max_val is not None:
            samples_count += 1
        if avg_val and max_val and avg_val > 0 and max_val >= avg_val * 2:
            spike_count += 1
        ext = dp.get("ExtendedStatistics")
        if isinstance(ext, dict) and "p95" in ext:
            try:
                p95_vals.append(float(ext["p95"]))
            except (TypeError, ValueError):
                continue

    avg = sum(avg_vals) / len(avg_vals) if avg_vals else 0.0
    peak = max(max_vals) if max_vals else 0.0
    p95 = percentile(p95_vals, 95.0) if p95_vals else 0.0
    avg_max = sum(max_vals) / len(max_vals) if max_vals else 0.0
    return MetricAggregate(
        avg=avg,
        peak=peak,
        p95=p95,
        spike_count=spike_count,
        samples_count=samples_count,
        avg_max=avg_max,
    )


def fetch_metric_bundle(cw, table_name: str) -> MetricBundle:
    results: Dict[str, Dict[str, MetricAggregate]] = {
        "read": {},
        "write": {},
        "throttle": {},
    }
    dimensions = [{"Name": "TableName", "Value": table_name}]

    metric_map = {
        "read": "ConsumedReadCapacityUnits",
        "write": "ConsumedWriteCapacityUnits",
        "throttle": "ThrottledRequests",
    }

    for label, metric_name in metric_map.items():
        for window_name, (days, period) in TIME_WINDOWS.items():
            start, end = window(days)
            try:
                datapoints = get_metric_statistics_multi(
                    cw,
                    namespace=DDB_NAMESPACE,
                    metric_name=metric_name,
                    dimensions=dimensions,
                    start=start,
                    end=end,
                    period=period,
                    statistics=["Average", "Maximum"],
                    extended_statistics=["p95"],
                )
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "Unknown")
                print(f"    [metrics] {table_name} {window_name} {metric_name} -> {code}", file=sys.stderr)
                datapoints = []
            agg = summarize_datapoints(datapoints)
            results[label][window_name] = agg

    return MetricBundle(
        read=results["read"],
        write=results["write"],
        throttle=results["throttle"],
    )


def stability_ratio(peak: float, avg: float) -> float:
    if not avg:
        return 0.0
    return peak / avg if avg else 0.0


def extract_table_metadata(table: Dict) -> Dict[str, Optional[str]]:
    billing_summary = table.get("BillingModeSummary") or {}
    billing_mode = billing_summary.get("BillingMode") or table.get("TableStatus")

    pitr_desc = table.get("PointInTimeRecoveryDescription") or {}
    pitr_status = pitr_desc.get("Status")
    if not pitr_status:
        pitr_status = "OFF"

    gsi_list = table.get("GlobalSecondaryIndexes") or []
    table_class_summary = table.get("TableClassSummary") or {}

    return {
        "TableName": table.get("TableName"),
        "BillingMode": billing_mode,
        "TableSizeMB": round((table.get("TableSizeBytes", 0) or 0) / (1024 * 1024), 2),
        "ItemCount": table.get("ItemCount", 0),
        "TableClass": table_class_summary.get("TableClass"),
        "PITR": pitr_status,
        "GSI_Count": len(gsi_list),
    }


def render_recommendation(
    agg: MetricBundle,
    table_meta: Dict[str, Optional[str]],
) -> str:
    read_7d = agg.read.get("7d")
    write_7d = agg.write.get("7d")
    read_30d = agg.read.get("30d")
    write_30d = agg.write.get("30d")

    avg_7d_total = (read_7d.avg if read_7d else 0.0) + (write_7d.avg if write_7d else 0.0)
    avg_30d_total = (read_30d.avg if read_30d else 0.0) + (write_30d.avg if write_30d else 0.0)

    stability_read = stability_ratio(read_7d.peak, read_7d.avg) if read_7d else 0.0
    stability_write = stability_ratio(write_7d.peak, write_7d.avg) if write_7d else 0.0
    stability_indicator = max(stability_read, stability_write)

    if avg_7d_total == 0 and avg_30d_total == 0:
        base = "Idle table – review for deletion or archive"
    elif stability_indicator and stability_indicator <= 3:
        base = "Stable usage – consider switching to Provisioned + Auto Scaling"
    elif stability_indicator == 0:
        base = "Stable usage – consider switching to Provisioned + Auto Scaling"
    else:
        base = "Variable usage – keep On-Demand"

    table_size_mb = table_meta.get("TableSizeMB") or 0
    pitr_status = (table_meta.get("PITR") or "").upper()
    if table_size_mb and table_size_mb > 100 and pitr_status in {"ENABLED", "ENABLING"}:
        base = base + " | Large table + PITR – review backup costs"
    return base


def collect_region(session, region: str) -> Tuple[List[Dict], Dict[str, int]]:
    dynamodb = session.client("dynamodb", region_name=region, config=CFG)
    cw = session.client("cloudwatch", region_name=region, config=CFG)

    paginator = dynamodb.get_paginator("list_tables")
    table_names: List[str] = []
    for page in paginator.paginate():
        table_names.extend(page.get("TableNames", []))

    rows: List[Dict] = []
    counts = {
        "total": 0,
        "stable": 0,
        "idle": 0,
    }

    for table_name in sorted(table_names):
        try:
            detail = dynamodb.describe_table(TableName=table_name)["Table"]
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            print(f"[{region}] describe_table {table_name} -> {code}", file=sys.stderr)
            continue

        metadata = extract_table_metadata(detail)
        metadata["Region"] = region

        # Optional: fetch tags (best-effort)
        table_arn = detail.get("TableArn")
        if table_arn:
            try:
                tag_resp = dynamodb.list_tags_of_resource(ResourceArn=table_arn)
                tags = {t["Key"]: t.get("Value", "") for t in tag_resp.get("Tags", [])}
                if tags:
                    metadata["Tags"] = ";".join(f"{k}={v}" for k, v in sorted(tags.items()))
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "Unknown")
                print(f"[{region}] list_tags_of_resource {table_name} -> {code}", file=sys.stderr)

        bundle = fetch_metric_bundle(cw, table_name)

        row = dict(metadata)

        for window_name in TIME_WINDOWS.keys():
            read_agg = bundle.read.get(window_name) or MetricAggregate()
            write_agg = bundle.write.get(window_name) or MetricAggregate()
            throttle_agg = bundle.throttle.get(window_name) or MetricAggregate()

            row[f"avg_{window_name}_read"] = round(read_agg.avg, 4)
            row[f"peak_{window_name}_read"] = round(read_agg.peak, 4)
            row[f"p95_{window_name}_read"] = round(read_agg.p95, 4)
            row[f"avgmax_{window_name}_read"] = round(read_agg.avg_max, 4)

            row[f"avg_{window_name}_write"] = round(write_agg.avg, 4)
            row[f"peak_{window_name}_write"] = round(write_agg.peak, 4)
            row[f"p95_{window_name}_write"] = round(write_agg.p95, 4)
            row[f"avgmax_{window_name}_write"] = round(write_agg.avg_max, 4)

            row[f"avg_{window_name}_throttle"] = round(throttle_agg.avg, 4)
            row[f"peak_{window_name}_throttle"] = round(throttle_agg.peak, 4)
            row[f"p95_{window_name}_throttle"] = round(throttle_agg.p95, 4)
            row[f"avgmax_{window_name}_throttle"] = round(throttle_agg.avg_max, 4)

        read_7d = bundle.read.get("7d")
        write_7d = bundle.write.get("7d")

        p95_sum = (read_7d.p95 if read_7d else 0.0) + (write_7d.p95 if write_7d else 0.0)
        avg_sum = (read_7d.avg if read_7d else 0.0) + (write_7d.avg if write_7d else 0.0)
        stability_7d_p95_ratio = (p95_sum / avg_sum) if avg_sum else 0.0

        samples_7d = (read_7d.samples_count if read_7d else 0) + (
            write_7d.samples_count if write_7d else 0
        )
        spikes_7d = (read_7d.spike_count if read_7d else 0) + (
            write_7d.spike_count if write_7d else 0
        )
        spike_ratio_7d = (spikes_7d / samples_7d) if samples_7d else 0.0

        throttle_7d = bundle.throttle.get("7d")
        throttle_30d = bundle.throttle.get("30d")
        avg_7d_throttle = throttle_7d.avg if throttle_7d else 0.0
        avg_30d_throttle = throttle_30d.avg if throttle_30d else 0.0
        throttle_indicator = (avg_7d_throttle + avg_30d_throttle) / 2.0

        stability_read = stability_ratio(read_7d.peak, read_7d.avg) if read_7d else 0.0
        stability_write = stability_ratio(write_7d.peak, write_7d.avg) if write_7d else 0.0

        row["stability_7d_read"] = round(stability_read, 4) if stability_read else 0.0
        row["stability_7d_write"] = round(stability_write, 4) if stability_write else 0.0
        row["stability_7d_p95_ratio"] = round(stability_7d_p95_ratio, 4)
        row["spike_ratio_7d"] = round(spike_ratio_7d, 4)
        row["throttle_indicator"] = round(throttle_indicator, 4)

        recommendation = render_recommendation(bundle, row)
        row["recommendation"] = recommendation
        row["confidence_score"] = round(row.get("confidence_score", 0.0), 4)

        if "Tags" not in row:
            row["Tags"] = ""

        avg_7d_total = (read_7d.avg if read_7d else 0.0) + (write_7d.avg if write_7d else 0.0)
        write_30d = bundle.write.get("30d")
        read_30d = bundle.read.get("30d")
        avg_30d_total = (read_30d.avg if read_30d else 0.0) + (write_30d.avg if write_30d else 0.0)
        stability_indicator = max(row["stability_7d_read"], row["stability_7d_write"])

        counts["total"] += 1
        if stability_indicator <= 3 and stability_indicator >= 0:
            counts["stable"] += 1
        if avg_7d_total == 0 and avg_30d_total == 0:
            counts["idle"] += 1

        rows.append(row)

    return rows, counts


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

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
    total_counts = {"total": 0, "stable": 0, "idle": 0}

    for region in regions:
        region_rows, counters = collect_region(session, region)
        all_rows.extend(region_rows)
        for key in total_counts:
            total_counts[key] += counters.get(key, 0)

    write_csv(args.output, all_rows, CSV_FIELDS)

    print("=== DynamoDB FinOps Review ===")
    print(f"Profile: {args.profile}")
    print(f"Regions: {', '.join(regions)}")
    print(f"Tables scanned: {total_counts['total']}")
    print(f"Stable tables (stability<=3): {total_counts['stable']}")
    print(f"Idle tables: {total_counts['idle']}")
    print(f"CSV written to: {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
