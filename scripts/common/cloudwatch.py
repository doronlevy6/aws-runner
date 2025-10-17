#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
import math
from botocore.config import Config as BotoConfig

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

# ----- Time helpers -----
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def window(days: int) -> Tuple[datetime, datetime]:
    end = utc_now()
    start = end - timedelta(days=days)
    return start, end

# ----- Stats helpers -----
def _percentile(sorted_series: List[float], p: float) -> Optional[float]:
    if not sorted_series:
        return None
    k = (len(sorted_series) - 1) * (p / 100.0)
    f = math.floor(k)
    c = min(f + 1, len(sorted_series) - 1)
    if f == c:
        return sorted_series[f]
    return sorted_series[f] + (sorted_series[c] - sorted_series[f]) * (k - f)

def summarize(series: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not series:
        return (None, None, None)
    s = sorted(series)
    avg = sum(s) / len(s)
    p95 = _percentile(s, 95.0)
    mx = s[-1]
    return (avg, p95, mx)

# ----- CloudWatch fetch -----
def get_metric_series(
    cw_client,
    namespace: str,
    metric_name: str,
    dimensions: List[Dict[str, str]],
    start: datetime,
    end: datetime,
    period: int,
    stat: str = "Average",
) -> List[float]:
    resp = cw_client.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=[stat],
    )
    dps = sorted(resp.get("Datapoints", []), key=lambda d: d["Timestamp"])
    return [float(dp[stat]) for dp in dps if stat in dp]

# ----- RDS helpers -----
def rds_dim(db_instance_id: str) -> List[Dict[str, str]]:
    return [{"Name": "DBInstanceIdentifier", "Value": db_instance_id}]

RDS_NS = "AWS/RDS"
RDS_METRICS = [
    "CPUUtilization",
    "DatabaseConnections",
    "ReadIOPS", "WriteIOPS",
    "ReadThroughput", "WriteThroughput",
    "FreeableMemory",
    "FreeStorageSpace",   # RDS (non-Aurora)
    "FreeLocalStorage",   # Aurora
    "ReadLatency", "WriteLatency",
]
