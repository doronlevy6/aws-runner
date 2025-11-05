#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""OpenSearch FinOps Review
=================================

Scans Amazon OpenSearch Service domains across profiles/regions, retrieves key
CloudWatch metrics (CPU, JVM pressure, memory, storage, EBS throughput,
latency/rate, network, ML activity, document churn, GC activity, threadpools),
and surfaces optimization insights.

The review produces a tabular summary with Avg/Max/P95 statistics and insights
per metric per domain. Results are written to ``outputs/opensearch_finops_summary.csv``
by default and can be consumed by other automation tooling.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, OperationNotPageableError, ProfileNotFound

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.cloudwatch import get_metric_series, summarize
from scripts.common.csvio import write_rows
from scripts.common.regions import parse_regions_arg

LOGGER = logging.getLogger(__name__)

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
CW_NAMESPACE = "AWS/ES"
DEFAULT_OUTPUT = os.path.join("outputs", "opensearch_finops_summary.csv")


@dataclass(frozen=True)
class WindowSpec:
    name: str
    days: int
    period: int


WINDOWS: Dict[str, WindowSpec] = {
    "24h": WindowSpec("24h", 1, 300),
    # 7d window needs a >= 5min period to stay under the 1,440-point CloudWatch limit
    "7d": WindowSpec("7d", 7, 900),
    "30d": WindowSpec("30d", 30, 3600),
}


@dataclass(frozen=True)
class MetricSpec:
    name: str
    stat: str = "Average"
    unit: str = ""
    convert: str = "identity"
    low: Optional[float] = None
    high: Optional[float] = None
    critical: Optional[float] = None
    windows: Sequence[str] = ("24h", "7d", "30d")
    direction: str = "high"  # "high" (high usage risky) vs "low" (low value risky)
    label: Optional[str] = None


METRICS: Sequence[MetricSpec] = (
    MetricSpec("CPUUtilization", unit="%", convert="percent", low=30.0, high=80.0, critical=90.0, label="CPU utilization"),
    MetricSpec("JVMMemoryPressure", unit="%", convert="percent", low=40.0, high=75.0, critical=85.0, label="JVM memory pressure"),
    MetricSpec("SysMemoryUtilization", unit="%", convert="percent", low=30.0, high=80.0, critical=90.0, label="System memory utilization"),
    MetricSpec("FreeStorageSpace", stat="Minimum", unit="GiB", convert="gib", low=150.0, critical=100.0, direction="low", windows=("24h", "7d", "30d"), label="Free storage"),
    MetricSpec("EBSIOBalance%", unit="%", convert="percent", low=70.0, critical=40.0, direction="low", windows=("24h", "7d"), label="EBS IO balance"),
    MetricSpec("EBSBytesRead", stat="Sum", unit="GiB", convert="gib_per_period", windows=("24h", "7d", "30d"), label="EBS bytes read"),
    MetricSpec("EBSBytesWritten", stat="Sum", unit="GiB", convert="gib_per_period", windows=("24h", "7d", "30d"), label="EBS bytes written"),
    MetricSpec("IndexingLatency", unit="ms", convert="identity", high=200.0, critical=400.0, label="Indexing latency"),
    MetricSpec("SearchLatency", unit="ms", convert="identity", high=200.0, critical=400.0, label="Search latency"),
    MetricSpec("IndexingRate", stat="Sum", unit="ops/s", convert="per_second", windows=("24h", "7d", "30d"), label="Indexing rate"),
    MetricSpec("SearchRate", stat="Sum", unit="ops/s", convert="per_second", windows=("24h", "7d", "30d"), label="Search rate"),
    MetricSpec("NetworkIn", stat="Sum", unit="MiB/s", convert="mib_per_second", windows=("24h", "7d", "30d"), label="Network in"),
    MetricSpec("NetworkOut", stat="Sum", unit="MiB/s", convert="mib_per_second", windows=("24h", "7d", "30d"), label="Network out"),
    MetricSpec("MlExecutingTaskCount", unit="tasks", convert="identity", high=5.0, label="ML executing tasks"),
    MetricSpec("DeletedDocuments", stat="Sum", unit="docs", convert="identity", windows=("24h", "7d", "30d"), label="Deleted documents"),
    MetricSpec("SearchableDocuments", stat="Average", unit="docs", convert="identity", windows=("24h", "7d", "30d"), label="Searchable documents"),
    MetricSpec("JVMGCYoungCollectionCount", stat="Sum", unit="count", convert="identity", windows=("24h", "7d", "30d"), label="JVM young GC count"),
    MetricSpec("JVMGCYoungCollectionTime", stat="Sum", unit="ms", convert="identity", windows=("24h", "7d", "30d"), label="JVM young GC time"),
    MetricSpec("JVMGCOldCollectionCount", stat="Sum", unit="count", convert="identity", windows=("24h", "7d", "30d"), label="JVM old GC count"),
    MetricSpec("JVMGCOldCollectionTime", stat="Sum", unit="ms", convert="identity", windows=("24h", "7d", "30d"), label="JVM old GC time"),
    MetricSpec("ThreadpoolWriteQueue", unit="queue depth", convert="identity", high=50.0, label="Threadpool write queue"),
    MetricSpec("ThreadpoolWriteRejected", stat="Sum", unit="rejections", convert="identity", windows=("24h", "7d", "30d"), label="Threadpool write rejections"),
    MetricSpec("ThreadpoolIndexQueue", unit="queue depth", convert="identity", high=50.0, label="Threadpool index queue"),
    MetricSpec("ThreadpoolIndexRejected", stat="Sum", unit="rejections", convert="identity", windows=("24h", "7d", "30d"), label="Threadpool index rejections"),
    MetricSpec("ThreadpoolSearchQueue", unit="queue depth", convert="identity", high=50.0, label="Threadpool search queue"),
    MetricSpec("ThreadpoolSearchRejected", stat="Sum", unit="rejections", convert="identity", windows=("24h", "7d", "30d"), label="Threadpool search rejections"),
)


@dataclass
class Stats:
    avg: Optional[float]
    p95: Optional[float]
    mx: Optional[float]
    points: int

    @classmethod
    def empty(cls) -> "Stats":
        return cls(None, None, None, 0)


class OpenSearchFinOpsReview:
    """Collects OpenSearch domain metrics and produces optimization insights."""

    def __init__(self, session, account_id: str, region: str) -> None:
        self.session = session
        self.account_id = account_id
        self.region = region
        self._cw = session.client("cloudwatch", region_name=region, config=CFG)
        self._es = self._init_opensearch_client(session, region)

    @staticmethod
    def _init_opensearch_client(session, region: str):
        try:
            return session.client("opensearch", region_name=region, config=CFG)
        except Exception as exc:  # pragma: no cover
            LOGGER.debug("opensearch client unavailable (%s), falling back to es", exc)
            return session.client("es", region_name=region, config=CFG)

    def run(self) -> List[Dict[str, object]]:
        now = datetime.now(timezone.utc)
        domains = self._list_domains()
        if not domains:
            LOGGER.info("[%s] no OpenSearch domains discovered", self.region)
            return []

        rows: List[Dict[str, object]] = []
        domain_metadata = {name: self._describe_domain(name) for name in domains}

        for domain in domains:
            dims = self._metric_dimensions(domain)
            metadata = domain_metadata.get(domain) or {}
            for metric in METRICS:
                stats_by_window = self._collect_metric(metric, dims, now)
                base = stats_by_window.get(metric.windows[0], Stats.empty())
                insight = self._derive_insight(metric, stats_by_window)

                row = {
                    "Profile": None,
                    "AccountId": self.account_id,
                    "Region": self.region,
                    "DomainName": domain,
                    "Metric": metric.label or metric.name,
                    "Window": metric.windows[0],
                    "Avg": base.avg,
                    "P95": base.p95,
                    "Max": base.mx,
                    "Insight": insight,
                    "EngineVersion": metadata.get("EngineVersion"),
                    "InstanceType": self._extract_instance_type(metadata),
                }

                for win in metric.windows[1:]:
                    st = stats_by_window.get(win)
                    if st and st.points:
                        row[f"Avg_{win}"] = st.avg
                        row[f"P95_{win}"] = st.p95
                        row[f"Max_{win}"] = st.mx

                rows.append(row)

        return rows

    def _collect_metric(self, metric: MetricSpec, dims: List[Dict[str, str]], now: datetime) -> Dict[str, Stats]:
        stats: Dict[str, Stats] = {}
        for window_name in metric.windows:
            spec = WINDOWS[window_name]
            start = now - timedelta(days=spec.days)
            series = self._safe_metric_series(metric, dims, start, now, spec.period)
            converted = self._convert_series(metric, series, spec.period)
            avg, p95, mx = summarize(converted)
            stats[window_name] = Stats(avg, p95, mx, len(converted))
        return stats

    def _safe_metric_series(
        self,
        metric: MetricSpec,
        dims: List[Dict[str, str]],
        start: datetime,
        end: datetime,
        period: int,
    ) -> List[float]:
        try:
            return get_metric_series(
                self._cw,
                CW_NAMESPACE,
                metric.name,
                dims,
                start,
                end,
                period,
                stat=metric.stat,
            )
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            LOGGER.warning(
                "[%s][%s] metric %s fetch failed (%s)",
                self.region,
                dims[0].get("Value"),
                metric.name,
                code,
            )
            return []

    def _convert_series(self, metric: MetricSpec, values: Iterable[float], period: int) -> List[float]:
        conv = metric.convert
        if conv in {"identity", "percent"}:
            return [float(v) for v in values]
        if conv in {"gib", "gib_per_period"}:
            return [float(v) / (1024 ** 3) for v in values]
        if conv == "per_second":
            if period <= 0:
                return [float(v) for v in values]
            return [float(v) / period for v in values]
        if conv == "mib_per_second":
            if period <= 0:
                period = 1
            return [float(v) / period / (1024 ** 2) for v in values]
        return [float(v) for v in values]

    def _derive_insight(
        self,
        metric: MetricSpec,
        stats_by_window: Dict[str, Stats],
    ) -> str:
        primary = stats_by_window.get(metric.windows[0], Stats.empty())
        if primary.points == 0:
            return "No datapoints"

        unit = metric.unit or ""
        parts: List[str] = []

        def fmt(val: Optional[float]) -> str:
            if val is None:
                return "n/a"
            if abs(val) >= 100:
                return f"{val:.0f}{unit}"
            if abs(val) >= 10:
                return f"{val:.1f}{unit}"
            if abs(val) >= 1:
                return f"{val:.2f}{unit}"
            return f"{val:.3f}{unit}"

        def pct_change(new: Optional[float], old: Optional[float]) -> Optional[float]:
            if new is None or old is None or abs(old) < 1e-6:
                return None
            return (new - old) / old

        if metric.direction == "high":
            if self._above(primary.mx, metric.critical):
                parts.append(f"Critical peak {fmt(primary.mx)} >= {metric.critical}{unit}")
            elif self._above(primary.p95, metric.high):
                parts.append(f"Sustained high usage (p95 {fmt(primary.p95)})")
            elif self._above(primary.avg, metric.high):
                parts.append(f"Average high at {fmt(primary.avg)}")

            if metric.low is not None and primary.avg is not None and primary.avg < metric.low:
                parts.append(f"Underutilized (avg {fmt(primary.avg)} < {metric.low}{unit})")
        else:
            if metric.critical is not None and self._below(primary.avg, metric.critical):
                parts.append(f"Critical low capacity (avg {fmt(primary.avg)})")
            elif metric.low is not None and self._below(primary.avg, metric.low):
                parts.append(f"Low reserve (avg {fmt(primary.avg)})")

            if metric.high is not None and primary.avg is not None and primary.avg > metric.high:
                parts.append(f"Healthy buffer (avg {fmt(primary.avg)} > {metric.high}{unit})")

        if len(metric.windows) > 1:
            older_name = metric.windows[1]
            older_stats = stats_by_window.get(older_name)
            change = pct_change(primary.avg, older_stats.avg if older_stats else None)
            if change is not None and abs(change) >= 0.2:
                direction = "higher" if change > 0 else "lower"
                parts.append(f"{metric.windows[0]} avg {direction} {abs(change)*100:.0f}% vs {older_name}")

        name = metric.name
        if name == "DeletedDocuments" and primary.avg and primary.avg > 0:
            parts.append(f"Deletes observed (~{fmt(primary.avg)} per {metric.windows[0]})")
        elif name.endswith("Rejected") and primary.avg and primary.avg > 0:
            parts.append("Threadpool rejections detected; consider scaling")
        elif name.endswith("Queue") and self._above(primary.mx, metric.high):
            parts.append("Queue growth indicates saturation")

        if not parts:
            parts.append(f"Avg {fmt(primary.avg)} (p95 {fmt(primary.p95)})")

        return "; ".join(parts)

    @staticmethod
    def _above(value: Optional[float], threshold: Optional[float]) -> bool:
        return value is not None and threshold is not None and value >= threshold

    @staticmethod
    def _below(value: Optional[float], threshold: Optional[float]) -> bool:
        return value is not None and threshold is not None and value <= threshold

    def _list_domains(self) -> List[str]:
        domains: List[str] = []
        try:
            paginator = self._es.get_paginator("list_domain_names")
            for page in paginator.paginate(EngineType="OpenSearch"):
                for entry in page.get("DomainNames", []) or []:
                    name = entry.get("DomainName")
                    if name:
                        domains.append(name)
        except OperationNotPageableError:
            try:
                resp = self._es.list_domain_names(EngineType="OpenSearch")
            except Exception as exc:  # pragma: no cover - network/SDK errors
                LOGGER.error("[%s] failed to list domains: %s", self.region, exc)
                return []
            for entry in resp.get("DomainNames", []) or []:
                name = entry.get("DomainName") if isinstance(entry, dict) else entry
                if name:
                    domains.append(name)
        except Exception as exc:
            LOGGER.error("[%s] failed to list domains: %s", self.region, exc)
            return []
        return sorted(set(domains))

    def _describe_domain(self, name: str) -> Dict[str, object]:
        try:
            resp = self._es.describe_domain(DomainName=name)
            status = resp.get("DomainStatus", {}) or {}
            return status
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "Unknown")
            LOGGER.warning("[%s][%s] describe_domain failed (%s)", self.region, name, code)
            return {}

    def _metric_dimensions(self, domain_name: str) -> List[Dict[str, str]]:
        return [
            {"Name": "DomainName", "Value": domain_name},
            {"Name": "ClientId", "Value": self.account_id},
        ]

    @staticmethod
    def _extract_instance_type(metadata: Dict[str, object]) -> Optional[str]:
        cluster = metadata.get("ClusterConfig") if metadata else None
        if isinstance(cluster, dict):
            return cluster.get("InstanceType")
        return None


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Amazon OpenSearch FinOps review")
    parser.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (space separated)")
    parser.add_argument("--regions", required=True, help="Comma-separated AWS regions (e.g. us-east-1,eu-west-1)")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output path (default: outputs/opensearch_finops_summary.csv)")
    return parser.parse_args(argv)


def main(args: Optional[Sequence[str]] = None) -> int:
    parsed = parse_args(args)
    try:
        regions = parse_regions_arg(parsed.regions)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 2

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    all_rows: List[Dict[str, object]] = []

    for profile in parsed.profiles:
        print(f"\n[profile: {profile}]", file=sys.stderr)
        try:
            session = session_for_profile(profile)
        except ProfileNotFound:
            print(f"  ! profile '{profile}' not found", file=sys.stderr)
            continue

        try:
            account_id, caller = sts_whoami(session)
            print(f"  account: {account_id}", file=sys.stderr)
            print(f"  caller : {caller}", file=sys.stderr)
        except ClientError as exc:
            print(f"  ! STS whoami failed: {exc}", file=sys.stderr)
            continue

        for region in regions:
            review = OpenSearchFinOpsReview(session, account_id, region)
            rows = review.run()
            if not rows:
                print(f"  [{region}] no data", file=sys.stderr)
                continue
            for row in rows:
                row["Profile"] = profile
            all_rows.extend(rows)
            print(f"  [{region}] collected {len(rows)} metric rows", file=sys.stderr)

    if not all_rows:
        print("No results collected; nothing written.", file=sys.stderr)
        return 0

    write_rows(parsed.output, all_rows)
    print(f"\n-> wrote {len(all_rows)} rows to {parsed.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
