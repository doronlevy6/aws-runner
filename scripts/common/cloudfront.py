#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Common helpers for CloudFront reviews.
- CloudFront service is GLOBAL (no region selector).
- CloudFront **metrics** are queried via CloudWatch **in us-east-1** with Dimensions:
    DistributionId=<ID>, Region=Global
- Requests/Bytes/ErrorRate are available by default.
- CacheHitRate exists in CloudWatch but may be None unless "Additional metrics" are enabled per distribution.
No external deps besides boto3/botocore/stdlib.
"""

from typing import Dict, Tuple, Optional, List
import sys
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
CF_NS = "AWS/CloudFront"

# ---------- Clients ----------
def _cf(session):
    # CloudFront is global (no region)
    return session.client("cloudfront", config=CFG)

def _cw(session):
    # CloudFront metrics live in us-east-1 with Region=Global dimension
    return session.client("cloudwatch", region_name="us-east-1", config=CFG)

# ---------- Distributions / Config ----------
def list_all_distributions(session) -> List[Dict]:
    """Return minimal info for all distributions in the account/profile."""
    cf = _cf(session)
    out: List[Dict] = []
    marker = None
    while True:
        kwargs = {"MaxItems": "100"}
        if marker:
            kwargs["Marker"] = marker
        resp = cf.list_distributions(**kwargs)
        dist_list = resp.get("DistributionList", {})
        for item in (dist_list.get("Items") or []):
            out.append({
                "Id": item["Id"],
                "DomainName": item.get("DomainName"),
                "Enabled": item.get("Enabled", False),
                "Staging": item.get("Staging", False),
                "ARN": item.get("ARN"),
                "PriceClass": item.get("PriceClass"),
                "WebACLId": item.get("WebACLId"),
            })
        if not dist_list.get("IsTruncated"):
            break
        marker = dist_list.get("NextMarker")
    return out

def get_distribution_config(session, dist_id: str) -> Dict:
    """Return full DistributionConfig for a given distribution."""
    cf = _cf(session)
    resp = cf.get_distribution_config(Id=dist_id)
    return resp.get("DistributionConfig", {})

# ---------- Origins & Behaviors ----------
def is_s3_origin(origin: Dict) -> bool:
    return "S3OriginConfig" in origin

def origin_oai_oac_flags(origin: Dict) -> Tuple[bool, bool, Optional[str]]:
    """
    Returns (has_oai, has_oac, origin_type)
      has_oai: S3OriginConfig.OriginAccessIdentity is non-empty
      has_oac: OriginAccessControlId present (OAC)
      origin_type: 's3' / 'custom' / None
    """
    otype = "s3" if is_s3_origin(origin) else ("custom" if "CustomOriginConfig" in origin else None)
    has_oai = bool(origin.get("S3OriginConfig", {}).get("OriginAccessIdentity"))
    has_oac = bool(origin.get("OriginAccessControlId"))
    return has_oai, has_oac, otype

def _cache_policy_config(session, policy_id: str, _cache: Dict[str, Dict]) -> Optional[Dict]:
    """Fetch CachePolicyConfig by ID and memoize."""
    if policy_id in _cache:
        return _cache[policy_id]
    try:
        cf = _cf(session)
        resp = cf.get_cache_policy(Id=policy_id)
        cfg = resp.get("CachePolicy", {}).get("CachePolicyConfig")
        if cfg:
            _cache[policy_id] = cfg
        return cfg
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[cache-policy:{policy_id}] skip ({code})", file=sys.stderr)
        return None

def analyze_behavior(session, behavior: Dict, cache_policies: Dict[str, Dict]) -> Dict:
    """
    Normalize behavior into TTLs + cache key forwarding (queries/cookies),
    supporting both modern CachePolicy and legacy ForwardedValues.
    """
    out: Dict = {
        "cache_policy_mode": None,  # 'cache_policy' | 'legacy'
        "cache_policy_id": None,
        "origin_req_policy_id": behavior.get("OriginRequestPolicyId"),
        "min_ttl": None,
        "default_ttl": None,
        "max_ttl": None,
        "query_behavior": None,      # none | whitelist | all | allExcept
        "query_items_count": None,
        "cookies_behavior": None,    # none | whitelist | all | allExcept
        "cookies_items_count": None,
    }

    policy_id = behavior.get("CachePolicyId")
    if policy_id:
        out["cache_policy_mode"] = "cache_policy"
        out["cache_policy_id"] = policy_id
        cfg = _cache_policy_config(session, policy_id, cache_policies) or {}

        out["min_ttl"] = cfg.get("MinTTL")
        out["default_ttl"] = cfg.get("DefaultTTL")
        out["max_ttl"] = cfg.get("MaxTTL")

        params = (cfg.get("ParametersInCacheKeyAndForwardedToOrigin") or {})
        qcfg = (params.get("QueryStringsConfig") or {})
        ccfg = (params.get("CookiesConfig") or {})

        out["query_behavior"] = qcfg.get("QueryStringBehavior")
        out["query_items_count"] = (qcfg.get("QueryStrings") or {}).get("Quantity")
        out["cookies_behavior"] = ccfg.get("CookieBehavior")
        out["cookies_items_count"] = (ccfg.get("Cookies") or {}).get("Quantity")

    else:
        # Legacy
        out["cache_policy_mode"] = "legacy"
        out["min_ttl"] = behavior.get("MinTTL")
        out["default_ttl"] = behavior.get("DefaultTTL")
        out["max_ttl"] = behavior.get("MaxTTL")

        fwd = behavior.get("ForwardedValues") or {}
        # Querystring
        q_bool = fwd.get("QueryString")
        if q_bool is True:
            keys = (fwd.get("QueryStringCacheKeys") or {})
            out["query_behavior"] = "whitelist" if (keys.get("Quantity") or 0) > 0 else "all"
            out["query_items_count"] = keys.get("Quantity")
        elif q_bool is False:
            out["query_behavior"] = "none"
        # Cookies
        ck = fwd.get("Cookies") or {}
        out["cookies_behavior"] = ck.get("Forward")
        names = (ck.get("WhitelistedNames") or {})
        out["cookies_items_count"] = names.get("Quantity")

    return out

# ---------- Metrics (CW) ----------
def _summarize_points(datapoints: List[Dict], stat_key: str):
    """Return (sum, avg) over datapoints using stat_key ('Sum' or 'Average')."""
    if not datapoints:
        return (None, None)
    vals = [p.get(stat_key) for p in datapoints if p.get(stat_key) is not None]
    if not vals:
        return (None, None)
    s = float(sum(vals))
    a = s / float(len(vals))
    return (s, a)

def _get_metric(cw, metric_name: str, stat: str, dist_id: str, start, end, period: int):
    """Single metric fallback helper."""
    try:
        resp = cw.get_metric_statistics(
            Namespace=CF_NS,
            MetricName=metric_name,
            Dimensions=[
                {"Name": "DistributionId", "Value": dist_id},
                {"Name": "Region", "Value": "Global"},
            ],
            StartTime=start,
            EndTime=end,
            Period=period,
            Statistics=[stat],
        )
        dps = resp.get("Datapoints", [])
        return _summarize_points(dps, stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[cw:{metric_name}/{dist_id}] skip ({code})", file=sys.stderr)
        return (None, None)

def get_cf_metrics_bulk(cw, dist_id: str, start, end, period: int) -> Dict:
    """
    One call (GetMetricData) for:
      - Requests (Sum), BytesDownloaded (Sum), TotalErrorRate (Average), CacheHitRate (Average)
    Returns:
      requests_sum, bytes_downloaded_sum, total_error_rate_avg_pct, cache_hit_rate_avg_pct
    Note: cache_hit_rate_avg_pct may be None if Additional metrics aren't enabled.
    """
    def mid(name):  # metric id
        return name.replace(" ", "").replace("%", "").replace("/", "_").lower()

    metrics = [
        ("Requests", "Sum"),
        ("BytesDownloaded", "Sum"),
        ("TotalErrorRate", "Average"),
        ("CacheHitRate", "Average"),
    ]
    queries = []
    for metric, stat in metrics:
        queries.append({
            "Id": mid(metric),
            "MetricStat": {
                "Metric": {
                    "Namespace": CF_NS,
                    "MetricName": metric,
                    "Dimensions": [
                        {"Name": "DistributionId", "Value": dist_id},
                        {"Name": "Region", "Value": "Global"},
                    ],
                },
                "Period": period,
                "Stat": stat,
            },
            "ReturnData": True,
        })

    try:
        resp = cw.get_metric_data(
            MetricDataQueries=queries,
            StartTime=start,
            EndTime=end,
            ScanBy="TimestampAscending",
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[cw:GetMetricData/{dist_id}] skip ({code})", file=sys.stderr)
        return {
            "requests_sum": None,
            "bytes_downloaded_sum": None,
            "total_error_rate_avg_pct": None,
            "cache_hit_rate_avg_pct": None,
        }

    series = {r["Id"]: r for r in resp.get("MetricDataResults", [])}

    def sum_series(mid_):
        vals = series.get(mid_, {}).get("Values") or []
        return float(sum(vals)) if vals else None

    def avg_series(mid_):
        vals = series.get(mid_, {}).get("Values") or []
        return (float(sum(vals)) / float(len(vals))) if vals else None

    return {
        "requests_sum":              sum_series(mid("Requests")),
        "bytes_downloaded_sum":      sum_series(mid("BytesDownloaded")),
        "total_error_rate_avg_pct":  avg_series(mid("TotalErrorRate")),
        "cache_hit_rate_avg_pct":    avg_series(mid("CacheHitRate")),  # may be None
    }

def get_cf_cache_stats(cw, dist_id: str, start, end, period: int) -> Dict:
    """Try bulk first; fallback to single-metric calls if needed."""
    bulk = get_cf_metrics_bulk(cw, dist_id, start, end, period)
    if any(v is not None for v in bulk.values()):
        return bulk

    # Fallback
    requests_sum, _ = _get_metric(cw, "Requests", "Sum", dist_id, start, end, period)
    bytes_sum, _ = _get_metric(cw, "BytesDownloaded", "Sum", dist_id, start, end, period)
    _, terr_avg = _get_metric(cw, "TotalErrorRate", "Average", dist_id, start, end, period)
    _, hit_avg = _get_metric(cw, "CacheHitRate", "Average", dist_id, start, end, period)

    return {
        "requests_sum": requests_sum,
        "bytes_downloaded_sum": bytes_sum,
        "total_error_rate_avg_pct": terr_avg,
        "cache_hit_rate_avg_pct": hit_avg,
    }

def safe_gib(bytes_val: Optional[float]) -> Optional[float]:
    if bytes_val is None:
        return None
    try:
        return float(bytes_val) / (1024.0 ** 3)
    except Exception:
        return None
