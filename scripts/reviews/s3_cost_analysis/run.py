#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/run_review.sh s3_cost_analysis --profiles steam-fi tea-fi --regions us-east-1,eu-west-1 --days 30

Amazon S3 Cost & Usage Analysis
- Collects bucket metadata, storage metrics, and configuration posture
- Computes optimization score + recommendations per bucket
- Outputs per-profile CSVs and a consolidated CSV under outputs/s3_cost_analysis_<timestamp>/
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.cloudwatch import get_metric_series, window
from scripts.common.csvio import write_rows

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

S3_NS = "AWS/S3"
STORAGE_TYPE_MAP: Dict[str, str] = {
    "StandardStorage": "standard_storage_bytes",
    "StandardIAStorage": "standard_ia_storage_bytes",
    "OneZoneIAStorage": "onezone_ia_storage_bytes",
    "ReducedRedundancyStorage": "rrs_storage_bytes",
    "GlacierStorage": "glacier_storage_bytes",
    "GlacierStagingStorage": "glacier_staging_storage_bytes",
    "GlacierObjectOverhead": "glacier_overhead_storage_bytes",
    "GlacierInstantRetrievalStorage": "glacier_ir_storage_bytes",
    "DeepArchiveStorage": "deep_archive_storage_bytes",
    "IntelligentTieringFAStorage": "intelligent_tiering_frequent_bytes",
    "IntelligentTieringAAStorage": "intelligent_tiering_automatic_archive_bytes",
    "IntelligentTieringIAStorage": "intelligent_tiering_infrequent_bytes",
    "IntelligentTieringAIAStorage": "intelligent_tiering_archive_infrequent_bytes",
}


def bytes_to_gib(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) / (1024 ** 3)
    except Exception:
        return None


def format_dt(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def resolve_bucket_region(raw: Optional[str]) -> str:
    """
    Normalise S3 LocationConstraint to standard region name (None -> us-east-1, 'EU' -> eu-west-1, etc.)
    """
    if not raw:
        return "us-east-1"
    if raw == "EU":
        return "eu-west-1"
    if raw == "US":
        return "us-east-1"
    return raw


def safe_metric(
    cw,
    bucket: str,
    storage_type: str,
    start,
    end,
    period: int,
    stat: str = "Average",
) -> List[float]:
    dims = [
        {"Name": "BucketName", "Value": bucket},
        {"Name": "StorageType", "Value": storage_type},
    ]
    try:
        return get_metric_series(
            cw, S3_NS, "BucketSizeBytes", dims, start, end, period, stat=stat
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric BucketSizeBytes/{storage_type}] skip ({code})", file=sys.stderr)
        return []


def safe_obj_metric(
    cw,
    bucket: str,
    start,
    end,
    period: int,
) -> List[float]:
    dims = [
        {"Name": "BucketName", "Value": bucket},
        {"Name": "StorageType", "Value": "AllStorageTypes"},
    ]
    try:
        return get_metric_series(
            cw, S3_NS, "NumberOfObjects", dims, start, end, period, stat="Average"
        )
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric NumberOfObjects] skip ({code})", file=sys.stderr)
        return []


def gather_bucket_config(s3, bucket: str) -> Dict[str, Optional[object]]:
    cfg: Dict[str, Optional[object]] = {
        "has_lifecycle": False,
        "lifecycle_rules": 0,
        "has_intelligent_tiering": False,
        "intelligent_tiering_configs": 0,
        "has_versioning": False,
        "mfa_delete": False,
        "has_encryption": False,
        "default_encryption_type": None,
        "has_access_logging": False,
    }

    try:
        resp = s3.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = resp.get("Rules", []) or []
        cfg["has_lifecycle"] = bool(rules)
        cfg["lifecycle_rules"] = len(rules)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") not in ("NoSuchLifecycleConfiguration", "NoSuchBucket"):
            print(f"    lifecycle check failed: {e.response.get('Error', {}).get('Code')}", file=sys.stderr)

    try:
        resp = s3.list_bucket_intelligent_tiering_configurations(Bucket=bucket)
        configs = resp.get("IntelligentTieringConfigurationList", []) or []
    except s3.exceptions.ClientError:
        configs = []
    count = len(configs)
    if count > 0:
        cfg["has_intelligent_tiering"] = True
        cfg["intelligent_tiering_configs"] = count

    try:
        vers = s3.get_bucket_versioning(Bucket=bucket)
        cfg["has_versioning"] = vers.get("Status") == "Enabled"
        cfg["mfa_delete"] = vers.get("MFADelete") == "Enabled"
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        print(f"    versioning check failed: {code}", file=sys.stderr)

    try:
        enc = s3.get_bucket_encryption(Bucket=bucket)
        rules = enc.get("ServerSideEncryptionConfiguration", {}).get("Rules", []) or []
        if rules:
            cfg["has_encryption"] = True
            first = rules[0].get("ApplyServerSideEncryptionByDefault", {})
            cfg["default_encryption_type"] = first.get("SSEAlgorithm")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("ServerSideEncryptionConfigurationNotFoundError", "NoSuchBucket", "AccessDenied"):
            print(f"    encryption check failed: {code}", file=sys.stderr)

    try:
        log = s3.get_bucket_logging(Bucket=bucket)
        cfg["has_access_logging"] = "LoggingEnabled" in log
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code not in ("NoSuchBucket", "AccessDenied"):
            print(f"    logging check failed: {code}", file=sys.stderr)

    return cfg


def count_incomplete_uploads(s3, bucket: str) -> int:
    """
    Returns the number of incomplete multipart uploads for the given bucket.
    These represent partial uploads that can be safely cleaned up by enabling lifecycle rules.
    """
    try:
        resp = s3.list_multipart_uploads(Bucket=bucket)
        uploads = resp.get("Uploads", []) or []
        return len(uploads)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code not in ("NoSuchUpload", "NoSuchBucket", "AccessDenied"):
            print(f"    multipart uploads check failed: {code}", file=sys.stderr)
        return 0


def bucket_metrics(
    cw,
    bucket: str,
    start,
    end,
    period: int,
) -> Tuple[Dict[str, float], Optional[int]]:
    storage: Dict[str, float] = {}
    for storage_type, field in STORAGE_TYPE_MAP.items():
        series = safe_metric(cw, bucket, storage_type, start, end, period)
        if series:
            storage[field] = series[-1]

    obj_series = safe_obj_metric(cw, bucket, start, end, period)
    obj_count = int(obj_series[-1]) if obj_series else None

    return storage, obj_count


def cold_storage_bytes(storage: Dict[str, float]) -> float:
    cold_keys = [
        "standard_ia_storage_bytes",
        "onezone_ia_storage_bytes",
        "glacier_storage_bytes",
        "glacier_staging_storage_bytes",
        "glacier_overhead_storage_bytes",
        "glacier_ir_storage_bytes",
        "deep_archive_storage_bytes",
        "intelligent_tiering_automatic_archive_bytes",
        "intelligent_tiering_infrequent_bytes",
        "intelligent_tiering_archive_infrequent_bytes",
    ]
    return sum(storage.get(k, 0.0) for k in cold_keys)


def compute_recommendations(row: Dict[str, object]) -> Tuple[float, str]:
    score = 100
    recs: List[str] = []

    total_gb = row.get("total_storage_gb") or 0.0
    standard_gb = row.get("standard_storage_gb") or 0.0
    lifecycle = bool(row.get("has_lifecycle"))
    intelligent = bool(row.get("has_intelligent_tiering"))
    object_count = row.get("object_count") or 0

    # High standard storage without lifecycle/intelligent tiering
    if standard_gb >= 50 and not lifecycle:
        score -= 25
        recs.append("Define lifecycle to transition Standard objects")
    if standard_gb >= 50 and not intelligent and object_count >= 1_000_000:
        score -= 15
        recs.append("Evaluate S3 Intelligent-Tiering for high object count")

    cold_ratio = row.get("cold_storage_ratio_pct") or 0.0
    if total_gb >= 50 and cold_ratio < 20.0 and not lifecycle:
        score -= 10

    if row.get("has_versioning") and not lifecycle:
        score -= 10
        recs.append("Add lifecycle rules for noncurrent versions")

    if not row.get("has_encryption"):
        score -= 5
        recs.append("Enable default encryption (SSE-S3 or KMS)")

    if not row.get("has_access_logging") and total_gb >= 100:
        score -= 5

    if score < 0:
        score = 0

    return float(score), "; ".join(recs)


def collect_profile(
    profile: str,
    regions_filter: Optional[List[str]],
    days: int,
    period: int,
) -> Tuple[List[Dict[str, object]], str]:
    sess = session_for_profile(profile)
    acct_id, _ = sts_whoami(sess)
    s3 = sess.client("s3", config=CFG)

    buckets_resp = s3.list_buckets()
    buckets = buckets_resp.get("Buckets", []) or []
    start, end = window(days + 1)  # include buffer day for daily metrics

    print(f"  account: {acct_id}", file=sys.stderr)
    print(f"  discovered {len(buckets)} buckets", file=sys.stderr)

    rows: List[Dict[str, object]] = []

    for bucket in buckets:
        bucket_name = bucket["Name"]
        try:
            loc = s3.get_bucket_location(Bucket=bucket_name)
            region = resolve_bucket_region(loc.get("LocationConstraint"))
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            print(f"[{profile}] bucket {bucket_name}: unable to resolve region ({code})", file=sys.stderr)
            continue

        if regions_filter and region not in regions_filter:
            continue

        print(f"  - bucket {bucket_name} ({region})", file=sys.stderr)

        cw = sess.client("cloudwatch", region_name=region, config=CFG)
        storage_bytes, obj_count = bucket_metrics(cw, bucket_name, start, end, period)

        if not storage_bytes and obj_count is None:
            # Skip buckets without metrics (e.g., newly created <24h)
            print(f"[{profile}] bucket {bucket_name}: no metrics returned", file=sys.stderr)
            continue

        cfg = gather_bucket_config(s3, bucket_name)

        total_bytes = sum(storage_bytes.values())
        total_gb = bytes_to_gib(total_bytes) or 0.0

        row: Dict[str, object] = {
            "profile": profile,
            "account_id": acct_id,
            "bucket_name": bucket_name,
            "region": region,
            "creation_date": format_dt(bucket.get("CreationDate")),
            "total_storage_gb": round(total_gb, 3),
            "object_count": obj_count,
        }

        for storage_type, field in STORAGE_TYPE_MAP.items():
            bytes_val = storage_bytes.get(field)
            gb_val = bytes_to_gib(bytes_val) if bytes_val is not None else None
            row[field.replace("_bytes", "_gb")] = round(gb_val, 3) if gb_val is not None else None

        cold_bytes = cold_storage_bytes(storage_bytes)
        cold_ratio_pct = (cold_bytes / total_bytes * 100.0) if total_bytes > 0 else 0.0

        row["cold_storage_ratio_pct"] = round(cold_ratio_pct, 2)

        class_breakdown = {
            "Standard": row.get("standard_storage_gb") or 0.0,
            "Standard-IA": row.get("standard_ia_storage_gb") or 0.0,
            "One Zone-IA": row.get("onezone_ia_storage_gb") or 0.0,
            "Intelligent-Tiering Frequent": row.get("intelligent_tiering_frequent_gb") or 0.0,
            "Intelligent-Tiering Infrequent": row.get("intelligent_tiering_infrequent_gb") or 0.0,
            "Intelligent-Tiering Archive": (
                (row.get("intelligent_tiering_automatic_archive_gb") or 0.0)
                + (row.get("intelligent_tiering_archive_infrequent_gb") or 0.0)
            ),
            "Glacier Instant Retrieval": row.get("glacier_ir_storage_gb") or 0.0,
            "Glacier Flexible Retrieval": row.get("glacier_storage_gb") or 0.0,
            "Glacier Deep Archive": row.get("deep_archive_storage_gb") or 0.0,
        }
        dominant = None
        if total_gb > 0:
            dominant = max(class_breakdown.items(), key=lambda kv: kv[1])[0]
        row["dominant_storage_class"] = dominant

        row.update(cfg)

        incomplete_uploads = count_incomplete_uploads(s3, bucket_name)
        row["incomplete_uploads_count"] = incomplete_uploads

        score, rec = compute_recommendations(row)
        row["optimization_score"] = score
        row["primary_recommendation"] = rec

        rows.append(row)

    return rows, acct_id


def parse_regions(regions_arg: Optional[str]) -> Optional[List[str]]:
    if not regions_arg or regions_arg.strip().lower() == "all":
        return None
    parts = [r.strip() for r in regions_arg.split(",") if r.strip()]
    return parts or None


def parse_args():
    parser = argparse.ArgumentParser(description="Amazon S3 Cost & Usage Analysis")
    parser.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g. steam-fi tea-fi)")
    parser.add_argument("--regions", default="all", help="Comma-separated regions or 'all' (default: all)")
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default 30)")
    parser.add_argument(
        "--period",
        type=int,
        default=86400,
        help="CloudWatch period seconds (default 86400 — S3 metrics are daily)",
    )
    parser.add_argument("--outdir", default=None, help="Override output directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    regions_filter = parse_regions(args.regions)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"s3_cost_analysis_{ts}")
    os.makedirs(outdir, exist_ok=True)

    print("== Amazon S3 Cost & Usage Analysis ==", file=sys.stderr)
    if regions_filter:
        print(f"  regions filter: {', '.join(regions_filter)}", file=sys.stderr)
    else:
        print("  regions: all buckets", file=sys.stderr)
    effective_period = max(args.period, 86400)
    print(f"  days={args.days}, period={effective_period}s", file=sys.stderr)
    if effective_period != args.period:
        print(f"    (requested {args.period}s -> adjusted to daily S3 frequency)", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

    all_rows: List[Dict[str, object]] = []

    for profile in args.profiles:
        print(f"\n[profile: {profile}]", file=sys.stderr)
        try:
            rows, account_id = collect_profile(profile, regions_filter, args.days, effective_period)
        except ProfileNotFound:
            print("  ! profile not found in local AWS config", file=sys.stderr)
            continue
        except ClientError as e:
            print(f"  ! AWS error: {e}", file=sys.stderr)
            continue

        if not rows:
            print("  -> no S3 metrics collected (skipped/empty)", file=sys.stderr)
            continue

        outfile = os.path.join(outdir, f"s3_{profile}.csv")
        write_rows(outfile, rows)
        print(f"  -> wrote {len(rows)} rows to {outfile}", file=sys.stderr)

        for row in rows:
            row_copy = dict(row)
            row_copy["profile"] = profile
            row_copy["account_id"] = account_id
            all_rows.append(row_copy)

    if all_rows:
        consolidated = os.path.join(outdir, "s3_all_profiles.csv")
        write_rows(consolidated, all_rows)
        print(f"\nALL DONE -> {consolidated}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
