#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/run_review.sh rds_storage_audit --profiles tea-fi steam-fi --regions us-east-1,us-east-2

RDS Storage Audit — Unified Review (Backups & Snapshots)
(גרסה ללא חישוב עלות)

"""

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.csvio import write_csv

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})


# ---------- utils ----------

def iso(dt) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def days_ago(dt) -> Optional[int]:
    if not dt:
        return None
    now = datetime.now(timezone.utc)
    base = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return (now - base).days


# ---------- collectors: DB instance snapshots (manual/automated) ----------

def collect_db_manual_snapshots(sess, region: str, older_than_days: int,
                                profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    try:
        paginator = rds.get_paginator("describe_db_snapshots")
        for page in paginator.paginate(SnapshotType="manual"):
            for s in page.get("DBSnapshots", []):
                created = s.get("SnapshotCreateTime")
                if created and created < cutoff:
                    size_gib = s.get("AllocatedStorage")
                    rows.append({
                        "profile": profile,
                        "account_id": account_id,
                        "region": region,
                        "finding_type": "manual_snapshot_old",
                        "scope": "db",
                        "snapshot_id": s.get("DBSnapshotIdentifier"),
                        "source_db_instance_id": s.get("DBInstanceIdentifier"),
                        "db_instance_id": s.get("DBInstanceIdentifier"),
                        "engine": s.get("Engine"),
                        "snapshot_create_time": iso(created),
                        "days_old": days_ago(created),
                        "storage_gib": size_gib,
                        "remarks": "Manual snapshot older than threshold",
                    })
    except ClientError as e:
        print(f"[{profile}/{region}] manual DB snapshots skipped: {e.response['Error']['Code']}", file=sys.stderr)

    return rows


def collect_db_automated_snapshots(sess, region: str,
                                   profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)

    try:
        paginator = rds.get_paginator("describe_db_snapshots")
        for page in paginator.paginate(SnapshotType="automated"):
            for s in page.get("DBSnapshots", []):
                created = s.get("SnapshotCreateTime")
                size_gib = s.get("AllocatedStorage")
                dbid = s.get("DBInstanceIdentifier")

                finding_type = (
                    "automated_snapshot_active" if dbid else "automated_snapshot_orphan"
                )
                remarks = (
                    "Automated snapshot (active)" if dbid else "Automated snapshot orphan (DB missing)"
                )

                rows.append({
                    "profile": profile,
                    "account_id": account_id,
                    "region": region,
                    "finding_type": finding_type,
                    "scope": "db",
                    "snapshot_id": s.get("DBSnapshotIdentifier"),
                    "db_instance_id": dbid,
                    "engine": s.get("Engine"),
                    "snapshot_create_time": iso(created),
                    "days_old": days_ago(created),
                    "storage_gib": size_gib,
                    "remarks": remarks,
                })
    except ClientError as e:
        print(f"[{profile}/{region}] automated DB snapshots skipped: {e.response['Error']['Code']}", file=sys.stderr)

    return rows


# ---------- collectors: AURORA cluster snapshots (manual/automated) ----------

def collect_cluster_manual_snapshots(sess, region: str, older_than_days: int,
                                     profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)
    cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)

    try:
        paginator = rds.get_paginator("describe_db_cluster_snapshots")
        for page in paginator.paginate(SnapshotType="manual"):
            for s in page.get("DBClusterSnapshots", []):
                created = s.get("SnapshotCreateTime")
                if created and created < cutoff:
                    rows.append({
                        "profile": profile,
                        "account_id": account_id,
                        "region": region,
                        "finding_type": "manual_snapshot_old",
                        "scope": "cluster",
                        "snapshot_id": s.get("DBClusterSnapshotIdentifier"),
                        "aurora_cluster_id": s.get("DBClusterIdentifier"),
                        "engine": s.get("Engine"),
                        "snapshot_create_time": iso(created),
                        "days_old": days_ago(created),
                        "storage_gib": None,
                        "remarks": "Manual cluster snapshot older than threshold",
                    })
    except ClientError as e:
        print(f"[{profile}/{region}] manual CLUSTER snapshots skipped: {e.response['Error']['Code']}", file=sys.stderr)

    return rows


def collect_cluster_automated_snapshots(sess, region: str,
                                        profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)

    try:
        paginator = rds.get_paginator("describe_db_cluster_snapshots")
        for page in paginator.paginate(SnapshotType="automated"):
            for s in page.get("DBClusterSnapshots", []):
                created = s.get("SnapshotCreateTime")
                cluster_id = s.get("DBClusterIdentifier")
                finding_type = (
                    "cluster_automated_snapshot_active" if cluster_id else "cluster_automated_snapshot_orphan"
                )
                remarks = (
                    "Cluster automated snapshot (active)" if cluster_id else "Cluster automated snapshot orphan (cluster missing)"
                )

                rows.append({
                    "profile": profile,
                    "account_id": account_id,
                    "region": region,
                    "finding_type": finding_type,
                    "scope": "cluster",
                    "snapshot_id": s.get("DBClusterSnapshotIdentifier"),
                    "aurora_cluster_id": cluster_id,
                    "engine": s.get("Engine"),
                    "snapshot_create_time": iso(created),
                    "days_old": days_ago(created),
                    "storage_gib": None,
                    "remarks": remarks,
                })
    except ClientError as e:
        print(f"[{profile}/{region}] automated CLUSTER snapshots skipped: {e.response['Error']['Code']}", file=sys.stderr)

    return rows


# ---------- collectors: retention (DB + Cluster) ----------

def collect_db_retention(sess, region: str, max_days: int,
                         profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)
    try:
        paginator = rds.get_paginator("describe_db_instances")
        for page in paginator.paginate():
            for inst in page.get("DBInstances", []):
                brp = inst.get("BackupRetentionPeriod")
                dbid = inst.get("DBInstanceIdentifier")
                engine = inst.get("Engine")
                if brp is None:
                    continue
                if brp == 0:
                    finding_type = "backup_retention_disabled"
                    remarks = "Automated backups disabled (retention=0)"
                elif brp > max_days:
                    finding_type = "backup_retention_high"
                    remarks = f"Reduce retention from {brp}→{max_days} days"
                else:
                    continue
                rows.append({
                    "profile": profile,
                    "account_id": account_id,
                    "region": region,
                    "finding_type": finding_type,
                    "scope": "db",
                    "db_instance_id": dbid,
                    "engine": engine,
                    "backup_retention_days": brp,
                    "recommended_max_days": max_days,
                    "remarks": remarks,
                })
    except ClientError as e:
        print(f"[{profile}/{region}] DB retention skipped: {e.response['Error']['Code']}", file=sys.stderr)
    return rows


def collect_cluster_retention(sess, region: str, max_days: int,
                              profile: str, account_id: str) -> List[Dict]:
    rows: List[Dict] = []
    rds = sess.client("rds", region_name=region, config=CFG)
    try:
        paginator = rds.get_paginator("describe_db_clusters")
        for page in paginator.paginate():
            for c in page.get("DBClusters", []):
                brp = c.get("BackupRetentionPeriod")
                cid = c.get("DBClusterIdentifier")
                engine = c.get("Engine")
                if brp is None:
                    continue
                if brp == 0:
                    finding_type = "cluster_backup_retention_disabled"
                    remarks = "Cluster automated backups disabled (retention=0)"
                elif brp > max_days:
                    finding_type = "cluster_backup_retention_high"
                    remarks = f"Reduce cluster retention from {brp}→{max_days} days"
                else:
                    continue
                rows.append({
                    "profile": profile,
                    "account_id": account_id,
                    "region": region,
                    "finding_type": finding_type,
                    "scope": "cluster",
                    "aurora_cluster_id": cid,
                    "engine": engine,
                    "backup_retention_days": brp,
                    "recommended_max_days": max_days,
                    "remarks": remarks,
                })
    except ClientError as e:
        print(f"[{profile}/{region}] Cluster retention skipped: {e.response['Error']['Code']}", file=sys.stderr)
    return rows


# ---------- CLI / MAIN ----------

def parse_args():
    p = argparse.ArgumentParser(description="RDS Storage Audit (snapshots + retention, no cost calc)")
    p.add_argument("--profiles", nargs="+", required=True)
    p.add_argument("--regions", required=True)
    p.add_argument("--manual-older-than", type=int, default=30)
    p.add_argument("--max-retention-days", type=int, default=7)
    p.add_argument("--outdir", default=None)
    return p.parse_args()


def main():
    import datetime
    args = parse_args()
    regions = parse_regions_arg(args.regions)

    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"rds_storage_audit_{ts}")
    os.makedirs(outdir, exist_ok=True)

    all_rows: List[Dict] = []

    for prof in args.profiles:
        sess = session_for_profile(prof)
        acct, _ = sts_whoami(sess)

        for region in regions:
            all_rows.extend(collect_db_manual_snapshots(sess, region, args.manual_older_than, prof, acct))
            all_rows.extend(collect_db_automated_snapshots(sess, region, prof, acct))
            all_rows.extend(collect_cluster_manual_snapshots(sess, region, args.manual_older_than, prof, acct))
            all_rows.extend(collect_cluster_automated_snapshots(sess, region, prof, acct))
            all_rows.extend(collect_db_retention(sess, region, args.max_retention_days, prof, acct))
            all_rows.extend(collect_cluster_retention(sess, region, args.max_retention_days, prof, acct))

    csv_path = os.path.join(outdir, "rds_storage_audit.csv")
    headers = [
        "profile","account_id","region","finding_type","scope",
        "snapshot_id","source_db_instance_id","db_instance_id","aurora_cluster_id","engine",
        "snapshot_create_time","days_old",
        "backup_retention_days","recommended_max_days",
        "storage_gib","remarks"
    ]
    write_csv(csv_path, all_rows, headers)
    print(f"\nALL DONE -> {csv_path} ({len(all_rows)} rows)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
