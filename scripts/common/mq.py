#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from typing import Optional, Tuple, Dict, Any, List
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

def list_brokers(session, region: str) -> List[Dict[str, Any]]:
    """List Amazon MQ brokers (minimal fields)."""
    mq = session.client("mq", region_name=region, config=CFG)
    out: List[Dict[str, Any]] = []
    paginator = mq.get_paginator("list_brokers")
    for page in paginator.paginate(PaginationConfig={"PageSize": 50}):
        out.extend(page.get("BrokerSummaries", []) or [])
    return out

def describe_broker(session, region: str, broker_id: str) -> Optional[Dict[str, Any]]:
    """Describe a single MQ broker safely."""
    mq = session.client("mq", region_name=region, config=CFG)
    try:
        return mq.describe_broker(BrokerId=broker_id)
    except ClientError:
        return None

def find_mq_log_group(session, region: str, broker_id: str, broker_name: Optional[str]) -> Tuple[Optional[str], Optional[int], bool]:
    """
    Locate CloudWatch Logs group for this broker.
    Heuristics: groups starting with '/aws/amazonmq', prefer one containing broker_id, else broker_name.
    Returns: (group_name, retention_days or 0 if unlimited/undefined, enabled_flag)
    """
    logs = session.client("logs", region_name=region, config=CFG)
    chosen_name: Optional[str] = None
    chosen_retention = 0

    prefixes = ["/aws/amazonmq", "/aws/amazonmq/broker", "/aws/amazonmq/broker/"]
    for prefix in prefixes:
        token = None
        try:
            while True:
                params: Dict[str, Any] = {"logGroupNamePrefix": prefix}
                if token:
                    params["nextToken"] = token
                resp = logs.describe_log_groups(**params)
                for lg in resp.get("logGroups", []) or []:
                    name = lg.get("logGroupName")
                    if not name:
                        continue
                    # strict match (id)
                    if broker_id and broker_id in name:
                        return (name, lg.get("retentionInDays") or 0, True)
                    # fallback (name)
                    if (not chosen_name) and broker_name and broker_name in name:
                        chosen_name = name
                        chosen_retention = lg.get("retentionInDays") or 0
                token = resp.get("nextToken")
                if not token:
                    break
        except ClientError:
            continue

    if chosen_name:
        return (chosen_name, chosen_retention, True)
    return (None, None, False)

def backup_recovery_points(session, region: str, resource_arn: str) -> Tuple[int, Optional[str]]:
    """
    Count AWS Backup recovery points for the broker resource ARN.
    Returns: (count, latest_recovery_point_iso)  latest may be None if none exist.

    API: list_recovery_points_by_resource(ResourceArn=..., MaxResults=?, NextToken=?)
    """
    bkp = session.client("backup", region_name=region, config=CFG)
    count = 0
    latest_iso: Optional[str] = None
    token: Optional[str] = None

    if not resource_arn:
        return (0, None)

    try:
        while True:
            params: Dict[str, Any] = {"ResourceArn": resource_arn, "MaxResults": 1000}
            if token:
                params["NextToken"] = token
            resp = bkp.list_recovery_points_by_resource(**params)

            for rp in resp.get("RecoveryPoints", []) or []:
                count += 1
                ctime = rp.get("CreationDate")
                if ctime:
                    iso = ctime.replace(microsecond=0).isoformat()
                    if (latest_iso is None) or (iso > latest_iso):
                        latest_iso = iso

            token = resp.get("NextToken")
            if not token:
                break
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"    [backup] skip ({code})", flush=True)
        # אין הרשאות או לא מוגדר גיבוי — נחזיר 0/None בשקט
        return (0, None)

    return (count, latest_iso)

def any_flow_logs_enabled(session, region: str) -> bool:
    """
    Region-level indicator: return True if there exists at least one VPC Flow Logs resource in region.
    """
    ec2 = session.client("ec2", region_name=region, config=CFG)
    try:
        resp = ec2.describe_flow_logs(MaxResults=5)
        return bool(resp.get("FlowLogs"))
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [flowlogs] skip ({code})", flush=True)
        return False