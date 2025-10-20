# scripts/common/ecs.py
# -*- coding: utf-8 -*-

import sys
from typing import List, Dict, Tuple, Optional
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

# ---------- Existence check (כמו rds_instances_exist_in_region) ----------
def ecs_clusters_exist_in_region(session, region: str) -> bool:
    """
    Return True if the region contains at least one ECS cluster (ACTIVE).
    Safe to call with read-only permissions.
    """
    ecs = session.client("ecs", region_name=region, config=CFG)
    try:
        paginator = ecs.get_paginator("list_clusters")
        for page in paginator.paginate(PaginationConfig={"PageSize": 50}):
            if page.get("clusterArns"):
                return True
        return False
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[{region}] skip ({code})", file=sys.stderr)
        return False

# ---------- List/Describe ----------
def list_clusters_arns(ecs) -> List[str]:
    arns: List[str] = []
    paginator = ecs.get_paginator("list_clusters")
    for page in paginator.paginate(PaginationConfig={"PageSize": 100}):
        arns.extend(page.get("clusterArns", []))
    return arns

def list_services_arns(ecs, cluster_arn: str) -> List[str]:
    arns: List[str] = []
    paginator = ecs.get_paginator("list_services")
    for page in paginator.paginate(cluster=cluster_arn, PaginationConfig={"PageSize": 100}):
        arns.extend(page.get("serviceArns", []))
    return arns

def describe_services_safe(ecs, cluster_arn: str, service_arns: List[str]) -> List[Dict]:
    out: List[Dict] = []
    for i in range(0, len(service_arns), 10):  # מגבלת API
        batch = service_arns[i:i+10]
        try:
            resp = ecs.describe_services(cluster=cluster_arn, services=batch, include=["TAGS"])
            out.extend(resp.get("services", []))
        except ClientError:
            continue
    return out

# ---------- ARN / Names ----------
def cluster_name_from_arn(arn: str) -> str:
    # arn:aws:ecs:region:acct:cluster/my-cluster
    return arn.split("/")[-1] if arn else arn

# ---------- CloudWatch Dimensions helpers (תואם לסגנון RDS rds_dim) ----------
def ecs_cluster_dim(cluster_name: str):
    return [{"Name": "ClusterName", "Value": cluster_name}]

def ecs_ci_service_dims(cluster_name: str, service_name: str):
    return [
        {"Name": "ClusterName", "Value": cluster_name},
        {"Name": "ServiceName", "Value": service_name},
    ]

# ---------- Task definition sizing ----------
def _to_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(str(val))
    except Exception:
        return None

def taskdef_cpu_mem(ecs, taskdef_arn: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    """
    מחזיר (task_cpu_units, task_memory_mb). אם לא מוגדר ברמת Task — מסכמים containerDefinitions.
    """
    if not taskdef_arn:
        return None, None
    try:
        td = ecs.describe_task_definition(taskDefinition=taskdef_arn).get("taskDefinition", {})
    except ClientError:
        return None, None

    cpu_i = _to_int(td.get("cpu"))
    mem_i = _to_int(td.get("memory"))

    if cpu_i is None or mem_i is None:
        total_cpu = 0
        total_mem = 0
        any_found = False
        for c in td.get("containerDefinitions", []):
            c_cpu = _to_int(c.get("cpu"))
            c_mem = _to_int(c.get("memory"))
            if c_cpu is not None:
                any_found = True
                total_cpu += c_cpu
            if c_mem is not None:
                any_found = True
                total_mem += c_mem
        if any_found:
            cpu_i = cpu_i if cpu_i is not None else (total_cpu or None)
            mem_i = mem_i if mem_i is not None else (total_mem or None)

    return cpu_i, mem_i

# ---------- Capacity Providers mix ----------
def capacity_provider_mix(cps: List[Dict]) -> Tuple[List[str], Optional[float]]:
    """
    מחזיר (שמות capacity providers, אחוז Spot משוקלל).
    משקל = base + weight. מזהים SPOT לפי שם.
    """
    if not cps:
        return [], None
    names: List[str] = []
    spot_w = 0
    total_w = 0
    for item in cps:
        name = item.get("capacityProvider")
        base = int(item.get("base") or 0)
        weight = int(item.get("weight") or 0)
        names.append(name)
        w = base + weight
        total_w += w
        if name and "SPOT" in name.upper():
            spot_w += w
    spot_pct = (spot_w * 100.0 / total_w) if total_w > 0 else None
    return names, spot_pct
