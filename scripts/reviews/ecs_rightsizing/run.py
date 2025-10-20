#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ECS Rightsizing Collector — Extended (DATA ONLY, FinOps-ready)
אוסף נתונים מקיפים לזיהוי חוסר פעילות וריסייזינג אפשרי ב-ECS.
כולל:
 - Utilization (CPU/Memory) ברמת Service (ContainerInsights ואם לא קיים — AWS/ECS fallback)
 - Desired/Running/Pending
 - גיל ממוצע של Tasks + ספירת קונטיינרים
 - סטטוס AutoScaling
 - סימוני-עזר (signals_*) לזיהוי חיסכון אפשרי: ריסייזינג CPU/Memory, עודף רפליקות, Non-Prod, Spot candidate
 - רמות Cluster (fallback ל-AWS/ECS)
אין חישובי עלויות/המלצות — נתונים וסימנים בלבד.
"""

import argparse
import os
import sys
import re
from typing import List, Dict, Optional
from datetime import datetime, timezone

from botocore.exceptions import ClientError, ProfileNotFound
from botocore.config import Config as BotoConfig

# מודולים משותפים — זהה לסגנון RDS
from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.cloudwatch import get_metric_series, summarize, window
from scripts.common.csvio import write_csv
from scripts.common.ecs import (
    ecs_clusters_exist_in_region,
    list_clusters_arns,
    list_services_arns,
    describe_services_safe,
    cluster_name_from_arn,
    ecs_cluster_dim,
    ecs_ci_service_dims,
    taskdef_cpu_mem,
    capacity_provider_mix,
)

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
CI_NS = "ECS/ContainerInsights"  # Service/Task deep metrics (דורש CI)
ECS_NS = "AWS/ECS"               # Cluster/Service בסיסי (זמין גם בלי CI)

# ---------- CloudWatch ----------
def safe_series(cw, namespace, metric, dimensions, start, end, period, stat="Average"):
    try:
        return get_metric_series(cw, namespace, metric, dimensions, start, end, period, stat=stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric:{metric}] skip ({code})", file=sys.stderr)
        return []
    except Exception as e:
        print(f"    [metric:{metric}] skip (generic: {e})", file=sys.stderr)
        return []

def min_period_for_days(days: int) -> int:
    total_seconds = days * 86400
    raw = (total_seconds + 1440 - 1) // 1440
    return ((raw + 59) // 60) * 60 if raw % 60 != 0 else raw

def effective_period(days: int, requested: int) -> int:
    return max(requested, min_period_for_days(days))

# ---------- Metrics helpers ----------
def summarize_ci_metric(cw, cluster_name: str, service_name: str, metric: str, start, end, period: int, stat="Average"):
    dims = ecs_ci_service_dims(cluster_name, service_name)
    series = safe_series(cw, CI_NS, metric, dims, start, end, period, stat=stat)
    avg, p95, p99 = summarize(series)
    return avg, p95, p99

def summarize_ecs_service_metric(cw, cluster_name: str, service_name: str, metric: str, start, end, period: int, stat="Average"):
    """Fallback: Service-level metrics מתוך AWS/ECS (זמין ללא CI)"""
    dims = [
        {"Name": "ClusterName", "Value": cluster_name},
        {"Name": "ServiceName", "Value": service_name},
    ]
    series = safe_series(cw, ECS_NS, metric, dims, start, end, period, stat=stat)
    avg, p95, p99 = summarize(series)
    return avg, p95, p99

def cluster_level_utilization(cw, cluster_name: str, start, end, period: int) -> Dict[str, Optional[float]]:
    """Cluster-level מתוך AWS/ECS (זמין תמיד)"""
    out: Dict[str, Optional[float]] = {}
    dims = ecs_cluster_dim(cluster_name)
    cpu_util, _, _ = summarize(safe_series(cw, ECS_NS, "CPUUtilization", dims, start, end, period))
    cpu_resv, _, _ = summarize(safe_series(cw, ECS_NS, "CPUReservation", dims, start, end, period))
    mem_util, _, _ = summarize(safe_series(cw, ECS_NS, "MemoryUtilization", dims, start, end, period))
    mem_resv, _, _ = summarize(safe_series(cw, ECS_NS, "MemoryReservation", dims, start, end, period))
    out["cluster_cpu_util_avg"] = cpu_util
    out["cluster_cpu_resv_avg"] = cpu_resv
    out["cluster_mem_util_avg"] = mem_util
    out["cluster_mem_resv_avg"] = mem_resv
    return out

# ---------- Utility ----------
NONPROD_RE = re.compile(r"(dev|stage|staging|test|qa|sandbox|non[-_]?prod)", re.IGNORECASE)

def is_nonprod_name(cluster_name: str, service_name: str) -> bool:
    target = f"{cluster_name}/{service_name}"
    return bool(NONPROD_RE.search(target))

# ---------- Collect per service ----------
def collect_service_row(ecs, cw, region: str, cluster_arn: str, svc: Dict, start, end, period: int) -> Dict:
    cluster_name = cluster_name_from_arn(cluster_arn)
    service_name = svc.get("serviceName")

    # --- בסיסי Service ---
    launch_type = (svc.get("launchType") or "").upper()      # EC2/FARGATE או ריק (כש-CP בשימוש)
    desired = svc.get("desiredCount", 0)
    running = svc.get("runningCount", 0)
    pending = svc.get("pendingCount", 0)
    taskdef_arn = svc.get("taskDefinition")
    has_lb = bool(svc.get("loadBalancers"))

    # פריסות/בריאות
    deployment_count = len(svc.get("deployments", []) or [])
    min_healthy = svc.get("deploymentConfiguration", {}).get("minimumHealthyPercent")
    max_percent = svc.get("deploymentConfiguration", {}).get("maximumPercent")
    circuit_breaker = svc.get("deploymentCircuitBreaker", {}).get("enable", False)
    platform_version = svc.get("platformVersion")

    # Capacity providers
    cap_mix, spot_pct = capacity_provider_mix(svc.get("capacityProviderStrategy", []))

    # Task definition size (vCPU units -> vCPU)
    td_cpu_units, td_mem_mb = taskdef_cpu_mem(ecs, taskdef_arn)
    td_vcpu = (td_cpu_units / 1024.0) if td_cpu_units else None
    total_vcpu_alloc = (td_vcpu * desired) if (td_vcpu is not None) else None
    total_mem_mb_alloc = (td_mem_mb * desired) if td_mem_mb is not None else None

    # --- Metrics: ניסיון CI קודם, ואז Fallback ל-AWS/ECS ---
    cpu_avg = cpu_p95 = mem_avg = mem_p95 = None
    net_rx_avg = net_tx_avg = None
    try:
        cpu_avg, cpu_p95, _ = summarize_ci_metric(cw, cluster_name, service_name, "CPUUtilization", start, end, period)
        mem_avg, mem_p95, _ = summarize_ci_metric(cw, cluster_name, service_name, "MemoryUtilization", start, end, period)
        # Network דורש CI — אם אין CI, נתונים אלה יישארו None
        net_rx_avg, _, _ = summarize_ci_metric(cw, cluster_name, service_name, "NetworkRxBytes", start, end, period)
        net_tx_avg, _, _ = summarize_ci_metric(cw, cluster_name, service_name, "NetworkTxBytes", start, end, period)
    except Exception:
        pass

    # Fallback לשירותים ללא CI — CPU/Memory מ-AWS/ECS (כמו בקונסולה)
    if cpu_avg is None:
        try:
            cpu_avg, cpu_p95, _ = summarize_ecs_service_metric(cw, cluster_name, service_name, "CPUUtilization", start, end, period)
        except Exception:
            pass
    if mem_avg is None:
        try:
            mem_avg, mem_p95, _ = summarize_ecs_service_metric(cw, cluster_name, service_name, "MemoryUtilization", start, end, period)
        except Exception:
            pass

    # --- Task-level: גיל ממוצע + ספירת קונטיינרים + Inactive ---
    inactive_tasks = 0
    task_ages = []
    container_count = 0
    try:
        tasks_arns = ecs.list_tasks(cluster=cluster_arn, serviceName=service_name).get("taskArns", [])
        if tasks_arns:
            task_desc = ecs.describe_tasks(cluster=cluster_arn, tasks=tasks_arns).get("tasks", [])
            for t in task_desc:
                if t.get("lastStatus") != "RUNNING":
                    inactive_tasks += 1
                started = t.get("startedAt")
                if started:
                    task_ages.append((datetime.now(timezone.utc) - started).total_seconds() / 3600)
                container_count += len(t.get("containers", []))
    except Exception:
        pass
    avg_task_age_hrs = round(sum(task_ages) / len(task_ages), 2) if task_ages else None

    # --- Auto Scaling policies (Target tracking) ---
    autoscaling_enabled = False
    try:
        appautoscale = ecs.meta.client.session.client("application-autoscaling", region_name=region)
        scalable_targets = appautoscale.describe_scalable_targets(
            ServiceNamespace="ecs",
            ResourceIds=[f"service/{cluster_name}/{service_name}"]
        ).get("ScalableTargets", [])
        autoscaling_enabled = bool(scalable_targets)
    except Exception:
        pass

    # --- FinOps helper signals (נתונים בינאריים/ספים פשוטים) ---
    # הערכים ניתנים לכיול בהמשך; כאן זה רק סיגנלים
    signal_low_cpu = (cpu_avg is not None and cpu_avg < 30.0)          # ריסייזינג CPU אפשרי
    signal_low_mem = (mem_avg is not None and mem_avg < 40.0)          # ריסייזינג Memory אפשרי
    signal_over_replicas = (desired and desired > 1 and cpu_avg is not None and mem_avg is not None
                            and cpu_avg < 20.0 and mem_avg < 30.0)     # לשקול הורדת Desired/הפעלת AS
    signal_no_autoscaling = (not autoscaling_enabled and desired and desired > 1)
    signal_nonprod = is_nonprod_name(cluster_name, service_name)
    # Spot candidate: Fargate ללא Spot, וסביבה לא-פרוד (היגיון שמרני)
    uses_spot = (spot_pct is not None and spot_pct > 0)
    signal_spot_candidate = (launch_type == "FARGATE" and not uses_spot and signal_nonprod)

    return {
        # זיהוי
        "region": region,
        "cluster_name": cluster_name,
        "service_name": service_name,
        "launch_type": launch_type,
        "capacity_providers": ",".join(cap_mix) if cap_mix else None,
        "capacity_provider_spot_pct": spot_pct,

        # Desired/Running/Pending
        "desired_count": desired,
        "running_count": running,
        "pending_count": pending,

        # TaskDef הקצאה לכל טסק + סה"כ מוקצה (לפי Desired)
        "task_definition": taskdef_arn,
        "taskdef_cpu_units": td_cpu_units,
        "taskdef_vcpu": td_vcpu,
        "taskdef_memory_mb": td_mem_mb,
        "total_alloc_vcpu": total_vcpu_alloc,
        "total_alloc_memory_mb": total_mem_mb_alloc,

        # Deploy/Health
        "deployment_count": deployment_count,
        "min_healthy_percent": min_healthy,
        "maximum_percent": max_percent,
        "has_load_balancer": has_lb,
        "platform_version": platform_version,
        "circuit_breaker_enabled": circuit_breaker,

        # Metrics (Service-level)
        "cpu_util_avg_pct": cpu_avg,
        "cpu_util_p95_pct": cpu_p95,
        "mem_util_avg_pct": mem_avg,
        "mem_util_p95_pct": mem_p95,
        "net_rx_bytes_avg_per_period": net_rx_avg,  # יתמלא רק כש-CI פעיל
        "net_tx_bytes_avg_per_period": net_tx_avg,  # יתמלא רק כש-CI פעיל

        # Tasks details
        "inactive_tasks": inactive_tasks,
        "avg_task_age_hours": avg_task_age_hrs,
        "container_count_total": container_count,

        # AutoScaling
        "autoscaling_enabled": autoscaling_enabled,

        # Cluster-level (ימולא ב-collect_profile אם אין Service metrics)
        "cluster_cpu_util_avg": None,
        "cluster_cpu_resv_avg": None,
        "cluster_mem_util_avg": None,
        "cluster_mem_resv_avg": None,

        # FinOps helper signals
        "signals_low_cpu": signal_low_cpu,
        "signals_low_mem": signal_low_mem,
        "signals_over_replicas": signal_over_replicas,
        "signals_no_autoscaling": signal_no_autoscaling,
        "signals_nonprod_name": signal_nonprod,
        "signals_spot_candidate": signal_spot_candidate,
    }

# ---------- Collect per profile ----------
def collect_profile(profile: str, regions: List[str], days: int, period: int) -> List[Dict]:
    rows: List[Dict] = []
    sess = session_for_profile(profile)
    acct_id, _ = sts_whoami(sess)
    start, end = window(days)

    for region in regions:
        ecs = sess.client("ecs", region_name=region, config=CFG)
        cw  = sess.client("cloudwatch", region_name=region, config=CFG)

        try:
            cluster_arns = list_clusters_arns(ecs)
            if not cluster_arns:
                print(f"[{profile}/{region}] (no ECS clusters)", file=sys.stderr)
                continue

            for cl_arn in cluster_arns:
                cluster_name = cluster_name_from_arn(cl_arn)
                svc_arns = list_services_arns(ecs, cl_arn)

                if not svc_arns:
                    # שורת Cluster גם כשאין Services
                    cl_util = cluster_level_utilization(cw, cluster_name, start, end, period)
                    rows.append({
                        "region": region,
                        "cluster_name": cluster_name,
                        "service_name": None,
                        "cluster_cpu_util_avg": cl_util.get("cluster_cpu_util_avg"),
                        "cluster_cpu_resv_avg": cl_util.get("cluster_cpu_resv_avg"),
                        "cluster_mem_util_avg": cl_util.get("cluster_mem_util_avg"),
                        "cluster_mem_resv_avg": cl_util.get("cluster_mem_resv_avg"),
                        "profile": profile,
                        "account_id": acct_id,
                    })
                    continue

                svc_desc = describe_services_safe(ecs, cl_arn, svc_arns)
                cl_util: Optional[Dict[str, float]] = None

                for svc in svc_desc:
                    row = collect_service_row(ecs, cw, region, cl_arn, svc, start, end, period)

                    # אם לא קיבלנו Utilization ברמת Service (גם אחרי fallback) — נצרף Cluster-level פעם אחת
                    if (row["cpu_util_avg_pct"] is None and row["mem_util_avg_pct"] is None) and cl_util is None:
                        cl_util = cluster_level_utilization(cw, cluster_name, start, end, period)
                    if cl_util is not None and row["cpu_util_avg_pct"] is None and row["mem_util_avg_pct"] is None:
                        row["cluster_cpu_util_avg"] = cl_util.get("cluster_cpu_util_avg")
                        row["cluster_cpu_resv_avg"] = cl_util.get("cluster_cpu_resv_avg")
                        row["cluster_mem_util_avg"] = cl_util.get("cluster_mem_util_avg")
                        row["cluster_mem_resv_avg"] = cl_util.get("cluster_mem_resv_avg")

                    row["profile"] = profile
                    row["account_id"] = acct_id
                    rows.append(row)

        except ClientError as e:
            print(f"[{profile}/{region}] skipping due to error: {e}", file=sys.stderr)
            continue

    return rows

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="ECS Rightsizing Collector — Extended (DATA ONLY, FinOps-ready)")
    p.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g., steam-fi tea-fi)")
    p.add_argument("--regions", required=True, help="Comma-separated regions, e.g. 'us-east-1,eu-west-1'")
    p.add_argument("--days", type=int, default=14, help="Lookback window in days (default 14)")
    p.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (>=60; default 300)")
    p.add_argument("--outdir", default=None, help="Output dir (default: outputs/ecs_rightsize_<timestamp>)")
    return p.parse_args()

def main():
    args = parse_args()

    try:
        regions_all = parse_regions_arg(args.regions)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    eff_period = effective_period(args.days, args.period)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"ecs_rightsize_{ts}")
    os.makedirs(outdir, exist_ok=True)

    all_rows: List[Dict] = []

    print("== ECS Rightsizing Collector — Extended (DATA ONLY, FinOps-ready) ==", file=sys.stderr)
    print(f"  regions: {', '.join(regions_all)}", file=sys.stderr)
    print(f"  days={args.days}, period={eff_period}s", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

    for prof in args.profiles:
        print(f"\n[profile: {prof}]", file=sys.stderr)
        try:
            sess = session_for_profile(prof)
        except ProfileNotFound:
            print(f"  ! profile '{prof}' not found in ~/.aws/config", file=sys.stderr)
            continue

        try:
            acct, arn = sts_whoami(sess)
            print(f"  account: {acct}", file=sys.stderr)
            print(f"  caller : {arn}", file=sys.stderr)
        except ClientError as e:
            print(f"  ! STS failed: {e}", file=sys.stderr)
            continue

        active_regions = [r for r in regions_all if ecs_clusters_exist_in_region(sess, r)]
        if not active_regions:
            print("  (no ECS clusters in selected regions)", file=sys.stderr)
            continue

        rows = collect_profile(prof, active_regions, args.days, eff_period)
        if rows:
            all_rows.extend(rows)
            write_csv(os.path.join(outdir, f"ecs_{prof}.csv"), rows, rows[0].keys())
            print(f"  -> wrote {len(rows)} rows to {os.path.join(outdir, f'ecs_{prof}.csv')}", file=sys.stderr)
        else:
            print("  -> no data collected for this profile.", file=sys.stderr)

    if all_rows:
        write_csv(os.path.join(outdir, "ecs_all_profiles.csv"), all_rows, all_rows[0].keys())
        print(f"\nALL DONE -> {os.path.join(outdir, 'ecs_all_profiles.csv')}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())
