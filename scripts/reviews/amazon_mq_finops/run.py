#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
הרצה ללא נוד
scripts/rr_mq.sh us-east-1 tea-fi

הרצה עם נוד 
scripts/run_review.sh amazon_mq_finops --regions "us-east-1" --profiles tea-fi --days 14 --period 300 --per-node


Amazon MQ FinOps Scan — DATA ONLY
אוסף מטא-דאטה של ברוקרים, מטריקות שימוש X ימים, סטטוס לוגים/באקאפ/Flow Logs,
ומחשב דגלים להחלטות חיסכון. מפיק:
  - mq_finops_scan.csv (לפי ברוקר)
  - mq_finops_readiness.csv (תנאי-קדם פר חשבון/אזור)

אופציונלי:
  - --per-node -> mq_nodes_<profile>.csv עם CPU/Mem/Network לכל Node ב-Cluster
"""

import argparse
import os
import sys
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timezone
from collections import defaultdict

from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.regions import parse_regions_arg
from scripts.common.csvio import write_csv
from scripts.common.cloudwatch import get_metric_series, summarize, window
from scripts.common.mq import (
    list_brokers, describe_broker, find_mq_log_group,
    backup_recovery_points, any_flow_logs_enabled
)

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
CW_NS = "AWS/AmazonMQ"

# ---------------------- Helpers ---------------------- #
def safe_series(cw, metric: str, dimensions: List[Dict[str, str]], start, end, period: int, stat="Average"):
    try:
        return get_metric_series(cw, CW_NS, metric, dimensions, start, end, period, stat=stat)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        print(f"    [metric:{metric}] skip ({code})", file=sys.stderr)
        return []

def effective_period(days: int, requested: int) -> int:
    total_seconds = days * 86400
    raw = (total_seconds + 1440 - 1) // 1440
    if raw % 60 != 0:
        raw = ((raw + 59) // 60) * 60
    return max(requested, raw)

def pick_cpu_metric(engine_type: str) -> str:
    et = (engine_type or "").lower()
    return "SystemCpuUtilization" if "rabbit" in et else "CpuUtilization"

def pick_conn_metric(engine_type: str) -> str:
    et = (engine_type or "").lower()
    return "ConnectionCount" if "rabbit" in et else "CurrentConnectionsCount"

def message_activity_metric_pair(engine_type: str) -> Tuple[Optional[str], Optional[str]]:
    et = (engine_type or "").lower()
    return ("MessageCount", "MessageReadyCount") if "rabbit" in et else ("EnqueueCount", "DequeueCount")

def publish_consume_metrics(engine_type: str) -> Tuple[Optional[str], Optional[str]]:
    et = (engine_type or "").lower()
    return ("PublishRate", "AckRate") if "rabbit" in et else (None, None)

def discover_dims_for_metric(cw_client, metric_name: str, broker_id: str, broker_name: Optional[str]) -> List[Dict[str, str]]:
    """
    מגלה סט Dimensions תקף *למטריקה הספציפית* ברמת Broker.
    מנסה קודם ‘RecentlyActive’ לצמצום, ובודק גם BrokerId וגם Broker.
    """
    paginator = cw_client.get_paginator("list_metrics")

    def scan(**kwargs) -> List[Dict[str, str]]:
        for page in paginator.paginate(Namespace=CW_NS, MetricName=metric_name, **kwargs):
            for m in page.get("Metrics", []) or []:
                dims = m.get("Dimensions", []) or []
                if broker_id and any(d.get("Name") == "BrokerId" and d.get("Value") == broker_id for d in dims):
                    return dims
                if broker_name and any(d.get("Name") == "Broker" and d.get("Value") == broker_name for d in dims):
                    return dims
        return []

    dims = scan(RecentlyActive='PT3H')
    return dims or scan()

def get_stat_with_fallback(cw, metric: str, dims: List[Dict[str, str]], start, end, period: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    # נסה Average; אם אין סדרה, נסה Maximum (חלק מה-MQ metrics יוצאות כסמפלים בודדים)
    s_avg = safe_series(cw, metric, dims, start, end, period, stat="Average")
    a, p95, mx = summarize(s_avg)
    if a is not None or mx is not None:
        return a, p95, mx
    s_max = safe_series(cw, metric, dims, start, end, period, stat="Maximum")
    a2, p95_2, mx2 = summarize(s_max)
    return a2, p95_2, mx2

def compute_flags(avg_cpu: Optional[float], avg_conn: Optional[float], msg_signal: Optional[float],
                  host_type: Optional[str], deployment_mode: Optional[str],
                  logs_retention_days: Optional[int],
                  logs_enabled: bool,
                  backup_count: int,
                  flow_logs_enabled: bool) -> Dict[str, bool]:
    cpu = avg_cpu if avg_cpu is not None else 0.0
    conn = avg_conn if avg_conn is not None else 0.0
    msg  = msg_signal if msg_signal is not None else 0.0
    host = (host_type or "").lower()

    flag_idle_candidate = (cpu < 5.0 and conn < 1.0 and abs(msg) < 1e-6)
    large_host = (".xlarge" in host) or (".2xlarge" in host) or (".4xlarge" in host) or ("m5." in host) or ("m6g." in host) or ("m6i." in host)
    flag_overprovisioned_candidate = (cpu < 15.0 and large_host)

    flag_single_az_attention = (deployment_mode or "").lower().startswith("single")
    r = logs_retention_days or 0
    flag_logs_retention_long = (logs_enabled and (r == 0 or r > 30))
    flag_no_logs_detected = (not logs_enabled)
    flag_no_backup_detected = (backup_count == 0)
    flag_no_flowlogs_detected = (not flow_logs_enabled)

    return dict(
        flag_idle_candidate=flag_idle_candidate,
        flag_overprovisioned_candidate=flag_overprovisioned_candidate,
        flag_single_az_attention=flag_single_az_attention,
        flag_logs_retention_long=flag_logs_retention_long,
        flag_no_logs_detected=flag_no_logs_detected,
        flag_no_backup_detected=flag_no_backup_detected,
        flag_no_flowlogs_detected=flag_no_flowlogs_detected,
    )

def recommend_action(flags: Dict[str, bool], logs_enabled: bool=False) -> str:
    recs: List[str] = []
    if flags.get("flag_idle_candidate"):
        recs.append("Stop/Downsize broker")
    if flags.get("flag_overprovisioned_candidate"):
        recs.append("Downsize instance type")
    if logs_enabled and flags.get("flag_logs_retention_long"):
        recs.append("Reduce CW Logs retention to 14–30d")
    if flags.get("flag_no_backup_detected"):
        recs.append("Configure AWS Backup or verify DR plan")
    if flags.get("flag_single_az_attention"):
        recs.append("Assess HA vs cost")
    if flags.get("flag_no_flowlogs_detected"):
        recs.append("Consider enabling VPC Flow Logs (optional)")
    return "; ".join(recs) if recs else ""

# ---------------------- Per-Node helpers ---------------------- #
def list_node_dims(cw_client, metric_name: str, broker_id: str, broker_name: Optional[str]) -> List[List[Dict[str, str]]]:
    """
    מאתר את כל ה-Nodes של הברוקר עבור metric נתון (Dimensions כוללים Node).
    מחזיר רשימה של סטי Dimensions (כל סט מייצג Node אחר).
    """
    paginator = cw_client.get_paginator("list_metrics")
    nodes: Dict[str, List[Dict[str, str]]] = {}

    def collect(recent=False):
        kwargs = dict(Namespace=CW_NS, MetricName=metric_name)
        if recent:
            kwargs["RecentlyActive"] = 'PT3H'
        for page in paginator.paginate(**kwargs):
            for m in page.get("Metrics", []) or []:
                dims = m.get("Dimensions", []) or []
                ok_broker = (
                    (broker_id and any(d.get("Name") == "BrokerId" and d.get("Value") == broker_id for d in dims)) or
                    (broker_name and any(d.get("Name") == "Broker" and d.get("Value") == broker_name for d in dims))
                )
                node_val = next((d.get("Value") for d in dims if d.get("Name") == "Node"), None)
                if ok_broker and node_val:
                    nodes[node_val] = dims  # union by node name

    # קודם פעילים מאוד, ואז מלא — מאחדים
    collect(recent=True)
    collect(recent=False)

    return list(nodes.values())

def node_metric_avg(cw, metric: str, dims: List[Dict[str, str]], start, end, period: int) -> Optional[float]:
    a, _, mx = get_stat_with_fallback(cw, metric, dims, start, end, period)
    return a if (a is not None) else mx

def collect_nodes(cw, broker_id: str, broker_name: Optional[str], start, end, effp: int) -> Tuple[List[Dict], Dict[str, float]]:
    """
    מחזיר:
      - rows: רשומות פר-Node עם CPU/Mem/NetIn/NetOut
      - agg : אגרגציה לרמת Broker (ממוצע וסטיית תקן ל-CPU)
    """
    rows: List[Dict] = []
    cpu_nodes = list_node_dims(cw, "SystemCpuUtilization", broker_id, broker_name)
    if not cpu_nodes:
        return rows, {}

    # ננסה סט דימנשנים תואם גם למדדים הנוספים
    mem_map = {tuple(sorted((d["Name"], d["Value"]) for d in dims)): dims
               for dims in list_node_dims(cw, "RabbitMQMemUsed", broker_id, broker_name)}
    netin_map = {tuple(sorted((d["Name"], d["Value"]) for d in dims)): dims
                 for dims in list_node_dims(cw, "NetworkIn", broker_id, broker_name)}
    netout_map = {tuple(sorted((d["Name"], d["Value"]) for d in dims)): dims
                  for dims in list_node_dims(cw, "NetworkOut", broker_id, broker_name)}

    cpu_vals: List[float] = []
    for dims in cpu_nodes:
        node_name = next((d.get("Value") for d in dims if d.get("Name") == "Node"), None)

        cpu_avg = node_metric_avg(cw, "SystemCpuUtilization", dims, start, end, effp)
        key = tuple(sorted((d["Name"], d["Value"]) for d in dims))
        mem_dims = mem_map.get(key) or dims
        in_dims  = netin_map.get(key) or dims
        out_dims = netout_map.get(key) or dims

        mem_avg = node_metric_avg(cw, "RabbitMQMemUsed", mem_dims, start, end, effp)
        net_in  = node_metric_avg(cw, "NetworkIn", in_dims, start, end, effp)
        net_out = node_metric_avg(cw, "NetworkOut", out_dims, start, end, effp)

        if cpu_avg is not None:
            cpu_vals.append(cpu_avg)

        rows.append(dict(
            node=node_name,
            cpu_avg_pct=cpu_avg,
            rabbitmq_mem_used_avg=mem_avg,
            network_in_avg_bps=net_in,
            network_out_avg_bps=net_out,
        ))

    agg: Dict[str, float] = {}
    if cpu_vals:
        n = float(len(cpu_vals))
        mean = sum(cpu_vals) / n
        var = sum((v - mean) ** 2 for v in cpu_vals) / n
        agg = dict(node_count=len(cpu_vals), cpu_mean=mean, cpu_stddev=var ** 0.5)

    return rows, agg

# ---------------------- Collector (Broker-level) ---------------------- #
def collect_profile(sess, profile: str, acct_id: str, regions: List[str], days: int, period: int, want_per_node: bool) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    scan_rows: List[Dict] = []
    readiness_rows: List[Dict] = []
    nodes_rows_all: List[Dict] = []
    start, end = window(days)

    for region in regions:
        cw   = sess.client("cloudwatch", region_name=region, config=CFG)
        logs = sess.client("logs",       region_name=region, config=CFG)

        # Readiness probes (coarse)
        cloudwatch_ok = True
        logs_ok = True
        backup_ok = True
        ce_ok = True
        notes: List[str] = []

        try:
            cw.list_metrics(Namespace=CW_NS)
        except ClientError as e:
            cloudwatch_ok = False
            notes.append(f"CW:{e.response.get('Error', {}).get('Code')}")
        try:
            logs.describe_log_groups(logGroupNamePrefix="/aws/amazonmq", limit=1)
        except ClientError as e:
            logs_ok = False
            notes.append(f"LOGS:{e.response.get('Error', {}).get('Code')}")
        try:
            bkp = sess.client("backup", region_name=region, config=CFG)
            bkp.list_backup_vaults(MaxResults=1)
        except ClientError as e:
            backup_ok = False
            notes.append(f"BKP:{e.response.get('Error', {}).get('Code')}")

        readiness_rows.append(dict(
            account_id=acct_id, region=region,
            cloudwatch_access_ok=cloudwatch_ok,
            logs_access_ok=logs_ok,
            backup_access_ok=backup_ok,
            ce_access_ok=ce_ok,
            notes=";".join(notes) if notes else ""
        ))

        flowlogs_enabled = any_flow_logs_enabled(sess, region)

        try:
            brokers = list_brokers(sess, region)
        except ClientError as e:
            print(f"[{profile}/{region}] list_brokers error: {e}", file=sys.stderr)
            continue

        for br in brokers:
            broker_id = br.get("BrokerId")
            broker_name = br.get("BrokerName")
            d = describe_broker(sess, region, broker_id) or {}

            engine_type = d.get("EngineType") or br.get("EngineType")
            engine_version = d.get("EngineVersion") or br.get("EngineVersion")
            instance_type = d.get("HostInstanceType") or br.get("HostInstanceType")
            deploy_mode = d.get("DeploymentMode") or br.get("DeploymentMode")
            state = d.get("BrokerState") or br.get("BrokerState")
            auto_minor = bool(d.get("AutoMinorVersionUpgrade"))
            broker_arn = d.get("BrokerArn") or br.get("BrokerArn")

            created_time = None
            if d.get("Created"):
                try:
                    created_time = d["Created"].replace(microsecond=0).isoformat()
                except Exception:
                    pass
            maint_start = d.get("MaintenanceWindowStartTime")
            data_replication_mode = d.get("DataReplicationMode")
            publicly_accessible = d.get("PubliclyAccessible")

            # Logs group
            lg_name, lg_retention, lg_enabled = find_mq_log_group(sess, region, broker_id or "", broker_name)

            effp = effective_period(days, period)

            # --- Metrics (per-broker) --- #
            cpu_metric = pick_cpu_metric(engine_type)
            conn_metric = pick_conn_metric(engine_type)
            m1, m2 = message_activity_metric_pair(engine_type)
            pub_metric, ack_metric = publish_consume_metrics(engine_type)

            avg_cpu = max_cpu = avg_conn = None
            msg_signal = None
            msg_count_avg = msg_ready_avg = None
            publish_rate_avg = ack_rate_avg = None

            # CPU
            cpu_dims = discover_dims_for_metric(cw, cpu_metric, broker_id or "", broker_name)
            if cpu_dims:
                a_cpu, p95_cpu, mx_cpu = get_stat_with_fallback(cw, cpu_metric, cpu_dims, start, end, effp)
                avg_cpu, max_cpu = a_cpu, mx_cpu

            # Connections
            conn_dims = discover_dims_for_metric(cw, conn_metric, broker_id or "", broker_name)
            if not conn_dims and cpu_dims:
                conn_dims = cpu_dims[:]
            if conn_dims:
                a_conn, _, _ = get_stat_with_fallback(cw, conn_metric, conn_dims, start, end, effp)
                avg_conn = a_conn

            # Message activity aggregates
            val1 = val2 = 0.0
            if m1:
                m1_dims = discover_dims_for_metric(cw, m1, broker_id or "", broker_name)
                if not m1_dims and cpu_dims:
                    m1_dims = cpu_dims[:]
                if m1_dims:
                    v1, _, _ = get_stat_with_fallback(cw, m1, m1_dims, start, end, effp)
                    msg_count_avg = v1
                    val1 = v1 or 0.0

            if m2:
                m2_dims = discover_dims_for_metric(cw, m2, broker_id or "", broker_name)
                if not m2_dims and cpu_dims:
                    m2_dims = cpu_dims[:]
                if m2_dims:
                    v2, _, _ = get_stat_with_fallback(cw, m2, m2_dims, start, end, effp)
                    msg_ready_avg = v2
                    val2 = v2 or 0.0

            msg_signal = (val1 + val2) if (val1 or val2) else 0.0

            # Publish / Ack rates
            if pub_metric:
                pub_dims = discover_dims_for_metric(cw, pub_metric, broker_id or "", broker_name)
                if not pub_dims and cpu_dims:
                    pub_dims = cpu_dims[:]
                if pub_dims:
                    v, _, _ = get_stat_with_fallback(cw, pub_metric, pub_dims, start, end, effp)
                    publish_rate_avg = v

            if ack_metric:
                ack_dims = discover_dims_for_metric(cw, ack_metric, broker_id or "", broker_name)
                if not ack_dims and cpu_dims:
                    ack_dims = cpu_dims[:]
                if ack_dims:
                    v, _, _ = get_stat_with_fallback(cw, ack_metric, ack_dims, start, end, effp)
                    ack_rate_avg = v

            # Backup counts
            bkp_count, bkp_latest = (0, None)
            if broker_arn:
                bkp_count, bkp_latest = backup_recovery_points(sess, region, broker_arn)

            flags = compute_flags(avg_cpu, avg_conn, msg_signal, instance_type, deploy_mode,
                                  lg_retention, bool(lg_name), bkp_count, flowlogs_enabled)
            rec = recommend_action(flags, logs_enabled=bool(lg_name))

            row = dict(
                account_id=acct_id,
                region=region,
                broker_arn=broker_arn,
                broker_id=broker_id,
                broker_name=broker_name,
                engine_type=engine_type,
                engine_version=engine_version,
                host_instance_type=instance_type,
                deployment_mode=deploy_mode,
                broker_state=state,
                auto_minor_version_upgrade=auto_minor,

                avg_cpu_Xd=avg_cpu,
                max_cpu_Xd=max_cpu,
                avg_connections_Xd=avg_conn,
                msg_activity_Xd=msg_signal,
                msg_count_avg=msg_count_avg,
                msg_ready_avg=msg_ready_avg,
                publish_rate_avg=publish_rate_avg,
                ack_rate_avg=ack_rate_avg,

                logs_group_name=lg_name,
                logs_retention_days=lg_retention,
                backup_recovery_points_count=bkp_count,
                backup_last_recovery_point_time=bkp_latest,
                flow_logs_enabled=flowlogs_enabled,

                flag_idle_candidate=flags["flag_idle_candidate"],
                flag_overprovisioned_candidate=flags["flag_overprovisioned_candidate"],
                flag_single_az_attention=flags["flag_single_az_attention"],
                flag_logs_retention_long=flags["flag_logs_retention_long"],
                flag_no_logs_detected=flags["flag_no_logs_detected"],
                flag_no_backup_detected=flags["flag_no_backup_detected"],
                flag_no_flowlogs_detected=flags["flag_no_flowlogs_detected"],

                recommended_action=rec,
                created_time=created_time,
                maintenance_window_start_time=str(maint_start) if maint_start else None,
                data_replication_mode=data_replication_mode,
                publicly_accessible=publicly_accessible,
            )
            scan_rows.append(row)

            # --- Per-node (optional) --- #
            if want_per_node:
                node_rows, node_agg = collect_nodes(cw, broker_id or "", broker_name, start, end, effp)
                for r in node_rows:
                    r["region"] = region
                    r["broker_id"] = broker_id
                    r["broker_name"] = broker_name
                nodes_rows_all.extend(node_rows)

    return scan_rows, readiness_rows, nodes_rows_all

# ---------------------- CLI & Main ---------------------- #
def parse_args():
    p = argparse.ArgumentParser(description="Amazon MQ FinOps Scan — DATA ONLY")
    p.add_argument("--profiles", nargs="+", required=True, help="AWS CLI profiles (e.g., steam-fi tea-fi)")
    p.add_argument("--regions", required=True, help="Comma-separated regions, e.g. 'us-east-1,eu-west-1'")
    p.add_argument("--days", type=int, default=14, help="Lookback window in days (default 14)")
    p.add_argument("--period", type=int, default=300, help="CloudWatch period seconds (>=60; default 300)")
    p.add_argument("--outdir", default=None, help="Output dir (default: outputs/amazon_mq_finops_<timestamp>)")
    p.add_argument("--per-node", action="store_true", help="Collect per-node metrics and write mq_nodes_*.csv")
    return p.parse_args()

def main():
    args = parse_args()
    try:
        regions = parse_regions_arg(args.regions)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 2

    eff_period = effective_period(args.days, args.period)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"amazon_mq_finops_{ts}")
    os.makedirs(outdir, exist_ok=True)

    print("== Amazon MQ FinOps Scan — DATA ONLY ==", file=sys.stderr)
    print(f"  regions: {', '.join(regions)}", file=sys.stderr)
    print(f"  days={args.days}, period={eff_period}s", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

    all_rows: List[Dict] = []
    all_ready: List[Dict] = []
    all_nodes: List[Dict] = []

    for prof in args.profiles:
        print(f"\n[profile: {prof}]", file=sys.stderr)
        try:
            sess = session_for_profile(prof)
        except ProfileNotFound:
            print(f"  ! profile '{prof}' not found in ~/.aws/config", file=sys.stderr)
            continue

        try:
            acct_id, arn = sts_whoami(sess)
            print(f"  account: {acct_id}", file=sys.stderr)
            print(f"  caller : {arn}", file=sys.stderr)
        except ClientError as e:
            print(f"  ! STS failed: {e}", file=sys.stderr)
            continue

        rows, ready, nodes_rows = collect_profile(sess, prof, acct_id, regions, args.days, eff_period, args.per_node)
        if rows:
            all_rows.extend(rows)
            field_order = [
                "account_id","region","broker_arn","broker_id","broker_name",
                "engine_type","engine_version","host_instance_type","deployment_mode","broker_state","auto_minor_version_upgrade",
                "avg_cpu_Xd","max_cpu_Xd","avg_connections_Xd","msg_activity_Xd",
                "msg_count_avg","msg_ready_avg","publish_rate_avg","ack_rate_avg",
                "logs_group_name","logs_retention_days","backup_recovery_points_count","backup_last_recovery_point_time","flow_logs_enabled",
                "flag_idle_candidate","flag_overprovisioned_candidate","flag_single_az_attention","flag_logs_retention_long","flag_no_logs_detected","flag_no_backup_detected","flag_no_flowlogs_detected",
                "recommended_action",
                "created_time","maintenance_window_start_time","data_replication_mode","publicly_accessible",
            ]
            write_csv(os.path.join(outdir, f"mq_{prof}.csv"), rows, field_order)
            print(f"  -> wrote {len(rows)} rows to {os.path.join(outdir, f'mq_{prof}.csv')}", file=sys.stderr)
        else:
            print("  -> no brokers found / no data.", file=sys.stderr)

        if ready:
            all_ready.extend(ready)
        if nodes_rows:
            all_nodes.extend(nodes_rows)

    if all_rows:
        field_order = [
            "account_id","region","broker_arn","broker_id","broker_name",
            "engine_type","engine_version","host_instance_type","deployment_mode","broker_state","auto_minor_version_upgrade",
            "avg_cpu_Xd","max_cpu_Xd","avg_connections_Xd","msg_activity_Xd",
            "msg_count_avg","msg_ready_avg","publish_rate_avg","ack_rate_avg",
            "logs_group_name","logs_retention_days","backup_recovery_points_count","backup_last_recovery_point_time","flow_logs_enabled",
            "flag_idle_candidate","flag_overprovisioned_candidate","flag_single_az_attention","flag_logs_retention_long","flag_no_logs_detected","flag_no_backup_detected","flag_no_flowlogs_detected",
            "recommended_action",
            "created_time","maintenance_window_start_time","data_replication_mode","publicly_accessible",
        ]
        write_csv(os.path.join(outdir, "mq_finops_scan.csv"), all_rows, field_order)
        print(f"\nALL DONE -> {os.path.join(outdir, 'mq_finops_scan.csv')}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)

    if all_ready:
        ro = ["account_id","region","cloudwatch_access_ok","logs_access_ok","backup_access_ok","ce_access_ok","notes"]
        write_csv(os.path.join(outdir, "mq_finops_readiness.csv"), all_ready, ro)

    if args.per_node and all_nodes:
        node_fields = ["region","broker_id","broker_name","node","cpu_avg_pct","rabbitmq_mem_used_avg","network_in_avg_bps","network_out_avg_bps"]
        write_csv(os.path.join(outdir, "mq_nodes_all_profiles.csv"), all_nodes, node_fields)
        print(f"  -> wrote per-node {len(all_nodes)} rows to {os.path.join(outdir, 'mq_nodes_all_profiles.csv')}", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())
