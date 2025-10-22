#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EC2 Utilization Audit + Infra Complements (EBS / Snapshots / EIPs / NAT)
- Keeps original EC2 utilization outputs intact
- Adds additional CSVs for EBS volumes, snapshots, EIPs, NAT Gateways

Outputs under outputs/ec2_utilization_<ts>/:
  Core (existing):
    - ec2_<profile>.csv
    - ec2_all_profiles.csv
    - category_summary.csv
  New:
    - ebs_volumes.csv
    - snapshots.csv
    - eip_addresses.csv
    - eip_per_instance.csv
    - nat_gateways.csv
    - instance_state_summary.csv
"""

import argparse
import csv
import os
import datetime

import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple, Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError, ProfileNotFound

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

# ----------------- time & math helpers -----------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def iso(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()

def chunk_windows(start: datetime, end: datetime, max_days: int) -> List[Tuple[datetime, datetime]]:
    windows = []
    cur = start
    step = timedelta(days=max_days)
    while cur < end:
        nxt = min(cur + step, end)
        windows.append((cur, nxt))
        cur = nxt
    return windows

def mean(xs: List[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0

def p95(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    idx = int(round(0.95 * (len(ys) - 1)))
    return ys[idx]

# ----------------- categorization -----------------

FIELD_ORDER = [
    "account_id", "account_name", "region", "instance_id", "name", "type",
    "cpu_avg_pct", "cpu_p95_pct", "net_mb_per_day", "cpu_credit_balance", "category", "note"
]

def categorize(cpu_avg: float, cpu_p95_: float, net_mb_per_day: float) -> Tuple[str, str]:
    # Priority
    if cpu_avg >= 40.0 or cpu_p95_ >= 70.0:
        return ("High Utilization", "Consider scale-up/RI/SP alignment if persistent.")
    if cpu_avg < 10.0 and net_mb_per_day > (5 * 1024):
        return ("Network-Heavy", "CPU low but traffic high; check data transfer/placement.")
    if cpu_avg < 5.0 and net_mb_per_day < 200.0:
        return ("Idle / Over-Provisioned", "Very low CPU & traffic; candidate for stop/resize.")
    return ("Moderate", "Balanced utilization; monitor.")

# ----------------- AWS helpers -----------------

def session_for_profile(profile: str):
    try:
        return boto3.Session(profile_name=profile)
    except ProfileNotFound:
        print(f"[{profile}] AWS profile not found", file=sys.stderr)
        raise

def sts_whoami(sess) -> Tuple[str, str]:
    sts = sess.client("sts", config=CFG)
    ident = sts.get_caller_identity()
    return ident["Account"], ident["Arn"]

def list_regions(sess, regions_arg: str) -> List[str]:
    if regions_arg.lower() == "all":
        ec2 = sess.client("ec2", region_name="us-east-1", config=CFG)
        resp = ec2.describe_regions(AllRegions=False)
        return sorted([r["RegionName"] for r in resp.get("Regions", [])])
    return [r.strip() for r in regions_arg.split(",") if r.strip()]

# ---------- EC2 instances ----------

def list_instances_all_states(sess, region: str) -> Dict[str, Dict]:
    """
    Return mapping: instance_id -> {state, name, tags}
    States include: running, stopped, terminated, etc.
    """
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    mapping: Dict[str, Dict] = {}
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(PaginationConfig={"PageSize": 1000}):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                iid = inst["InstanceId"]
                st = inst.get("State", {}).get("Name", "")
                tags = {t["Key"]: t.get("Value", "") for t in inst.get("Tags", []) or []}
                mapping[iid] = {
                    "state": st,
                    "name": tags.get("Name", ""),
                    "tags": tags,
                }
    return mapping

def list_running_instances(sess, region: str) -> List[Dict]:
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    instances = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(
        Filters=[{"Name": "instance-state-name", "Values": ["running"]}],
        PaginationConfig={"PageSize": 1000}
    ):
        for res in page.get("Reservations", []):
            for inst in res.get("Instances", []):
                tags = {t["Key"]: t.get("Value", "") for t in inst.get("Tags", []) or []}
                instances.append({
                    "InstanceId": inst["InstanceId"],
                    "InstanceType": inst["InstanceType"],
                    "LaunchTime": inst.get("LaunchTime"),
                    "Tags": tags,
                    "Name": tags.get("Name", "")
                })
    return instances

def is_t_family(instance_type: str) -> bool:
    return instance_type.startswith(("t2.", "t3.", "t3a.", "t4g."))

# ---------- CloudWatch collectors (instances) ----------

def cw_get_metric_data(cw, queries: List[Dict], start: datetime, end: datetime):
    return cw.get_metric_data(MetricDataQueries=queries, StartTime=start, EndTime=end, ScanBy="TimestampAscending")

def fetch_cpu_points_5m(cw, instance_id: str, start: datetime, end: datetime) -> List[float]:
    points: List[float] = []
    for s, e in chunk_windows(start, end, max_days=5):
        resp = cw_get_metric_data(cw, [{
            "Id": "cpu",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUUtilization",
                    "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]
                },
                "Period": 300,
                "Stat": "Average"
            },
            "ReturnData": True
        }], s, e)
        for r in resp.get("MetricDataResults", []):
            points.extend(r.get("Values", []))
    return points

def fetch_network_daily_mb(cw, instance_id: str, start: datetime, end: datetime) -> float:
    resp = cw_get_metric_data(cw, [
        {"Id": "in_sum", "MetricStat": {"Metric": {"Namespace":"AWS/EC2","MetricName":"NetworkIn",
         "Dimensions":[{"Name":"InstanceId","Value":instance_id}]}, "Period":86400, "Stat":"Sum"}, "ReturnData": True},
        {"Id": "out_sum","MetricStat": {"Metric": {"Namespace":"AWS/EC2","MetricName":"NetworkOut",
         "Dimensions":[{"Name":"InstanceId","Value":instance_id}]}, "Period":86400, "Stat":"Sum"}, "ReturnData": True}
    ], start, end)
    in_vals = out_vals = []
    for r in resp.get("MetricDataResults", []):
        if r["Id"] == "in_sum": in_vals = r.get("Values", [])
        elif r["Id"] == "out_sum": out_vals = r.get("Values", [])
    days = min(len(in_vals), len(out_vals))
    if days == 0:
        return 0.0
    total_bytes = sum(in_vals[:days]) + sum(out_vals[:days])
    return (total_bytes / (1024 * 1024)) / days

def fetch_cpu_credit_min(cw, instance_id: str, start: datetime, end: datetime) -> Optional[float]:
    mins: List[float] = []
    for s, e in chunk_windows(start, end, max_days=5):
        resp = cw_get_metric_data(cw, [{
            "Id": "credit",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/EC2",
                    "MetricName": "CPUCreditBalance",
                    "Dimensions": [{"Name": "InstanceId", "Value": instance_id}]
                },
                "Period": 300,
                "Stat": "Minimum"
            },
            "ReturnData": True
        }], s, e)
        for r in resp.get("MetricDataResults", []):
            vals = r.get("Values", [])
            if vals:
                mins.append(min(vals))
    return min(mins) if mins else None

# ---------- EBS volumes ----------

def collect_ebs_volumes(sess, region: str, instances_map: Dict[str, Dict]) -> List[Dict]:
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    rows: List[Dict] = []
    paginator = ec2.get_paginator("describe_volumes")
    for page in paginator.paginate(PaginationConfig={"PageSize": 500}):
        for v in page.get("Volumes", []):
            vid = v["VolumeId"]
            vtype = v.get("VolumeType", "")
            size = v.get("Size", 0)  # GiB
            state = v.get("State", "")  # in-use / available
            attachments = v.get("Attachments", []) or []
            attached_instance_id = attachments[0].get("InstanceId") if attachments else ""
            device = attachments[0].get("Device") if attachments else ""
            attached_state = instances_map.get(attached_instance_id, {}).get("state", "") if attached_instance_id else ""
            name_tag = (instances_map.get(attached_instance_id, {}).get("name", "") if attached_instance_id else "")
            is_unattached = (state == "available")
            is_attached_to_stopped = (attached_state == "stopped")
            # simple flags for action hints (no pricing here)
            recommended = []
            if vtype == "gp2":
                recommended.append("convert:gp2->gp3")
            if is_unattached:
                recommended.append("delete:unattached")
            if is_attached_to_stopped:
                recommended.append("review:attached-to-stopped")
            rows.append({
                "region": region,
                "volume_id": vid,
                "size_gib": size,
                "type": vtype,
                "state": state,
                "attached_instance_id": attached_instance_id,
                "attached_instance_state": attached_state,
                "attached_instance_name": name_tag,
                "device": device,
                "is_unattached": is_unattached,
                "is_attached_to_stopped": is_attached_to_stopped,
                "recommended_action": ";".join(recommended) if recommended else "",
            })
    return rows

# ---------- Snapshots ----------

def collect_snapshots(sess, region: str, existing_volume_ids: set, older_than_days: int) -> List[Dict]:
    """
    EBS snapshots owned by self. Marks 'is_volume_present' if the source volume currently exists.
    """
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    rows: List[Dict] = []
    cutoff = utc_now() - timedelta(days=older_than_days)
    paginator = ec2.get_paginator("describe_snapshots")
    try:
        for page in paginator.paginate(OwnerIds=["self"], PaginationConfig={"PageSize": 1000}):
            for s in page.get("Snapshots", []):
                sid = s["SnapshotId"]
                vol_id = s.get("VolumeId", "")
                size = s.get("VolumeSize", 0)
                start_time = s.get("StartTime")
                days_old = None
                if start_time:
                    base = start_time if start_time.tzinfo else start_time.replace(tzinfo=timezone.utc)
                    days_old = int((utc_now() - base).days)
                is_old = False
                if start_time:
                    is_old = (start_time < cutoff)
                rows.append({
                    "region": region,
                    "snapshot_id": sid,
                    "volume_id": vol_id,
                    "volume_size_gib": size,
                    "start_time": iso(start_time),
                    "days_old": days_old if days_old is not None else "",
                    "state": s.get("State", ""),
                    "encrypted": s.get("Encrypted", ""),
                    "is_old_over_threshold": is_old,
                    "is_volume_present": (vol_id in existing_volume_ids) if vol_id else "",
                })
    except ClientError as e:
        print(f"[{region}] describe_snapshots failed: {e}", file=sys.stderr)
    return rows

# ---------- EIPs ----------

def collect_eips(sess, region: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Returns (addresses_rows, per_instance_rows)
    """
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    rows: List[Dict] = []
    per_instance_counts: Dict[str, int] = defaultdict(int)
    try:
        resp = ec2.describe_addresses()
    except ClientError as e:
        print(f"[{region}] describe_addresses failed: {e}", file=sys.stderr)
        return rows, []
    for a in resp.get("Addresses", []):
        alloc = a.get("AllocationId", "")
        assoc = a.get("AssociationId", "")
        inst = a.get("InstanceId", "")
        ni = a.get("NetworkInterfaceId", "")
        pub = a.get("PublicIp", "")
        priv = a.get("PrivateIpAddress", "")
        domain = a.get("Domain", "")  # vpc / standard
        is_attached = bool(assoc or inst or ni)
        rows.append({
            "region": region,
            "public_ip": pub,
            "allocation_id": alloc,
            "association_id": assoc,
            "instance_id": inst,
            "network_interface_id": ni,
            "private_ip": priv,
            "domain": domain,
            "is_attached": is_attached
        })
        if inst:
            per_instance_counts[inst] += 1
    per_inst_rows = [{"region": region, "instance_id": iid, "eip_count": cnt} for iid, cnt in per_instance_counts.items()]
    return rows, per_inst_rows

# ---------- NAT Gateways (+ CW metrics) ----------

def fetch_nat_metrics(cw, nat_id: str, start: datetime, end: datetime) -> Tuple[float, float]:
    """
    NAT Gateway metrics: sum(BytesOutToDestination) over window (GiB),
    and average ActiveConnectionCount.
    """
    resp = cw_get_metric_data(cw, [
        {
            "Id": "bytes_out",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/NATGateway",
                    "MetricName": "BytesOutToDestination",
                    "Dimensions": [{"Name": "NatGatewayId", "Value": nat_id}]
                },
                "Period": 86400,
                "Stat": "Sum"
            },
            "ReturnData": True
        },
        {
            "Id": "conns",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/NATGateway",
                    "MetricName": "ActiveConnectionCount",
                    "Dimensions": [{"Name": "NatGatewayId", "Value": nat_id}]
                },
                "Period": 3600,
                "Stat": "Average"
            },
            "ReturnData": True
        }
    ], start, end)
    bytes_out = 0.0
    conns_vals: List[float] = []
    for r in resp.get("MetricDataResults", []):
        if r["Id"] == "bytes_out":
            bytes_out = sum(r.get("Values", []) or [])
        elif r["Id"] == "conns":
            conns_vals = r.get("Values", []) or []
    gib = bytes_out / (1024.0 * 1024.0 * 1024.0)
    avg_conns = mean(conns_vals)
    return gib, avg_conns

def collect_nat_gateways(sess, region: str, start: datetime, end: datetime) -> List[Dict]:
    ec2 = sess.client("ec2", region_name=region, config=CFG)
    cw = sess.client("cloudwatch", region_name=region, config=CFG)
    rows: List[Dict] = []
    paginator = ec2.get_paginator("describe_nat_gateways")
    for page in paginator.paginate(PaginationConfig={"PageSize": 100}):
        for ngw in page.get("NatGateways", []):
            nat_id = ngw["NatGatewayId"]
            state = ngw.get("State", "")
            vpc = ngw.get("VpcId", "")
            subnet = (ngw.get("SubnetId", "") or
                      (ngw.get("SubnetIds", [])[0] if ngw.get("SubnetIds") else ""))
            try:
                bytes_out_gib, avg_conns = fetch_nat_metrics(cw, nat_id, start, end)
            except ClientError as e:
                print(f"[{region}/{nat_id}] NAT metrics error: {e}", file=sys.stderr)
                bytes_out_gib, avg_conns = 0.0, 0.0
            status = "Active" if (bytes_out_gib > 0 or avg_conns > 0) else "Idle"
            rows.append({
                "region": region,
                "nat_gateway_id": nat_id,
                "state": state,
                "vpc_id": vpc,
                "subnet_id": subnet,
                "bytes_out_window_gib": round(bytes_out_gib, 3),
                "avg_active_conns": round(avg_conns, 2),
                "status": status,
                "recommended_action": "remove_if_unused" if status == "Idle" else ""
            })
    return rows

# ---------- IO ----------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def write_csv(path: str, rows: List[Dict], field_order: List[str]):
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=field_order)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in field_order})

# ---------- CLI ----------

def parse_args():
    p = argparse.ArgumentParser(description="EC2 Utilization + EBS/Snapshots/EIPs/NAT (non-breaking extension)")
    p.add_argument("--profiles", nargs="+", required=True)
    p.add_argument("--regions", required=True)
    p.add_argument("--days", type=int, default=14, help="Instance CPU/network window (days)")
    p.add_argument("--nat-days", type=int, default=7, help="NAT metrics window (days)")
    p.add_argument("--snap-old-days", type=int, default=90, help="Threshold for old snapshots")
    p.add_argument("--outdir", default=None)

    # optional skips (defaults: collect)
    p.add_argument("--skip-ebs", action="store_true")
    p.add_argument("--skip-snapshots", action="store_true")
    p.add_argument("--skip-eips", action="store_true")
    p.add_argument("--skip-nat", action="store_true")
    return p.parse_args()

# ---------- MAIN ----------

def main():
    args = parse_args()
    end = utc_now()
    start = end - timedelta(days=args.days)
    nat_start = end - timedelta(days=args.nat_days)

    ts = end.strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"ec2_utilization_{ts}")
    ensure_dir(outdir)

    all_rows: List[Dict] = []
    cat_counter = Counter()

    # For new CSVs (aggregated across profiles/regions)
    ebs_rows_all: List[Dict] = []
    snap_rows_all: List[Dict] = []
    eip_rows_all: List[Dict] = []
    eip_per_inst_all: List[Dict] = []
    nat_rows_all: List[Dict] = []
    inst_state_summary: Counter = Counter()

    for profile in args.profiles:
        sess = session_for_profile(profile)
        account_id, _ = sts_whoami(sess)
        account_name = sess.profile_name
        regions = list_regions(sess, args.regions)
        profile_rows: List[Dict] = []

        for region in regions:
            cw = sess.client("cloudwatch", region_name=region, config=CFG)

            # ---------- existing EC2 utilization (running only) ----------
            try:
                running_instances = list_running_instances(sess, region)
            except ClientError as e:
                print(f"[{profile}/{region}] describe_instances (running) failed: {e}", file=sys.stderr)
                running_instances = []

            for inst in running_instances:
                iid = inst["InstanceId"]
                itype = inst["InstanceType"]
                name = inst.get("Name", "")

                # CPU
                cpu_points = []
                try:
                    cpu_points = fetch_cpu_points_5m(cw, iid, start, end)
                except ClientError as e:
                    print(f"[{profile}/{region}/{iid}] CPUUtilization error: {e}", file=sys.stderr)
                cpu_avg = mean(cpu_points)
                cpu_p95_ = p95(cpu_points)

                # Network
                net_mb_day = 0.0
                try:
                    net_mb_day = fetch_network_daily_mb(cw, iid, start, end)
                except ClientError as e:
                    print(f"[{profile}/{region}/{iid}] NetworkIn/Out error: {e}", file=sys.stderr)

                # Credits
                credit_min = None
                if is_t_family(itype):
                    try:
                        credit_min = fetch_cpu_credit_min(cw, iid, start, end)
                    except ClientError as e:
                        print(f"[{profile}/{region}/{iid}] CPUCreditBalance error: {e}", file=sys.stderr)

                category, note = categorize(cpu_avg, cpu_p95_, net_mb_day)
                row = {
                    "account_id": account_id,
                    "account_name": account_name,
                    "region": region,
                    "instance_id": iid,
                    "name": name,
                    "type": itype,
                    "cpu_avg_pct": round(cpu_avg, 2),
                    "cpu_p95_pct": round(cpu_p95_, 2),
                    "net_mb_per_day": round(net_mb_day, 2),
                    "cpu_credit_balance": "" if credit_min is None else round(credit_min, 2),
                    "category": category,
                    "note": note
                }
                profile_rows.append(row)
                all_rows.append(row)
                cat_counter[category] += 1

            # ---------- NEW: infra complements ----------
            # build instance state map once per region to support EBS/EIP summaries
            try:
                inst_map = list_instances_all_states(sess, region)
            except ClientError as e:
                print(f"[{profile}/{region}] describe_instances (all states) failed: {e}", file=sys.stderr)
                inst_map = {}

            # state summary
            for iid, meta in inst_map.items():
                inst_state_summary[meta.get("state","unknown")] += 1

            # EBS volumes
            if not args.skip_ebs:
                try:
                    vol_rows = collect_ebs_volumes(sess, region, inst_map)
                    # decorate account info
                    for r in vol_rows:
                        r.update({"account_id": account_id, "account_name": account_name})
                    ebs_rows_all.extend(vol_rows)
                except ClientError as e:
                    print(f"[{profile}/{region}] describe_volumes failed: {e}", file=sys.stderr)

            # Snapshots (needs existing volume IDs for 'is_volume_present')
            if not args.skip_snapshots:
                existing_vol_ids = {r["volume_id"] for r in ebs_rows_all if r.get("volume_id")}
                try:
                    snap_rows = collect_snapshots(sess, region, existing_vol_ids, args.snap_old_days)
                    for r in snap_rows:
                        r.update({"account_id": account_id, "account_name": account_name})
                    snap_rows_all.extend(snap_rows)
                except ClientError as e:
                    print(f"[{profile}/{region}] describe_snapshots failed: {e}", file=sys.stderr)

            # EIPs
            if not args.skip_eips:
                addrs, per_inst = collect_eips(sess, region)
                for r in addrs:
                    r.update({"account_id": account_id, "account_name": account_name})
                for r in per_inst:
                    r.update({"account_id": account_id, "account_name": account_name})
                eip_rows_all.extend(addrs)
                eip_per_inst_all.extend(per_inst)

            # NAT Gateways
            if not args.skip_nat:
                try:
                    nat_rows = collect_nat_gateways(sess, region, nat_start, end)
                    for r in nat_rows:
                        r.update({"account_id": account_id, "account_name": account_name})
                    nat_rows_all.extend(nat_rows)
                except ClientError as e:
                    print(f"[{profile}/{region}] NAT collection failed: {e}", file=sys.stderr)

        # write per-profile CSV (existing)
        write_csv(os.path.join(outdir, f"ec2_{profile}.csv"), profile_rows, FIELD_ORDER)

    # write merged (existing)
    write_csv(os.path.join(outdir, "ec2_all_profiles.csv"), all_rows, FIELD_ORDER)
    write_csv(os.path.join(outdir, "category_summary.csv"),
              [{"category": k, "count": v} for k, v in sorted(cat_counter.items())],
              ["category", "count"])

    # write new CSVs
    if ebs_rows_all:
        write_csv(os.path.join(outdir, "ebs_volumes.csv"), ebs_rows_all, [
            "account_id","account_name","region","volume_id","size_gib","type","state",
            "attached_instance_id","attached_instance_state","attached_instance_name","device",
            "is_unattached","is_attached_to_stopped","recommended_action"
        ])
    if snap_rows_all:
        write_csv(os.path.join(outdir, "snapshots.csv"), snap_rows_all, [
            "account_id","account_name","region","snapshot_id","volume_id","volume_size_gib",
            "start_time","days_old","state","encrypted","is_old_over_threshold","is_volume_present"
        ])
    if eip_rows_all:
        write_csv(os.path.join(outdir, "eip_addresses.csv"), eip_rows_all, [
            "account_id","account_name","region","public_ip","allocation_id","association_id",
            "instance_id","network_interface_id","private_ip","domain","is_attached"
        ])
    if eip_per_inst_all:
        write_csv(os.path.join(outdir, "eip_per_instance.csv"), eip_per_inst_all, [
            "account_id","account_name","region","instance_id","eip_count"
        ])
    if nat_rows_all:
        write_csv(os.path.join(outdir, "nat_gateways.csv"), nat_rows_all, [
            "account_id","account_name","region","nat_gateway_id","state","vpc_id","subnet_id",
            "bytes_out_window_gib","avg_active_conns","status","recommended_action"
        ])
    if inst_state_summary:
        write_csv(os.path.join(outdir, "instance_state_summary.csv"),
                  [{"state": k, "count": v} for k, v in sorted(inst_state_summary.items())],
                  ["state","count"])

    print(f"✅ Done. Outputs at: {outdir}", file=sys.stderr)
    return 0

if __name__ == "__main__":
    sys.exit(main())
