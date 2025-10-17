#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
EC2 Inventory – per account/region(s)
-------------------------------------
שולף אינבנטורי יעיל של מופעי EC2 ומייצא גם למסך וגם ל-CSV.

תכונות:
- עובד מול פרופיל/חשבון שתבחר (SSO/CLI).
- אזור בודד, רשימת אזורים, או "all" לפי ההרשאות (opted-in).
- שדות שימושיים ל-FinOps/אופרציה: InstanceId, Type, State, AZ, LaunchTime, Platform,
  VPC/Subnet, Private/Public IP, Name tag, Spot/On-Demand, EBS root volume (type/size/iops/throughput), SGs וכו'.
- מטפל בפאג'ינציה וב-AccessDenied (SCP) פר אזור בלי להפיל את כל הריצה.

תלויות:
  pip install boto3 python-dateutil

הרשאות מינימליות:
  ec2:DescribeInstances
  ec2:DescribeRegions
  ec2:DescribeVolumes
"""
import os
import sys
import csv
import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

import boto3
import botocore


def iso(ts) -> str:
    if not ts:
        return ""
    if isinstance(ts, str):
        return ts
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_active_regions(session: boto3.Session, explicit_regions: Optional[List[str]]) -> List[str]:
    """Return list of regions to scan."""
    if explicit_regions and explicit_regions != ["all"]:
        return explicit_regions

    ec2 = session.client("ec2", region_name=session.region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    regions = ec2.describe_regions(AllRegions=True)["Regions"]
    out = [r["RegionName"] for r in regions if r.get("OptInStatus") in ("opt-in-not-required", "opted-in")]
    # אם המשתמש נתן "all" – זו הרשימה; אם לא נתן כלום – נשאר עם region ברירת-מחדל בלבד
    if explicit_regions == ["all"]:
        return out
    if not explicit_regions:
        return [session.region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")]
    return out


def collect_root_volume_details(ec2, instances: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Map root EBS volumeId -> details (type/size/iops/throughput)."""
    vol_ids = []
    for i in instances:
        root_name = i.get("RootDeviceName")
        for bd in i.get("BlockDeviceMappings", []) or []:
            if bd.get("DeviceName") == root_name and "Ebs" in bd:
                vid = bd["Ebs"].get("VolumeId")
                if vid:
                    vol_ids.append(vid)
                break
    vol_ids = list({v for v in vol_ids if v})
    vol_info = {}
    for chunk_start in range(0, len(vol_ids), 200):
        chunk = vol_ids[chunk_start:chunk_start + 200]
        if not chunk:
            continue
        resp = ec2.describe_volumes(VolumeIds=chunk)
        for v in resp.get("Volumes", []):
            vol_info[v["VolumeId"]] = {
                "Type": v.get("VolumeType", ""),
                "SizeGiB": v.get("Size", ""),
                "Iops": v.get("Iops", ""),
                "Throughput": v.get("Throughput", ""),
            }
    return vol_info


def row_from_instance(acct: str, region: str, inst: Dict[str, Any], root_vol_details: Dict[str, Dict[str, Any]]) -> List[str]:
    state = (inst.get("State") or {}).get("Name", "")
    iid = inst.get("InstanceId", "")
    itype = inst.get("InstanceType", "")
    az = (inst.get("Placement") or {}).get("AvailabilityZone", "")
    launch = iso(inst.get("LaunchTime"))
    platform = inst.get("PlatformDetails", inst.get("Platform", "")) or "Linux/UNIX"
    vpc = inst.get("VpcId", "")
    subnet = inst.get("SubnetId", "")
    priv_ip = inst.get("PrivateIpAddress", "")
    pub_ip = inst.get("PublicIpAddress", "")
    lifecycle = inst.get("InstanceLifecycle", "OnDemand")
    tenancy = (inst.get("Placement") or {}).get("Tenancy", "")
    ebs_opt = str(inst.get("EbsOptimized", ""))

    # root volume details
    root_dev = inst.get("RootDeviceName", "")
    root_vol_id, root_type, root_size, root_iops, root_thr = "", "", "", "", ""
    for bd in inst.get("BlockDeviceMappings", []) or []:
        if bd.get("DeviceName") == root_dev and "Ebs" in bd:
            root_vol_id = bd["Ebs"].get("VolumeId", "")
            info = root_vol_details.get(root_vol_id, {})
            root_type = info.get("Type", "")
            root_size = str(info.get("SizeGiB", "")) if info.get("SizeGiB", "") != "" else ""
            root_iops = str(info.get("Iops", "")) if info.get("Iops", "") != "" else ""
            root_thr = str(info.get("Throughput", "")) if info.get("Throughput", "") != "" else ""
            break

    # tags
    name = ""
    tags = inst.get("Tags", []) or []
    for t in tags:
        if t.get("Key") == "Name":
            name = t.get("Value", "")
            break
    tag_str = ";".join([f"{t.get('Key')}={t.get('Value')}" for t in tags])

    # security groups
    sgs = ";".join([sg.get("GroupId", "") for sg in inst.get("SecurityGroups", []) or []])

    return [
        acct, region, iid, name, itype, state, az, launch, platform,
        lifecycle, tenancy, vpc, subnet, priv_ip, pub_ip, ebs_opt,
        root_vol_id, root_type, root_size, root_iops, root_thr, sgs, tag_str
    ]


def main():
    ap = argparse.ArgumentParser(description="EC2 Inventory exporter (per profile/account + regions)")
    ap.add_argument("--profile", help="AWS profile name (overrides environment)", default=os.getenv("AWS_PROFILE"))
    ap.add_argument("--regions", help='Comma-separated list (e.g. "eu-west-1,us-east-1") or "all". Default: current region.',
                    default=None)
    ap.add_argument("--outdir", help="Output directory", default="out")
    ap.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format (file will match).")
    args = ap.parse_args()

    # create session
    try:
        session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    except Exception as e:
        print(f"ERROR: cannot create AWS session (profile='{args.profile}'): {e}", file=sys.stderr)
        sys.exit(2)

    # identify caller
    try:
        sts = session.client("sts")
        ident = sts.get_caller_identity()
    except botocore.exceptions.NoCredentialsError:
        print("ERROR: No AWS credentials/session. Run: aws sso login --profile <name>", file=sys.stderr)
        sys.exit(2)

    account = ident.get("Account", "")
    caller = ident.get("Arn", "")

    # regions to scan
    regions_arg = None
    if args.regions:
        regions_arg = [r.strip() for r in args.regions.split(",") if r.strip()]
        if not regions_arg:
            regions_arg = None
    regions = get_active_regions(session, regions_arg)

    # collect
    headers = [
        "Account", "Region", "InstanceId", "Name", "InstanceType", "State", "AZ", "LaunchTime", "Platform",
        "Lifecycle", "Tenancy", "VpcId", "SubnetId", "PrivateIp", "PublicIp", "EbsOptimized",
        "RootVolumeId", "RootVolumeType", "RootSizeGiB", "RootIops", "RootThroughput",
        "SecurityGroupIds", "Tags"
    ]
    rows: List[List[str]] = []

    for region in regions:
        try:
            ec2 = session.client("ec2", region_name=region)
            paginator = ec2.get_paginator("describe_instances")
            all_instances = []
            for page in paginator.paginate():
                for res in page.get("Reservations", []) or []:
                    for inst in res.get("Instances", []) or []:
                        all_instances.append(inst)

            root_details = collect_root_volume_details(ec2, all_instances)
            for inst in all_instances:
                rows.append(row_from_instance(account, region, inst, root_details))
            print(f"# OK  | Account {account} | Region {region} | Instances {len(all_instances)}")
        except botocore.exceptions.ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            print(f"# ERR | Account {account} | Region {region} | {code}: {e}", file=sys.stderr)
            continue
        except Exception as e:
            print(f"# ERR | Account {account} | Region {region} | {type(e).__name__}: {e}", file=sys.stderr)
            continue

    # ensure outdir
    os.makedirs(args.outdir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    region_tag = "multi" if len(regions) > 1 else regions[0]
    profile_tag = args.profile or (session.profile_name or "default")
    outfile = os.path.join(args.outdir, f"ec2_inventory_{profile_tag}_{region_tag}_{ts}.{args.format}")

    # write file
    if args.format == "csv":
        with open(outfile, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(headers)
            w.writerows(rows)
    else:
        data = [dict(zip(headers, r)) for r in rows]
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    # print a short preview to stdout
    print("\n# SUMMARY")
    print(f"# Caller:  {caller}")
    print(f"# Account: {account}")
    print(f"# Regions: {', '.join(regions)}")
    print(f"# Total instances: {len(rows)}")
    print(f"# File written: {outfile}")


if __name__ == "__main__":
    main()
