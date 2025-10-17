#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
List EC2 instances in the CURRENT region (AWS_DEFAULT_REGION or profile's region).
Prints: InstanceId, InstanceType, State, AZ, Name.
"""
import os, sys, json, boto3, botocore

def main():
    profile = os.getenv("AWS_PROFILE")
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    region = session.region_name or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

    sts = session.client("sts")
    try:
        ident = sts.get_caller_identity()
    except botocore.exceptions.NoCredentialsError:
        print("ERROR: No credentials. Run: aws sso login --profile <name>", file=sys.stderr)
        sys.exit(2)

    acct = ident["Account"]; arn = ident["Arn"]
    ec2 = session.client("ec2", region_name=region)

    rows = []
    try:
        for page in ec2.get_paginator("describe_instances").paginate():
            for r in page.get("Reservations", []):
                for i in r.get("Instances", []):
                    state = (i.get("State") or {}).get("Name")
                    itype = i.get("InstanceType")
                    iid   = i.get("InstanceId")
                    az    = (i.get("Placement") or {}).get("AvailabilityZone")
                    name  = next((t.get("Value","") for t in (i.get("Tags") or []) if t.get("Key")=="Name"), "")
                    rows.append([iid, itype, state, az, name])
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("AccessDenied", "UnauthorizedOperation"):
            print("ERROR: Access denied to ec2:DescribeInstances (SCP/permissions).", file=sys.stderr)
            sys.exit(3)
        raise

    print(f"# Account: {acct} | Region: {region} | Caller: {arn}")
    print("InstanceId,InstanceType,State,AZ,Name")
    for row in rows:
        print(",".join(x or "" for x in row))
    print(f"# Total: {len(rows)}")

if __name__ == "__main__":
    main()
