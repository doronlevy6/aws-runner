#!/usr/bin/env python3
import boto3, os

profile = os.getenv("AWS_PROFILE")
session = boto3.Session(profile_name=profile) if profile else boto3.Session()
ec2 = session.client("ec2")
regions = ec2.describe_regions(AllRegions=True)["Regions"]

for r in regions:
    if r["OptInStatus"] in ("opt-in-not-required", "opted-in"):
        print(r["RegionName"])
