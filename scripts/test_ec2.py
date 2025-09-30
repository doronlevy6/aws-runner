#!/usr/bin/env python3
import boto3, os

session = boto3.Session(region_name=os.getenv("AWS_DEFAULT_REGION", "eu-west-1"))
ec2 = session.client("ec2")

resp = ec2.describe_instances(MaxResults=5)

instances = []
for r in resp.get("Reservations", []):
    for i in r.get("Instances", []):
        instances.append(i["InstanceId"])

print(f"Found {len(instances)} instances (showing up to 5):")
for iid in instances:
    print("-", iid)
