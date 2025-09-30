# scripts/scan_selected_regions.py
#!/usr/bin/env python3
import os, boto3, json

session = boto3.Session()

# הדפס חשבון פעיל כדי לוודא שאנחנו סופרים את מה שציפית
sts = session.client("sts", region_name=os.getenv("AWS_DEFAULT_REGION","eu-west-1"))
who = sts.get_caller_identity()
print("ACTIVE ACCOUNT:", json.dumps(who, indent=2))

regions = ["eu-west-1", "eu-central-1", "il-central-1", "us-east-1"]

print("\nEC2 instance count by region:")
for r in regions:
    try:
        ec2 = session.client("ec2", region_name=r)
        paginator = ec2.get_paginator("describe_instances")
        count = 0
        for page in paginator.paginate():
            for res in page.get("Reservations", []):
                count += len(res.get("Instances", []))
        print(f"{r:12} -> {count}")
    except Exception as e:
        print(f"{r:12} -> error: {e.__class__.__name__}")
