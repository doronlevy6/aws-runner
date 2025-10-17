# scripts/common/aws_common.py  (פשוט ומינימלי)
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import sys
from typing import List, Tuple
import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$")

def session_for_profile(profile: str) -> boto3.session.Session:
    return boto3.Session(profile_name=profile)

def sts_whoami(session: boto3.session.Session) -> Tuple[str, str]:
    sts = session.client("sts", config=CFG)
    me = sts.get_caller_identity()
    return me["Account"], me["Arn"]

def parse_regions_arg(regions_arg: str) -> List[str]:
    """
    מקבל מחרוזת CSV של אזורים (למשל: 'us-east-1,eu-west-1')
    ומחזיר רשימה תקינה. אין תמיכה ב-'all'/'opted-in' — רק רשימה מפורשת.
    """
    if not regions_arg or not regions_arg.strip():
        raise ValueError("regions must be provided explicitly (e.g., --regions us-east-1,eu-west-1)")
    regions = [r.strip() for r in regions_arg.split(",") if r.strip()]
    bad = [r for r in regions if not _REGION_RE.match(r)]
    if bad:
        print(f"Invalid region name(s): {', '.join(bad)}", file=sys.stderr)
        raise ValueError("invalid region(s)")
    return regions

def rds_instances_exist_in_region(session: boto3.session.Session, region: str) -> bool:
    rds = session.client("rds", region_name=region, config=CFG)
    try:
        paginator = rds.get_paginator("describe_db_instances")
        for _page in paginator.paginate(PaginationConfig={"PageSize": 20}):
            if _page.get("DBInstances"):
                return True
        return False
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[{region}] skip ({code})", file=sys.stderr)
        return False
