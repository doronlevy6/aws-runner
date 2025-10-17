#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

def rds_instances_exist_in_region(session, region: str) -> bool:
    """
    Return True if the region contains at least one RDS DB instance.
    Safe to call with read-only permissions.
    """
    rds = session.client("rds", region_name=region, config=CFG)
    try:
        paginator = rds.get_paginator("describe_db_instances")
        for page in paginator.paginate(PaginationConfig={"PageSize": 20}):
            if page.get("DBInstances"):
                return True
        return False
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        print(f"[{region}] skip ({code})", file=sys.stderr)
        return False
