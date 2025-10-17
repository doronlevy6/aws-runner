#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
from typing import List

_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$")

def parse_regions_arg(regions_arg: str) -> List[str]:
    """
    Parse a comma-separated list of regions (e.g. 'us-east-1,eu-west-1').
    Returns a validated list. No 'all'/'opted-in' here by design.
    """
    if not regions_arg or not regions_arg.strip():
        raise ValueError("regions must be provided (e.g., --regions us-east-1,eu-west-1)")
    regions = [r.strip() for r in regions_arg.split(",") if r.strip()]
    bad = [r for r in regions if not _REGION_RE.match(r)]
    if bad:
        raise ValueError(f"invalid region(s): {', '.join(bad)}")
    return regions
