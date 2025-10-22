#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run:

פרופיל יחיד
scripts/rr_cloudfront_config.sh \
  --profiles tea-fi \
  --regions us-east-1



CloudFront Distributions Config Review
Collects per-distribution behaviors:
- TTL (min/default/max)
- CachePolicy vs legacy ForwardedValues
- Query strings & Cookies forwarding
- Origin access (OAI/OAC) + origin type/domain
- Viewer Protocol Policy
"""

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import List, Dict

from botocore.exceptions import ProfileNotFound
from botocore.config import Config as BotoConfig

from scripts.common.aws_common import session_for_profile, sts_whoami
from scripts.common.csvio import ensure_dir, write_csv
from scripts.common.cloudfront import (
    list_all_distributions,
    get_distribution_config,
    origin_oai_oac_flags,
    analyze_behavior,
)

CFG = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})

def parse_args():
    p = argparse.ArgumentParser(description="CloudFront Distributions Config Review")
    p.add_argument("--profiles", nargs="+", required=True)
    p.add_argument("--regions", default="global", help="Not used; CloudFront API is global")
    p.add_argument("--outdir", default=None)
    return p.parse_args()

def collect_for_profile(profile: str) -> List[Dict]:
    sess = session_for_profile(profile)
    acct, _ = sts_whoami(sess)

    rows: List[Dict] = []
    dists = list_all_distributions(sess)
    cache_policies: Dict[str, Dict] = {}

    for dist in dists:
        dist_id = dist["Id"]
        domain = dist.get("DomainName")

        try:
            cfg = get_distribution_config(sess, dist_id)
        except Exception as e:
            print(f"[{profile}/{dist_id}] get_distribution_config error: {e}", file=sys.stderr)
            continue

        origins = {o["Id"]: o for o in (cfg.get("Origins", {}).get("Items") or [])}

        def emit_row(path_label: str, behavior: Dict):
            origin_id = behavior.get("TargetOriginId")
            origin = origins.get(origin_id, {})
            has_oai, has_oac, origin_type = origin_oai_oac_flags(origin)
            origin_domain = origin.get("DomainName")
            viewer_policy = behavior.get("ViewerProtocolPolicy")

            b = analyze_behavior(sess, behavior, cache_policies)

            rows.append({
                "profile": profile,
                "account_id": acct,
                "distribution_id": dist_id,
                "distribution_domain": domain,

                "behavior_path": path_label,
                "origin_id": origin_id,
                "origin_domain": origin_domain,
                "origin_type": origin_type,
                "origin_has_oai": has_oai,
                "origin_has_oac": has_oac,

                "cache_policy_mode": b.get("cache_policy_mode"),
                "cache_policy_id": b.get("cache_policy_id"),
                "min_ttl": b.get("min_ttl"),
                "default_ttl": b.get("default_ttl"),
                "max_ttl": b.get("max_ttl"),
                "query_behavior": b.get("query_behavior"),
                "cookies_behavior": b.get("cookies_behavior"),
                "viewer_protocol_policy": viewer_policy,
            })

        # default behavior
        emit_row("Default", cfg.get("DefaultCacheBehavior") or {})
        # additional cache behaviors
        for cb in (cfg.get("CacheBehaviors", {}).get("Items") or []):
            emit_row(cb.get("PathPattern", "(unknown)"), cb)

    return rows

def main():
    args = parse_args()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    outdir = args.outdir or os.path.join("outputs", f"cloudfront_distributions_config_{ts}")
    ensure_dir(outdir)

    all_rows: List[Dict] = []
    print("== CloudFront Distributions Config Review ==", file=sys.stderr)
    print(f"  outdir: {outdir}", file=sys.stderr)

    for prof in args.profiles:
        try:
            prof_rows = collect_for_profile(prof)
        except ProfileNotFound:
            print(f"[!] profile {prof} not found", file=sys.stderr)
            continue
        if prof_rows:
            write_csv(os.path.join(outdir, f"cloudfront_config_{prof}.csv"), prof_rows, prof_rows[0].keys())
            all_rows.extend(prof_rows)

    if all_rows:
        write_csv(os.path.join(outdir, "cloudfront_distributions_config_all_profiles.csv"), all_rows, all_rows[0].keys())
        print(f"\nALL DONE -> {os.path.join(outdir, 'cloudfront_distributions_config_all_profiles.csv')}", file=sys.stderr)
    else:
        print("\nNo data collected.", file=sys.stderr)

    return 0

if __name__ == "__main__":
    sys.exit(main())
