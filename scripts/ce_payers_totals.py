#!/usr/bin/env python3
# =====================================================================
# File: scripts/ce_payers_totals.py
#
# What it does:
#   Iterates over a configured list of payer/management profiles.
#   For each payer, writes rows in unified schema (final order):
#       account_id,account_name,total_unblended_cost,kind
#   where:
#     - kind = ""   -> usage (excludes AWS Marketplace, Tax, SPP; see FILTER_MODE)
#     - kind = "mp" -> AWS Marketplace only (Tax excluded in ui/full)
#
# New:
#   SPLIT_RULES lets you carve-out specific linked accounts from a payer.
#   For those targets we write their own rows (usage/mp), and also write
#   the rest-of-org rows excluding those targets (usage/mp).
#
# FILTER_MODE:
#   - "none": Usage = not MP; MP = MP (gross, includes Tax).
#   - "ui":   Usage = not MP and not {Tax, SPP}; MP = MP and not {Tax}.
#   - "full": Usage = not MP and not {Refund,Credit,Discount,Tax};
#             MP    = MP and not {Refund,Credit,Discount,Tax}.
#
# Run:
#   python scripts/ce_payers_totals.py
#
# Output:
#   out/ce_payers_totals_<filter_mode>_<timestamp>.csv
# =====================================================================

import os, csv, datetime
from decimal import Decimal
import boto3
from botocore.config import Config

# ---------------- CONFIG ----------------
START_DAY, START_MONTH, START_YEAR = 1, 9, 2025
END_DAY,   END_MONTH,   END_YEAR   = 30, 9, 2025
FILTER_MODE = "ui"  # "ui" | "none" | "full"

# Not directly used in current filters, kept for reference
SERVICE_EXCLUDE_LIST = [
    "Solution Provider Program Discount",
    "Savings Plans",
    "Tax",
]

PAYER_PROFILES = [
    "altahq-from-abra","biz-from-abra","bot-authority","cobra-mgt",
    "aidome","coinmarket","communi","genoox","imagry","jobtestprep","nanox",
    "onriva","owlduet","partake","senseip","storeman","three-spring-media",
    "vsee","wallter","yes-management","zoozpower",
]

# =====================================================================
# SPECIAL PER-PAYER HANDLING (ENGLISH COMMENTS)
#
# Use SPLIT_RULES when a payer requires a different treatment.
# For each payer key:
#   - by:      "name" (substring, case-insensitive) or "id"
#   - targets: list of linked accounts to carve out (names or IDs)
#   - rest_name_suffix: optional suffix for the payer's "rest-of-org" name
#
# Example: For "cobra-mgt", carve out the linked account whose name
# contains "cobra-sap-prod", write separate rows (usage/mp) for it,
# and then write "rest-of-org" rows (usage/mp) for the remaining accounts.
# =====================================================================
SPLIT_RULES = {
    # SPECIAL: cobra-mgt -> carve-out cobra-sap-prod
    "cobra-mgt": {
        "by": "name",
        "targets": ["cobra-sap-prod"],   # add more if needed
        "rest_name_suffix": ""           # e.g. " (excluding cobra-sap-prod)"
    },
}

METRIC, GRANULARITY = "UnblendedCost", "MONTHLY"
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = f"out/ce_payers_totals_{FILTER_MODE}_{ts}.csv"
# ----------------------------------------

def iso_date(y,m,d):
    import datetime as _dt
    return _dt.date(y,m,d).isoformat()

def build_filters(mode: str):
    """
    Returns (usage_filter, mp_filter)
    Usage  = everything except AWS Marketplace and (optionally) Tax/Discounts
    MP     = BILLING_ENTITY='AWS Marketplace' and (optionally) no Tax/Refund/Credit/Discount
    """
    mp_base = {"Dimensions": {"Key": "BILLING_ENTITY", "Values": ["AWS Marketplace"]}}

    if mode == "none":
        usage = {"Not": mp_base}
        mp    = mp_base  # gross including Tax
        return usage, mp

    if mode == "ui":
        usage_parts = [
            {"Not": mp_base},
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Tax", "Solution Provider Program Discount"]}}},
        ]
        mp_parts = [
            mp_base,
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Tax"]}}},
        ]
        return {"And": usage_parts}, {"And": mp_parts}

    if mode == "full":
        usage_parts = [
            {"Not": mp_base},
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Refund","Credit","Discount","Tax"]}}},
        ]
        mp_parts = [
            mp_base,
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Refund","Credit","Discount","Tax"]}}},
        ]
        return {"And": usage_parts}, {"And": mp_parts}

    return None, mp_base

def clients_for_profile(profile: str):
    region = os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
    sess = boto3.Session(profile_name=profile, region_name=region)
    cfg = Config(retries={"max_attempts":5,"mode":"standard"})
    return sess.client("ce", config=cfg), sess.client("sts", config=cfg), sess.client("organizations", config=cfg)

def get_total_for_period(ce, start_iso, end_iso, ce_filter):
    kw={"TimePeriod":{"Start":start_iso,"End":end_iso},"Granularity":GRANULARITY,"Metrics":[METRIC]}
    if ce_filter: kw["Filter"]=ce_filter
    total=Decimal("0")
    resp=ce.get_cost_and_usage(**kw)

    def grab(b): return Decimal(b.get("Total",{}).get(METRIC,{}).get("Amount","0"))
    for b in resp.get("ResultsByTime",[]): total += grab(b)
    while resp.get("NextPageToken"):
        kw["NextPageToken"]=resp["NextPageToken"]; resp=ce.get_cost_and_usage(**kw)
        for b in resp.get("ResultsByTime",[]): total += grab(b)
    return total

def account_name(org, account_id: str, fallback: str):
    try:
        return org.describe_account(AccountId=account_id)["Account"]["Name"]
    except Exception:
        return fallback

# ---------- Helpers for split logic (English comments) ----------
def _AND(*parts):
    # Compose a single AND filter from multiple parts, skipping falsy items
    return {"And": [p for p in parts if p]}

def _filter_linked(ids):
    # Filter for the given set of LINKED_ACCOUNT ids
    return {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [str(x) for x in ids]}}

def resolve_split_targets(org, rule: dict):
    """
    Resolve the target linked accounts for a split rule.
    - If by == "id": take the IDs as-is (and fetch names from Organizations).
    - If by == "name": treat each target as a case-insensitive substring match
      over account names from Organizations.
    """
    mode = rule.get("by", "name")
    wants = rule.get("targets", [])
    accounts = []
    try:
        for page in org.get_paginator("list_accounts").paginate():
            accounts.extend(page.get("Accounts", []))
    except Exception:
        pass

    results = []
    if mode == "id":
        for tid in wants:
            nm = next((a["Name"] for a in accounts if a.get("Id")==tid), tid)
            results.append((tid, nm))
    else:  # name (substring, case-insensitive)
        low = [w.lower() for w in wants]
        for a in accounts:
            aid, nm = a.get("Id",""), a.get("Name","")
            if any(fragment in nm.lower() for fragment in low):
                results.append((aid, nm))

    # Dedupe & validate
    seen, uniq = set(), []
    for aid,nm in results:
        if aid and aid not in seen:
            uniq.append((aid,nm)); seen.add(aid)
    if not uniq:
        raise SystemExit(f"[ERR] split targets not found for rule: {rule}")
    return uniq

def main():
    import datetime as _dt
    start_iso = iso_date(START_YEAR,START_MONTH,START_DAY)
    end_iso   = (_dt.date(END_YEAR,END_MONTH,END_DAY) + _dt.timedelta(days=1)).isoformat()
    usage_filter, mp_filter = build_filters(FILTER_MODE)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV,"w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh)
        w.writerow(["account_id","account_name","total_unblended_cost","kind"])

        for profile in PAYER_PROFILES:
            ce, sts, org = clients_for_profile(profile)
            acct_id = sts.get_caller_identity().get("Account", profile)
            name    = account_name(org, acct_id, profile)

            # =================================================================
            # SPECIAL HANDLING ENTRY POINT:
            # If this payer appears in SPLIT_RULES, we:
            #   1) Write per-target carve-out rows (usage/mp) for each linked acct.
            #   2) Write "rest-of-org" rows (usage/mp) excluding those targets.
            # Otherwise, we write the default 1-2 rows for the payer as a whole.
            # =================================================================
            if profile in SPLIT_RULES:
                rule = SPLIT_RULES[profile]
                targets = resolve_split_targets(org, rule)   # [(linked_id, linked_name), ...]
                target_ids = [tid for tid,_ in targets]

                # ---- (1) Carve-outs: each target as its own "account" ----
                for tid, tname in targets:
                    u = get_total_for_period(ce, start_iso, end_iso, _AND(usage_filter, _filter_linked([tid])))
                    m = get_total_for_period(ce, start_iso, end_iso, _AND(mp_filter,    _filter_linked([tid])))
                    w.writerow([str(tid), tname, f"{u:,.2f}", ""])
                    if m > 0:
                        w.writerow([str(tid), tname, f"{m:,.2f}", "mp"])

                # ---- (2) Rest-of-org: exclude all target ids ----
                u_rest = get_total_for_period(ce, start_iso, end_iso, _AND(usage_filter, {"Not": _filter_linked(target_ids)}))
                m_rest = get_total_for_period(ce, start_iso, end_iso, _AND(mp_filter,    {"Not": _filter_linked(target_ids)}))
                rest_name = name + (rule.get("rest_name_suffix") or "")
                w.writerow([str(acct_id), rest_name, f"{u_rest:,.2f}", ""])
                if m_rest > 0:
                    w.writerow([str(acct_id), rest_name, f"{m_rest:,.2f}", "mp"])

            else:
                # Default behavior (no special handling):
                usage_total = get_total_for_period(ce, start_iso, end_iso, usage_filter)
                mp_total    = get_total_for_period(ce, start_iso, end_iso, mp_filter)
                w.writerow([str(acct_id), name, f"{usage_total:,.2f}", ""])
                if mp_total > 0:
                    w.writerow([str(acct_id), name, f"{mp_total:,.2f}", "mp"])

    print(f"Done. Wrote {OUT_CSV}")

if __name__=="__main__":
    main()
