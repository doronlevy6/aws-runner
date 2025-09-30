#!/usr/bin/env python3
# =====================================================================
# File: scripts/ce_payers_totals.py
#
# What it does:
#   Iterates over a configured list of payer/management profiles.
#   For each payer, writes rows in unified schema:
#       account_id,account_name,kind,total_unblended_cost
#   where kind ∈ {"usage","MP"}.
#
# Rules:
#   - "usage": excludes RECORD_TYPE in {Tax, Solution Provider Program Discount, Marketplace}
#   - "MP":    includes only RECORD_TYPE=Marketplace
#   - MP row is written ONLY if total > 0.00
#   - Amounts formatted with thousand separators and 2 decimals.
#
# Run:   python scripts/ce_payers_totals.py
# Output: out/ce_payers_totals_<filter_mode>_<timestamp>.csv
# =====================================================================



import os, csv, datetime
from decimal import Decimal
import boto3
from botocore.config import Config

print("[ce_payers_totals] running from:", os.path.abspath(__file__))

# ---------------- CONFIG ----------------
START_DAY, START_MONTH, START_YEAR = 1, 9, 2025
END_DAY,   END_MONTH,   END_YEAR   = 30, 9, 2025
FILTER_MODE = "ui"  # "ui" | "none" | "full"

SERVICE_EXCLUDE_LIST = [
    "Solution Provider Program Discount",
    "Savings Plans",
    "Tax",
]

PAYER_PROFILES = [
    "altahq-from-abra","biz-from-abra","bot-authority","cobra-mgt-from-abra",
    "aidome","coinmarket","communi","genoox","imagry","jobtestprep","nanox",
    "onriva","owlduet","partake","senseip","storeman","three-spring-media",
    "vsee","wallter","yes-management","zoozpower",
]

METRIC, GRANULARITY = "UnblendedCost", "MONTHLY"
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = f"out/ce_payers_totals_{FILTER_MODE}_{ts}.csv"
# ----------------------------------------

def iso_date(y,m,d):
    import datetime as _dt
    return _dt.date(y,m,d).isoformat()

def build_filter_by_mode(mode: str):
    if mode == "none":
        return None
    if mode == "ui":
        return {"Not":{"Dimensions":{"Key":"RECORD_TYPE","Values":["Tax","Solution Provider Program Discount"]}}}
    if mode == "full":
        parts=[{"Not":{"Dimensions":{"Key":"RECORD_TYPE","Values":["Refund","Credit","Discount","Tax"]}}}]
        if SERVICE_EXCLUDE_LIST:
            parts.append({"Not":{"Or":[{"Dimensions":{"Key":"SERVICE","Values":[s]}} for s in SERVICE_EXCLUDE_LIST]}})
        return {"And":parts}
    return None

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

def main():
    import datetime as _dt
    start_iso = iso_date(START_YEAR,START_MONTH,START_DAY)
    end_iso   = ( _dt.date(END_YEAR,END_MONTH,END_DAY) + _dt.timedelta(days=1) ).isoformat()
    ce_filter = build_filter_by_mode(FILTER_MODE)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    print("[ce_payers_totals] SCHEMA -> account_id,account_name,total_unblended_cost")

    with open(OUT_CSV,"w",newline="",encoding="utf-8") as fh:
        w=csv.writer(fh)
        w.writerow(["account_id","account_name","total_unblended_cost"])

        for profile in PAYER_PROFILES:
            ce, sts, org = clients_for_profile(profile)
            acct_id = sts.get_caller_identity().get("Account", profile)
            name    = account_name(org, acct_id, profile)
            total   = get_total_for_period(ce, start_iso, end_iso, ce_filter)

            formatted_total = f"{total:,.2f}"  # פסיקים + שתי ספרות
            w.writerow([str(acct_id), name, formatted_total])
            print(f"[ce_payers_totals] wrote row -> {acct_id},{name},{formatted_total}")

    print(f"Done. Wrote {OUT_CSV}")

if __name__=="__main__":
    main()
