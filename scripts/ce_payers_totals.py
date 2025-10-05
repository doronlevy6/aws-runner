# scripts/ce_payers_totals.py
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
# Rules:
#   - Marketplace is detected via BILLING_ENTITY = "AWS Marketplace".
#   - Usage is "everything except Marketplace" (+ optionally excludes
#     Tax / SPP / Refund / Credit / Discount according to FILTER_MODE).
#   - MP row is written ONLY if total > 0.00
#   - Amounts formatted with thousand separators and 2 decimals.
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
    Usage  = הכל פחות AWS Marketplace ופחות מס/הנחות לפי מצב
    MP     = BILLING_ENTITY='AWS Marketplace' וללא Tax (וב-'full' גם Refund/Credit/Discount)
    """
    mp_base = {"Dimensions": {"Key": "BILLING_ENTITY", "Values": ["AWS Marketplace"]}}

    if mode == "none":
        usage = {"Not": mp_base}
        mp    = mp_base  # ברוטו כולל מס
        return usage, mp

    if mode == "ui":
        usage_parts = [
            {"Not": mp_base},
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Tax", "Solution Provider Program Discount"]}}},
        ]
        mp_parts = [
            mp_base,
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE", "Values": ["Tax"]}}},  # נטו ללא מס
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

            usage_total = get_total_for_period(ce, start_iso, end_iso, usage_filter)
            mp_total    = get_total_for_period(ce, start_iso, end_iso, mp_filter)

            w.writerow([str(acct_id), name, f"{usage_total:,.2f}", ""])
            if mp_total > 0:
                w.writerow([str(acct_id), name, f"{mp_total:,.2f}", "mp"])

    print(f"Done. Wrote {OUT_CSV}")

if __name__=="__main__":
    main()
