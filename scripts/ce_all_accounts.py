#!/usr/bin/env python3
# =====================================================================
# File: scripts/ce_all_accounts.py
#
# What it does:
#   Runs AWS Cost Explorer for all linked accounts under the active
#   payer/management account (after sourcing `use-aws`), and outputs
#   TWO rows per linked account:
#       account_id,account_name,kind,total_unblended_cost
#   where `kind` ∈ {"usage","marketplace"} as defined below.
#
# How to run:
#   1) source scripts/use-aws abra-payer
#   2) python scripts/ce_all_accounts.py
#
# Output:
#   out/ce_all_accounts_<filter_mode>_<timestamp>.csv
#
# Notes:
#   - "usage": excludes Tax & SPP; excludes RECORD_TYPE=Marketplace.
#   - "marketplace": only RECORD_TYPE=Marketplace.
#   - Values formatted with thousand separators and 2 decimals.
# =====================================================================



import os, csv, time, datetime
from decimal import Decimal
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------- CONFIG ----------------
START_DAY, START_MONTH, START_YEAR = 1, 9, 2025
END_DAY,   END_MONTH,   END_YEAR   = 30, 9, 2025
GRANULARITY = "MONTHLY"
FILTER_MODE = "ui"  # "ui" | "none" | "full"
SERVICE_EXCLUDE_LIST = ["Solution Provider Program Discount", "Tax"]
METRIC = "UnblendedCost"
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = f"out/ce_all_accounts_{FILTER_MODE}_{ts}.csv"
# ----------------------------------------

def iso_date(y,m,d):
    return datetime.date(y,m,d).isoformat()

def build_filter_by_mode(mode: str):
    if mode == "none":
        return None
    if mode == "ui":
        return {
            "Not": {
                "Dimensions": {
                    "Key":"RECORD_TYPE",
                    "Values":["Tax","Solution Provider Program Discount"]
                }
            }
        }
    if mode == "full":
        parts = [
            {"Not":{"Dimensions":{"Key":"RECORD_TYPE","Values":["Refund","Credit","Discount","Tax"]}}}
        ]
        if SERVICE_EXCLUDE_LIST:
            parts.append({
                "Not":{
                    "Or":[{"Dimensions":{"Key":"SERVICE","Values":[s]}} for s in SERVICE_EXCLUDE_LIST]
                }
            })
        return {"And": parts}
    return None

def session_clients():
    region = os.getenv("AWS_DEFAULT_REGION","eu-west-1")
    cfg = Config(retries={"max_attempts":5,"mode":"standard"})
    sess = boto3.Session(region_name=region)  # נשען על הקרדנצ'לים הפעילים (use-aws)
    return sess.client("ce", config=cfg), sess.client("organizations", config=cfg)

def call_ce_with_retry(ce, kwargs, max_attempts=6):
    attempt, backoff = 0, 1.0
    while True:
        attempt += 1
        try:
            return ce.get_cost_and_usage(**kwargs)
        except ClientError as e:
            code = e.response.get("Error",{}).get("Code","")
            if code in ("Throttling","ThrottlingException","TooManyRequestsException") and attempt < max_attempts:
                time.sleep(backoff)
                backoff = min(backoff*2, 8.0)
                continue
            raise

def get_accounts_via_org(org):
    ids = []
    try:
        for page in org.get_paginator("list_accounts").paginate():
            for a in page.get("Accounts", []):
                ids.append(a["Id"])
    except Exception:
        pass
    return ids

def get_accounts_via_ce_dimension(ce, start_iso, end_iso):
    ids = set()
    kw = {
        "TimePeriod":{"Start":start_iso,"End":end_iso},
        "Dimension":"LINKED_ACCOUNT",
        "Context":"COST_AND_USAGE",
    }
    resp = ce.get_dimension_values(**kw)
    for v in resp.get("DimensionValues", []):
        ids.add(v.get("Value",""))
    while resp.get("NextPageToken"):
        kw["NextPageToken"] = resp["NextPageToken"]
        resp = ce.get_dimension_values(**kw)
        for v in resp.get("DimensionValues", []):
            ids.add(v.get("Value",""))
    return sorted(ids)

def get_linked_accounts(org, ce, start_iso, end_iso):
    ids = get_accounts_via_org(org)
    return ids if ids else get_accounts_via_ce_dimension(ce, start_iso, end_iso)

def map_account_names(org):
    m = {}
    try:
        for page in org.get_paginator("list_accounts").paginate():
            for a in page.get("Accounts", []):
                m[a["Id"]] = a.get("Name", "")
    except Exception:
        pass
    return m

def fetch_account_cost(ce, start_iso, end_iso, granularity, acct_id, extra_filter):
    base = {"Dimensions":{"Key":"LINKED_ACCOUNT","Values":[acct_id]}}
    f = {"And":[base, extra_filter]} if extra_filter else base
    kw = {
        "TimePeriod":{"Start":start_iso,"End":end_iso},
        "Granularity":granularity,
        "Metrics":[METRIC],
        "Filter":f
    }
    total = Decimal("0")
    resp = call_ce_with_retry(ce, kw)

    def grab(b):
        return Decimal(b.get("Total", {}).get(METRIC, {}).get("Amount", "0"))

    for b in resp.get("ResultsByTime", []):
        total += grab(b)

    while resp.get("NextPageToken"):
        kw["NextPageToken"] = resp["NextPageToken"]
        resp = call_ce_with_retry(ce, kw)
        for b in resp.get("ResultsByTime", []):
            total += grab(b)

    return total

def main():
    start_iso = iso_date(START_YEAR, START_MONTH, START_DAY)
    end_iso   = (datetime.date(END_YEAR, END_MONTH, END_DAY) + datetime.timedelta(days=1)).isoformat()

    ce, org = session_clients()
    ce_filter = build_filter_by_mode(FILTER_MODE)

    accounts = get_linked_accounts(org, ce, start_iso, end_iso)
    if not accounts:
        raise SystemExit("No accounts found.")
    names = map_account_names(org)

    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["account_id","account_name","total_unblended_cost"])
        for acct in accounts:
            total = fetch_account_cost(ce, start_iso, end_iso, GRANULARITY, acct, ce_filter)
            formatted_total = f"{total:,.2f}"   # פסיקי אלפים + שתי ספרות
            w.writerow([str(acct), names.get(str(acct), ""), formatted_total])

    print(f"Wrote totals CSV: {OUT_CSV}")

if __name__ == "__main__":
    main()
