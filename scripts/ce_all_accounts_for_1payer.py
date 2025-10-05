#!/usr/bin/env python3
# scripts/ce_all_accounts.py
# ----------------------------------------------------------
# Run:
#   source scripts/use-aws <payer-or-mgmt-profile>
#   python scripts/ce_all_accounts.py
# ----------------------------------------------------------

import os
import csv
import time
import datetime
from decimal import Decimal

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError

# ===========================
# CONFIG — לשנות כאן בקלות
# ===========================
# טווח תאריכים (מספרים)
START_DAY   = 1
START_MONTH = 9
START_YEAR  = 2025

END_DAY     = 30
END_MONTH   = 9
END_YEAR    = 2025

# גרנולריות: MONTHLY / DAILY (הסיכום תמיד לכל התקופה; DAILY שימושי לאימות)
GRANULARITY = "MONTHLY"

# מצב סינון נתונים (ברירת מחדל = כמו ב-UI ששלחת):
#   "ui"   -> מוציאים רק Tax ו-Solution Provider Program Discount (כרשומות Charge type)
#   "none" -> בלי סינון בכלל (מספר "גולמי")
#   "full" -> מוציאים Refund/Credit/Discount/Tax (RecordType) וגם Services (SPP/SavingsPlans/Tax)
FILTER_MODE = "ui"   # "ui" | "none" | "full"

# ❗אם תרצה להרחיב/לצמצם שירותים להוצאה (רק כש-FILTER_MODE="full"):
#   הוסף/הסר ערכים מהרשימה—הערכים חייבים להיות בדיוק כמו ב-Cost Explorer SERVICE.
SERVICE_EXCLUDE_LIST = [
    "Solution Provider Program Discount",  # SPP
   # "Savings Plans",
    "Tax",
    # דוגמאות נוספות שאולי תרצה:
    # "EC2 Instance Savings Plans",
    # "Compute Savings Plans",
    # "EC2 - Other"
]

# מטריקה
METRIC = "UnblendedCost"

# שם קובץ פלט עם חותמת זמן + מצב פילטר
ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = f"out/ce_all_accounts_{FILTER_MODE}_{ts}.csv"

# ===========================
# פונקציות עזר
# ===========================
def iso_date(y, m, d):
    return datetime.date(y, m, d).isoformat()

def build_filter_by_mode(mode: str):
    """
    בונה Filter עבור Cost Explorer לפי מצב:
      ui   : Not RECORD_TYPE in [Tax, SPP Discount]
      none : ללא פילטר
      full : Not RECORD_TYPE in [Refund, Credit, Discount, Tax]
             AND Not SERVICE in SERVICE_EXCLUDE_LIST
    """
    if mode == "none":
        return None

    if mode == "ui":
        return {
            "Not": {
                "Dimensions": {
                    "Key": "RECORD_TYPE",
                    "Values": ["Tax", "Solution Provider Program Discount"]
                }
            }
        }

    if mode == "full":
        parts = [
            {"Not": {"Dimensions": {"Key": "RECORD_TYPE",
                                    "Values": ["Refund", "Credit", "Discount", "Tax"]}}}
        ]
        if SERVICE_EXCLUDE_LIST:
            or_list = [{"Dimensions": {"Key": "SERVICE", "Values": [s]}}
                       for s in SERVICE_EXCLUDE_LIST]
            parts.append({"Not": {"Or": or_list}})
        return {"And": parts}

    # ברירת מחדל בטוחה
    return None

def session_clients():
    region = os.getenv("AWS_DEFAULT_REGION", "eu-west-1")
    cfg = Config(retries={"max_attempts": 5, "mode": "standard"})
    sess = boto3.Session(region_name=region)  # נשען על credים מה־use-aws
    return sess.client("ce", config=cfg), sess.client("organizations", config=cfg)

def call_ce_with_retry(ce, kwargs, max_attempts=6):
    attempt, backoff = 0, 1.0
    while True:
        attempt += 1
        try:
            return ce.get_cost_and_usage(**kwargs)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("Throttling", "ThrottlingException", "TooManyRequestsException") and attempt < max_attempts:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
                continue
            raise

def get_accounts_via_org(org_client):
    ids = []
    paginator = org_client.get_paginator("list_accounts")
    for page in paginator.paginate():
        for acct in page.get("Accounts", []):
            ids.append(acct["Id"])
    return ids

def get_accounts_via_ce_dimension(ce_client, start_iso, end_iso):
    ids = set()
    kwargs = {
        "TimePeriod": {"Start": start_iso, "End": end_iso},
        "Dimension": "LINKED_ACCOUNT",
        "Context": "COST_AND_USAGE",
    }
    resp = ce_client.get_dimension_values(**kwargs)
    for v in resp.get("DimensionValues", []):
        ids.add(v.get("Value", ""))
    while resp.get("NextPageToken"):
        kwargs["NextPageToken"] = resp["NextPageToken"]
        resp = ce_client.get_dimension_values(**kwargs)
        for v in resp.get("DimensionValues", []):
            ids.add(v.get("Value", ""))
    return sorted(ids)

def get_linked_accounts(org_client, ce_client, start_iso, end_iso):
    try:
        return get_accounts_via_org(org_client)
    except Exception:
        return get_accounts_via_ce_dimension(ce_client, start_iso, end_iso)

def map_account_names(org_client):
    mapping = {}
    try:
        paginator = org_client.get_paginator("list_accounts")
        for page in paginator.paginate():
            for acct in page.get("Accounts", []):
                mapping[acct["Id"]] = acct.get("Name", "")
    except Exception:
        pass
    return mapping

def fetch_account_cost(ce, start_iso, end_iso, granularity, acct_id, extra_filter):
    base_filter = {"Dimensions": {"Key": "LINKED_ACCOUNT", "Values": [acct_id]}}
    if extra_filter:
        f = {"And": [base_filter, extra_filter]}
    else:
        f = base_filter

    kwargs = {
        "TimePeriod": {"Start": start_iso, "End": end_iso},
        "Granularity": granularity,
        "Metrics": [METRIC],
        "Filter": f,
    }

    rows = []
    resp = call_ce_with_retry(ce, kwargs)
    for block in resp.get("ResultsByTime", []):
        start = block["TimePeriod"]["Start"]
        amt = Decimal(block.get("Total", {}).get(METRIC, {}).get("Amount", "0"))
        rows.append((start, amt))

    while resp.get("NextPageToken"):
        kwargs["NextPageToken"] = resp["NextPageToken"]
        resp = call_ce_with_retry(ce, kwargs)
        for block in resp.get("ResultsByTime", []):
            start = block["TimePeriod"]["Start"]
            amt = Decimal(block.get("Total", {}).get(METRIC, {}).get("Amount", "0"))
            rows.append((start, amt))
    return rows

def write_csv_totals(totals, acct_map, out_file, grand_total):
    os.makedirs(os.path.dirname(out_file), exist_ok=True)
    with open(out_file, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["linked_account_id", "linked_account_name", "total_unblended_cost"])
        for acct_id, total in sorted(totals.items(), key=lambda x: x[0]):
            w.writerow([acct_id, acct_map.get(acct_id, ""), str(total)])
        w.writerow([])
        w.writerow(["PAYER_TOTAL", "", str(grand_total)])

# ===========================
# MAIN
# ===========================
def main():
    start_iso = iso_date(START_YEAR, START_MONTH, START_DAY)
    end_iso = (datetime.date(END_YEAR, END_MONTH, END_DAY) + datetime.timedelta(days=1)).isoformat()

    ce, org = session_clients()
    ce_filter = build_filter_by_mode(FILTER_MODE)

    accounts = get_linked_accounts(org, ce, start_iso, end_iso)
    if not accounts:
        raise SystemExit("No accounts found. בדוק הרשאות Organizations או טווח תאריכים.")

    acct_map = map_account_names(org)

    print(f"[{FILTER_MODE}] Querying {len(accounts)} accounts {start_iso} -> {end_iso} (granularity={GRANULARITY}) ...")

    totals = {}
    for acct in accounts:
        print(f"- {acct}: fetching ...")
        rows = fetch_account_cost(ce, start_iso, end_iso, GRANULARITY, acct, ce_filter)
        totals[acct] = sum((amt for _, amt in rows), Decimal("0"))

    grand_total = sum(totals.values(), Decimal("0"))
    write_csv_totals(totals, acct_map, OUT_CSV, grand_total)
    print(f"Wrote totals CSV: {OUT_CSV}")
    print(f"Grand total (payer) [{FILTER_MODE}]: {grand_total}")

if __name__ == "__main__":
    main()
