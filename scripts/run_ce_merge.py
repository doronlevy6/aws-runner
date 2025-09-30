#!/usr/bin/env python3
# =====================================================================
# File: scripts/run_ce_merge.py
#
# What it does:
#   1) Runs scripts/ce_payers_totals.py -> totals per payer
#      (two kinds possible per payer: usage and optional marketplace)
#   2) Runs scripts/ce_all_accounts.py (after `use-aws abra-payer`) ->
#      totals per linked account (usage and optional marketplace)
#   3) Merges both into a unified format:
#        account_id,account_name,kind,total_unblended_cost
#   4) Produces:
#        - CSV: amounts as strings "1,234.56"
#        - Excel: numeric amounts (#,##0.00), account_id as text
#
# How to run:
#   python scripts/run_ce_merge.py
#
# Output:
#   out_ce_runs/billing_final_<timestamp>.csv
#   out_ce_runs/billing_final_<timestamp>.xlsx
#
# Open Excel (macOS):
#   open out_ce_runs/billing_final_*.xlsx
# =====================================================================



import re, sys, subprocess, pandas as pd, pathlib
from datetime import datetime

SCRIPT_TOTALS   = "scripts/ce_payers_totals.py"
SCRIPT_ACCOUNTS = "scripts/ce_all_accounts.py"
OUT_DIR = pathlib.Path("out_ce_runs"); OUT_DIR.mkdir(exist_ok=True)
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_OUT  = OUT_DIR / f"billing_final_{ts}.csv"
XLSX_OUT = OUT_DIR / f"billing_final_{ts}.xlsx"

def run(cmd:str)->str:
    p = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    if p.returncode != 0:
        print(p.stderr or p.stdout, file=sys.stderr); sys.exit(p.returncode)
    return p.stdout

def find_csv(stdout:str):
    m = re.search(r'(?:Wrote totals CSV:|Done\. Wrote)\s+([^\s]+\.csv)', stdout)
    return pathlib.Path(m.group(1)) if m else None

def to_number(s):
    if isinstance(s, str):
        s = s.replace(",", "").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

print("[1/3] totals לכל הארגונים ...")
out_totals = run(f"python {SCRIPT_TOTALS}")
totals_path = find_csv(out_totals)
if not totals_path or not totals_path.exists():
    print("[ERR] לא נמצא קובץ totals", file=sys.stderr); sys.exit(1)
print("      ✓", totals_path)

print("[2/3] accounts של abra-payer ...")
out_accounts = run(f"bash -lc 'source scripts/use-aws abra-payer && python {SCRIPT_ACCOUNTS}'")
accounts_path = find_csv(out_accounts)
if not accounts_path or not accounts_path.exists():
    print("[ERR] לא נמצא קובץ accounts", file=sys.stderr); sys.exit(1)
print("      ✓", accounts_path)

# --- קורא את שני ה-CSV בפורמט האחיד ---
req = ["account_id","account_name","total_unblended_cost"]
df_tot = pd.read_csv(totals_path, dtype={"account_id":"string"})
df_acc = pd.read_csv(accounts_path, dtype={"account_id":"string"})

# הגנת איכות: אם בטעות אחד מהם לא בא בסכימה הנכונה
for name, df in (("totals", df_tot), ("accounts", df_acc)):
    missing = [c for c in req if c not in df.columns]
    if missing:
        print(f"[ERR] {name} CSV חסר עמודות: {missing}. ודא ששני הסקריפטים כותבים את אותו פורמט.", file=sys.stderr)
        sys.exit(2)

# המרת הסכום ממחרוזת "1,234.56" למספר
for df in (df_tot, df_acc):
    df["total_unblended_cost"] = df["total_unblended_cost"].map(to_number)

df_all = pd.concat([df_tot[req], df_acc[req]], ignore_index=True)

# --- CSV: כותבים סכום כמחרוזת מעוצבת 1,234.56 ---
df_csv = df_all.copy()
df_csv["total_unblended_cost"] = df_csv["total_unblended_cost"].map(lambda x: f"{x:,.2f}")
df_csv.to_csv(CSV_OUT, index=False)
print(f"[3/3] CSV  -> {CSV_OUT}")

# --- Excel: סכום מספרי + עמודת account_id כטקסט ---
with pd.ExcelWriter(XLSX_OUT, engine="xlsxwriter") as xw:
    df_all.to_excel(xw, sheet_name="all", index=False)
    ws = xw.sheets["all"]; wb = xw.book
    money = wb.add_format({"num_format":"#,##0.00"})
    text  = wb.add_format({"num_format":"@"})
    ws.set_column("A:A", 20, text)   # account_id כטקסט
    ws.set_column("B:B", 40)         # account_name
    ws.set_column("C:C", 18, money)  # total_unblended_cost
print(f"Excel -> {XLSX_OUT}")
