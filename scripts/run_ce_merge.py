# scripts/run_ce_merge.py
#!/usr/bin/env python3
# =====================================================================
# File: scripts/run_ce_merge.py
#
# What it does:
#   1) Runs scripts/ce_payers_totals.py -> totals per payer (usage "" + optional "mp")
#   2) Runs scripts/ce_all_accounts.py  -> totals per linked account (usage "" + optional "mp")
#   3) Merges both into a unified format (STRICT schema & order):
#        account_id,account_name,total_unblended_cost,kind
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

FINAL_COLS = ["account_id","account_name","total_unblended_cost","kind"]

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

print("[1/4] מפעיל totals לכל הארגונים ...")
out_totals = run(f"python {SCRIPT_TOTALS}")
totals_path = find_csv(out_totals)
if not totals_path or not totals_path.exists():
    print("[ERR] לא נמצא קובץ totals", file=sys.stderr); sys.exit(1)
print("      ✓", totals_path)

print("[2/4] מפעיל accounts של abra-payer ...")
out_accounts = run(f"bash -lc 'source scripts/use-aws abra-payer && python {SCRIPT_ACCOUNTS}'")
accounts_path = find_csv(out_accounts)
if not accounts_path or not accounts_path.exists():
    print("[ERR] לא נמצא קובץ accounts", file=sys.stderr); sys.exit(1)
print("      ✓", accounts_path)

print("[3/4] קורא ומאחד (כופה סכימה) ...")
base_dtypes = {"account_id":"string","account_name":"string"}
df_tot = pd.read_csv(totals_path, dtype=base_dtypes)
df_acc = pd.read_csv(accounts_path, dtype=base_dtypes)

# תאימות לאחור: אם חסר kind, הוסף עמודה ריקה
for name, df in (("totals", df_tot), ("accounts", df_acc)):
    if "kind" not in df.columns:
        df["kind"] = ""
    # המרת סכום למספר (עבור Excel); CSV יעוצב בנפרד למחרוזת
    df["total_unblended_cost"] = df["total_unblended_cost"].map(to_number)

    # בדיקת סכימה קשיחה
    missing = [c for c in FINAL_COLS if c not in df.columns]
    extra   = [c for c in df.columns if c not in FINAL_COLS]
    print(f"      {name} columns -> {list(df.columns)}")
    if missing:
        print(f"[ERR] {name} חסר עמודות: {missing}", file=sys.stderr); sys.exit(2)
    if extra:
        # שומר רק את העמודות הנדרשות ובסדר נכון
        df.drop(columns=[c for c in extra if c in df.columns], inplace=True)

# איחוד + סדר עמודות סופי
df_all = pd.concat([df_tot[FINAL_COLS], df_acc[FINAL_COLS]], ignore_index=True)
print(f"      merged columns -> {list(df_all.columns)}")

# --- CSV: סכום כמחרוזת מעוצבת 1,234.56 ---
df_csv = df_all.copy()
df_csv["total_unblended_cost"] = df_csv["total_unblended_cost"].map(lambda x: f"{x:,.2f}")
df_csv = df_csv[FINAL_COLS]  # הבטחת סדר
df_csv.to_csv(CSV_OUT, index=False)
print(f"[4/4] CSV  -> {CSV_OUT}")

# --- Excel: סכום מספרי + עמודות טקסט ---
with pd.ExcelWriter(XLSX_OUT, engine="xlsxwriter") as xw:
    df_all[FINAL_COLS].to_excel(xw, sheet_name="all", index=False)
    ws = xw.sheets["all"]; wb = xw.book
    money = wb.add_format({"num_format":"#,##0.00"})
    text  = wb.add_format({"num_format":"@"})
    ws.set_column("A:A", 20, text)   # account_id כטקסט
    ws.set_column("B:B", 40)         # account_name
    ws.set_column("C:C", 18, money)  # total_unblended_cost
    ws.set_column("D:D", 8,  text)   # kind ("", "mp")

print(f"Excel -> {XLSX_OUT}")
