# Abra AWS Runner — Usage Guide

## 🌝 Overview

This project provides automation scripts for accessing and analyzing AWS accounts managed under the Abra payer organization via AWS SSO.

## ⚙️ Prerequisites

* AWS CLI v2 configured with profiles in `~/.aws/config`
* Python virtual environment in `.venv`
* Bash script `scripts/use-aws` (included)

## 🚀 Workflow

### 1. Login to SSO (once per day)

Run this command **once at the start of your workday** to authenticate your SSO session:

```bash
aws sso login --profile abra-payer
```

🟢 You only need to do this once a day.
After that, your SSO session remains active, and you can switch between accounts freely without re-logging in.

### 2. Load target account (switch between accounts as needed)

Once SSO is active, use the following command to switch accounts:

```bash
source scripts/use-aws <profile-name>
```

Example:

```bash
source scripts/use-aws fpaas-dev
```

This sets your AWS credentials and region automatically, verifies identity with `aws sts get-caller-identity`, and refreshes SSO if needed.

### 3. Activate Python environment

Activate the local Python virtual environment before running scripts:

```bash
source .venv/bin/activate
```

### 4. Run FinOps scripts

Once your environment is active, you can execute any of your automation scripts, for example:

```bash
python scripts/ce_all_accounts.py
```

## 💡 Tips

* `aws sso login --profile abra-payer` → **run once per day** to start your session.
* `source scripts/use-aws <profile>` → **use whenever you want to switch accounts**.
* The script automatically refreshes your SSO session if it has expired.
* Default AWS region is `eu-west-1`.
* To verify which account is currently active:

  ```bash
  aws sts get-caller-identity
  ```

## 🔁 Quick Start Examples

### 🕐 At the start of the day (first login)

Run once:

```bash
aws sso login --profile abra-payer && source scripts/use-aws abra-payer && source .venv/bin/activate
```

### 🔄 Switching to another account later (no need to log in again)

```bash
source scripts/use-aws <profile-name> && source .venv/bin/activate
```

Example:

```bash
source scripts/use-aws fpaas-dev && source .venv/bin/activate
```

## 🧱 Project Structure Example

```
aws-runner/
├── .venv/
├── scripts/
│   ├── use-aws
│   ├── ce_all_accounts.py
│   ├── ce_payers_totals.py
│   └── run_ce_merge.py
├── README.md
└── .aws/
    └── config
```

## ✅ Summary

* Perform `aws sso login --profile abra-payer` **once a day**.
* Use `source scripts/use-aws <profile>` to switch between customer accounts.
* Activate your Python environment with `source .venv/bin/activate`.
* Run FinOps scripts as needed.
* Everything else (credentials, refreshes, region) happens automatically.
