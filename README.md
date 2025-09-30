# aws-runner
Run small FinOps scripts against a chosen AWS profile (SSO/STS via ~/.aws/config). Outputs saved to ./out/.
Basic scripts:
- whoami: prints STS identity (account/arn)
- ce_monthly: Cost Explorer monthly cost between dates (us-east-1)
- cw_probe: tiny CloudWatch probe to verify access

Here’s a short, clean README.md section you can drop in as-is:

## Quick AWS Account Switching

This repo includes a helper script `scripts/use-aws` that locks your shell to any AWS profile defined in `~/.aws/config`. After switching, every AWS CLI/Python/Terraform command will run inside that account without adding `--profile`.

### One-time setup
```bash
# Install Python dependencies if needed
pip install -U boto3 botocore

# Log in to Abra payer (SSO root)
aws sso login --profile abra-payer

Switch accounts
# Syntax:
scripts/use-aws <profile> [region]

# Examples:
scripts/use-aws alta-prod
scripts/use-aws alta-dev
scripts/use-aws cobra-sap-prod
scripts/use-aws cobra-car-plus


The script:

Ensures Abra SSO is valid.

Exports temporary credentials for the selected profile.

Sets AWS_PROFILE, AWS_DEFAULT_REGION, AWS_SDK_LOAD_CONFIG=1.

Runs aws sts get-caller-identity to confirm.

Run commands in the active account
# Python scripts
python scripts/whoami.py
python scripts/ec2_inventory.py

# AWS CLI
aws ec2 describe-instances --no-cli-pager
aws ce get-cost-and-usage --time-period Start=2025-09-01,End=2025-10-01 --granularity MONTHLY --metrics UnblendedCost

Switch to another account
scripts/use-aws cobra-car-plus
python scripts/ec2_efficiency_metrics.py

If you see “No AWS credentials/session”
aws sso login --profile abra-payer
scripts/use-aws <profile>