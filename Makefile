PY?=python3
VENV=.venv
PIP=$(VENV)/bin/pip
PYBIN=$(VENV)/bin/python
PROFILE?=abra-payer
REGION?=eu-west-1
SERVICE?=ec2
START?=2025-08-01
END?=2025-09-01
GROUPBYS?=

setup:
	@echo ">>> Creating venv and installing deps"
	@test -d $(VENV) || python3 -m venv $(VENV)
	@$(PIP) install -r requirements.txt

login:
	@echo ">>> SSO login to $(PROFILE)"
	@aws sso login --profile $(PROFILE)

whoami:
	@$(PYBIN) scripts/whoami.py --profile $(PROFILE)

ce:
	@$(PYBIN) scripts/ce_monthly.py --profile $(PROFILE) --start $(START) --end $(END) --group-by "$(GROUPBYS)"

cw:
	@$(PYBIN) scripts/cw_probe.py --profile $(PROFILE) --region $(REGION) --service $(SERVICE)

clean:
	@rm -f out/*.csv out/*.json || true

ec2_audit:
	@$(PYBIN) scripts/ec2_audit.py --profile $(PROFILE) --start $(START) --end $(END)

