# Repository Validation Hardening - 2026-07-09

## Scope

Repository-local repair for Domain 1 governance safety and Domain 3 validation
coverage. No AWS deployment, Terraform plan, Terraform apply, live AWS read, or
cloud resource mutation was performed.

## Changes Verified

- Replaced mutating AWS Organizations Terraform in
  `infra/terraform/lakehouse/iam-governance.tf` with a non-mutating governance
  boundary inventory that preserves the accepted `eu-west-2` account and OU
  model.
- Restored the documented S3 backend posture by removing the accidental local
  backend override and stray Terraform copies/backups from the worktree.
- Extended `.github/workflows/validate.yml` so CI now checks Python validation
  dependencies, JSON contracts, public evidence redaction, Terraform formatting,
  and backend-free Terraform validation.
- Refreshed the active `PLANS.md` sequence so it matches the tracker state for
  Security Tooling, AWS Config, GuardDuty, and remaining governance gaps.

## Local Validation

All checks passed locally:

```bash
terraform fmt -check -recursive
terraform -chdir=infra/terraform/lakehouse init -backend=false -no-color
terraform -chdir=infra/terraform/lakehouse validate -no-color
python3 -m compileall lambda glue scripts energy_market
python3 scripts/check_lakehouse_iam_policies.py
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
bash scripts/check_public_evidence_redaction.sh
bash -n scripts/closeout_demo.sh
.venv/bin/python scripts/check_phase8_handlers.py
.venv/bin/python scripts/check_phase8_runtime.py
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python scripts/check_phase17ac_source_label_sanitization.py
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py
npm --prefix dashboard-ui run build
```

## Tracker Mapping

- SAP-C02 Domain 1: keeps Organizations/SCP/security-service governance out of
  ordinary workload Terraform unless an explicit live-change boundary is opened.
- SAP-C02 Domain 3: adds repeatable validation coverage for Terraform, JSON
  contracts, public evidence redaction, and core local self-checks.
