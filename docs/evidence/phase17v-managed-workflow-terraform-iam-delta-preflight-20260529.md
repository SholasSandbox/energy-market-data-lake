# Phase 17V Managed Workflow Terraform/IAM Delta Preflight

Date: 2026-05-29

## Scope

Phase 17V modeled the Terraform/IAM delta required to route the AI
orchestration workflow through the managed Bedrock path.

This was plan-only:

- no Bedrock invocation
- no Terraform apply
- no IAM mutation
- no Lambda deployment
- no Step Functions deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, S3 write, CloudFront invalidation, dashboard
  publish, or live workflow execution

## Implemented Local Delta

Terraform now has an opt-in managed AI workflow switch:

- `ai_orchestration_managed_ai_enabled`
- `ai_orchestration_bedrock_model_id`
- `ai_orchestration_bedrock_model_arn`
- `ai_orchestration_bedrock_provider`
- `ai_orchestration_bedrock_max_tokens`
- `ai_orchestration_bedrock_temperature`

When managed mode is disabled, the Terraform configuration stays on the
deterministic path.

When managed mode is enabled, Terraform models:

- Lambda environment mode set to `managed`
- Bedrock model/provider/max-token/temperature environment variables
- Step Functions transition from `CreateAiInputBundle` to
  `MergeAiInsightManaged`
- Lambda payload action set to `MergeAiInsightManaged`
- least-privilege `bedrock:InvokeModel` permission scoped to the configured
  Bedrock foundation model ARN

Schedule enablement remains separate and disabled unless
`ai_orchestration_schedule_enabled` is changed in a later boundary.

## Plan Evidence

Three `-refresh=false` plans were captured.

The first local plan used the current local tfvars plus managed mode:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -var 'ai_orchestration_managed_ai_enabled=true'
```

Evidence:

- `docs/evidence/phase17v-managed-workflow-terraform-plan-refreshfalse-20260529.txt`

Result:

- `Plan: 1 to add, 4 to change, 4 to destroy`
- the destroys were unrelated CloudFront/dashboard-hosting resources
- root cause: local tfvars did not preserve
  `dashboard_cloudfront_enabled = true`
- decision: this plan is not apply-safe

The second plan isolated the managed workflow delta while preserving
CloudFront:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true'
```

Evidence:

- `docs/evidence/phase17v-managed-workflow-terraform-plan-isolated-refreshfalse-20260529.txt`

Result:

- `Plan: 1 to add, 4 to change, 0 to destroy`
- add: Bedrock InvokeModel policy for the AI orchestration Lambda
- change: Lambda environment
- change: Step Functions state-machine definition
- change: IAM policy documents that Terraform re-renders during apply
- no CloudFront, S3 hosting, schedule, DNS, ACM, alarm, or budget destroy

The third plan proved the deterministic rollback/default posture while
preserving CloudFront:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=false'
```

Evidence:

- `docs/evidence/phase17v-deterministic-rollback-terraform-plan-refreshfalse-20260529.txt`

Result:

- `No changes`

## Decision

Immediate managed workflow deployment remains **no-go**.

The Terraform/IAM delta is now modeled locally, but it must be reviewed in a
separate deployment decision before any apply.

## Red-Green Evidence

Red:

- Phase 17U identified that managed workflow deployment still needed
  Terraform/IAM, Lambda environment, Step Functions routing, rollback, and
  failure-path proof.

Green:

- Phase 17V models the deployment delta and captures an isolated plan with no
  destroys.

Regression:

- deterministic rollback/default plan is no-op when managed mode is disabled
- local managed AI adapter proof remains green
- dashboard source-link proof remains green
- dashboard publish is unchanged
- schedules remain disabled

## Verification

Commands run:

```bash
terraform -chdir=infra/terraform/lakehouse fmt
terraform -chdir=infra/terraform/lakehouse validate
terraform -chdir=infra/terraform/lakehouse plan -refresh=false -no-color -var 'ai_orchestration_managed_ai_enabled=true'
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true'
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=false'
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
```

## Boundary

Phase 17V does not deploy managed workflow routing.

Next recommended state:

- Phase 17W: managed workflow deployment decision
- no apply unless explicitly approved after plan review
- no schedule enablement in the deployment decision
