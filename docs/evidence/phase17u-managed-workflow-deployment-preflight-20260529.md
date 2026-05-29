# Phase 17U Managed Workflow Deployment Preflight

Date: 2026-05-29

## Scope

Phase 17U reviewed whether the managed AI path is ready to become the deployed
Step Functions workflow path.

This was preflight-only:

- no Bedrock invocation
- no Terraform apply
- no IAM change
- no Lambda deploy
- no Step Functions deploy
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, hosting, S3 write, CloudFront invalidation, or
  dashboard publish

## Current State

Phase 17S published the approved managed AI dashboard snapshot and Phase 17T
verified the hosted demo read-only.

The managed handler exists locally:

- `lambda/news_ai_orchestration.py` supports `MergeAiInsightManaged`
- `energy_market/managed_ai.py` builds and parses Bedrock Runtime requests
- local fake-client proof validates success, rejection, fallback, wrapper, and
  malformed-output behavior

The deployed workflow remains deterministic in Terraform:

- `infra/terraform/lakehouse/lambda.tf` sets
  `AI_ORCHESTRATION_MODE = "deterministic"`
- `infra/terraform/lakehouse/stepfunctions.tf` routes
  `CreateAiInputBundle` to `MergeAiInsightDeterministic`
- the EventBridge schedule variable remains disabled by default
- there is no current Terraform-managed `bedrock:InvokeModel` permission for
  the AI orchestration Lambda role

## Deployment Gap

A later managed workflow deployment would need a reviewed Terraform delta for:

- least-privilege `bedrock:InvokeModel` permission scoped to the approved
  Mistral model in the approved region
- Lambda environment variables for managed model ID, provider, max tokens,
  temperature, and mode
- Step Functions routing from deterministic merge to managed merge, or an
  explicit choice/mode gate that keeps deterministic rollback simple
- validation and failed-record quarantine behavior preserved exactly
- schedule-disabled posture preserved until a later operating decision
- rollback path back to deterministic state-machine routing and deterministic
  Lambda environment

## Decision

Immediate managed workflow deployment is **no-go**.

The safer next boundary is a plan-only deployment delta preflight. That slice
should model the IAM, Lambda environment, Step Functions routing, rollback, and
failure-path changes without applying them.

## Red-Green Evidence

Red:

- the dashboard now serves a managed AI snapshot, but the deployed workflow
  still uses deterministic merge

Green:

- the deployment gap is explicitly identified without mutating AWS

Regression:

- local managed AI adapter proof remains green
- dashboard source-link proof remains green
- Terraform formatting and validation remain green
- deterministic fallback remains intact

## Verification

Commands run:

```bash
git status --short --branch
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py
terraform -chdir=infra/terraform/lakehouse fmt -check
terraform -chdir=infra/terraform/lakehouse validate
```

Results:

- branch was isolated for Phase 17U
- managed AI adapter proof passed
- dashboard source-link proof passed
- Terraform formatting check passed
- Terraform configuration validation passed

## Boundary

Phase 17U does not deploy managed workflow routing.

Next recommended state:

- Phase 17V: managed workflow Terraform/IAM delta preflight
- plan-only unless separately approved
- no schedules, dashboard publish, Bedrock invocation, or Terraform apply
