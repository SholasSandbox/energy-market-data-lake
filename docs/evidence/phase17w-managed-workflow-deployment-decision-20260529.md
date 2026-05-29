# Phase 17W Managed Workflow Deployment Decision

Date: 2026-05-29

## Scope

Phase 17W reviewed the Phase 17V Terraform/IAM delta and decided whether a
managed workflow deployment execution boundary is justified.

This was a decision-only slice:

- no Bedrock invocation
- no Terraform apply
- no IAM mutation
- no Lambda deployment
- no Step Functions deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, S3 write, CloudFront invalidation, dashboard
  publish, or live workflow execution

## Evidence Reviewed

Phase 17V evidence:

- `docs/evidence/phase17v-managed-workflow-terraform-iam-delta-preflight-20260529.md`
- `docs/evidence/phase17v-managed-workflow-terraform-plan-refreshfalse-20260529.txt`
- `docs/evidence/phase17v-managed-workflow-terraform-plan-isolated-refreshfalse-20260529.txt`
- `docs/evidence/phase17v-deterministic-rollback-terraform-plan-refreshfalse-20260529.txt`

## Decision

Managed workflow deployment is a **go-candidate**, not an automatic apply.

The deployment execution boundary remains blocked until explicit approval.

The unsafe local Phase 17V plan must not be applied because it showed unrelated
CloudFront destroys:

- `Plan: 1 to add, 4 to change, 4 to destroy`

The only acceptable apply-candidate shape is the isolated managed plan:

- preserve `dashboard_cloudfront_enabled = true`
- enable `ai_orchestration_managed_ai_enabled = true`
- keep `ai_orchestration_schedule_enabled = false`
- keep DNS, ACM, alarms, budgets, dashboard publish, and live workflow
  execution out of scope
- expected isolated plan shape: `Plan: 1 to add, 4 to change, 0 to destroy`

## Apply Candidate

The future execution command must be reviewed before use and should be based on
a saved plan, not an ad hoc apply.

The apply candidate should preserve the no-destroy plan shape:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -out=tfplan-phase17w-managed-workflow \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true' \
  -var 'ai_orchestration_schedule_enabled=false'
```

Any future apply must use the reviewed saved plan only after explicit
approval:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  tfplan-phase17w-managed-workflow
```

These commands are documented for the future execution boundary only. They
were not run in Phase 17W.

## Rollback Path

Rollback remains deterministic:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -out=tfplan-phase17w-deterministic-rollback \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=false' \
  -var 'ai_orchestration_schedule_enabled=false'
```

Phase 17V already proved the deterministic rollback/default plan with
CloudFront preserved showed `No changes`.

## Red-Green Evidence

Red:

- Phase 17V modeled the managed deployment delta, but one local plan showed
  unrelated CloudFront destroys when `dashboard_cloudfront_enabled = true` was
  not preserved.

Green:

- Phase 17W narrows any future execution to the isolated no-destroy plan shape
  and keeps apply approval separate.

Regression:

- deterministic rollback remains the safe posture
- schedule enablement remains blocked
- dashboard publish remains unchanged
- no workflow execution was run

## Boundary

Phase 17W is complete as a decision slice.

Next possible state:

- Phase 17W execution substate: controlled managed workflow Terraform apply
- explicit approval required
- no schedule enablement
- no live workflow execution unless separately approved after deployment proof
