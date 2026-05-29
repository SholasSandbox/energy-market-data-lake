# Phase 17W Controlled Managed Workflow Terraform Apply

Date: 2026-05-29

## Scope

Phase 17W execution applied the approved managed workflow Terraform delta.

Explicit approval was granted for this apply substate.

Guardrails:

- preserve `dashboard_cloudfront_enabled = true`
- keep `ai_orchestration_schedule_enabled = false`
- use the isolated no-destroy saved plan shape
- no Bedrock invocation
- no live Step Functions workflow execution
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, dashboard publish, S3 write outside Terraform,
  or CloudFront invalidation

## Plan

Saved plan command:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -no-color \
  -out=tfplan-phase17w-managed-workflow \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true' \
  -var 'ai_orchestration_schedule_enabled=false'
```

Evidence:

- `docs/evidence/phase17w-managed-workflow-terraform-apply-plan-20260529.txt`

Result:

- `Plan: 1 to add, 4 to change, 0 to destroy`

## Apply

Apply command:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  -no-color \
  tfplan-phase17w-managed-workflow
```

Evidence:

- `docs/evidence/phase17w-managed-workflow-terraform-apply-20260529.txt`

Result:

- `Apply complete! Resources: 1 added, 2 changed, 0 destroyed.`
- added the AI orchestration Lambda Bedrock `InvokeModel` policy
- updated Lambda environment variables for managed mode
- updated the Step Functions definition to route through
  `MergeAiInsightManaged`

## Post-Apply Proof

Post-apply Terraform plan:

- `docs/evidence/phase17w-managed-workflow-postapply-plan-refreshfalse-20260529.txt`
- result: `No changes`

Read-only AWS evidence:

- `docs/evidence/phase17w-managed-workflow-lambda-config-20260529.json`
- `docs/evidence/phase17w-managed-workflow-state-machine-20260529.json`
- `docs/evidence/phase17w-managed-workflow-schedule-state-20260529.json`
- `docs/evidence/phase17w-managed-workflow-bedrock-policy-20260529.json`
- `docs/evidence/phase17w-managed-workflow-cloudfront-status-20260529.json`
- `docs/evidence/phase17w-managed-workflow-cloudfront-snapshot-http-check-20260529.txt`

Confirmed state:

- Lambda `AI_ORCHESTRATION_MODE` is `managed`
- Lambda `BEDROCK_MODEL_ID` is `mistral.ministral-3-8b-instruct`
- Step Functions status is `ACTIVE`
- `CreateAiInputBundle` now routes to `MergeAiInsightManaged`
- the deterministic merge state is no longer in the deployed state-machine
  definition
- EventBridge schedule remains `DISABLED`
- Bedrock policy allows `bedrock:InvokeModel` only on
  `arn:aws:bedrock:eu-west-2::foundation-model/mistral.ministral-3-8b-instruct`
- CloudFront distribution `E2H9BGRGYAHKPN` remains deployed
- live `dashboard_snapshot_v1.json` still returns `200`

Local regression proof:

- managed AI adapter proof passed
- dashboard source-link proof passed
- evidence JSON files parse successfully
- contract validation passed with failure samples still rejected

## Red-Green Evidence

Red:

- managed workflow deployment was previously a go-candidate only, not deployed.

Green:

- the controlled saved plan applied with no destroys and deployed managed
  workflow routing.

Regression:

- schedule enablement remains blocked
- dashboard publish remains unchanged
- CloudFront remains deployed
- no live workflow execution was run

## Boundary

Phase 17W execution deployed managed workflow routing only.

It did not run the managed workflow.

Next recommended state:

- Phase 17X: managed workflow smoke decision
- decide whether to run one controlled manual Step Functions execution
- keep schedules disabled until a later operating decision
