# Phase 17Z Lambda Package Refresh Apply

Date: 2026-06-02

## Scope

Phase 17Z execution refreshed the deployed AI orchestration Lambda package after
explicit approval to use the normal root Terraform plan.

Guardrails:

- normal root saved plan only
- no Step Functions execution
- no Bedrock invocation
- no EventBridge schedule enablement
- no S3 write or CloudFront invalidation
- no dashboard publish
- no managed workflow retry

## Package Rebuild

Evidence:

- `docs/evidence/phase17z-execution-lambda-package-rebuild-command-20260602.txt`
- `docs/evidence/phase17z-execution-lambda-package-after-rebuild-20260602.txt`

Result:

- rebuilt package path:
  `infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip`
- rebuilt package base64 SHA-256:
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- rebuilt package contains `energy_market/managed_ai.py`
- rebuilt package contains `MergeAiInsightManaged`
- handler actions in the rebuilt package are:
  - `InitializeRun`
  - `ExportEnergyInput`
  - `IngestNewsSummary`
  - `CreateAiInputBundle`
  - `MergeAiInsightDeterministic`
  - `MergeAiInsightManaged`
  - `PublishDashboardSnapshot`

## Pre-Apply Evidence

Evidence:

- `docs/evidence/phase17z-execution-aws-identity-sanitized-20260602.txt`
- `docs/evidence/phase17z-execution-preapply-lambda-config-20260602.json`
- `docs/evidence/phase17z-execution-preapply-schedule-state-20260602.json`
- `docs/evidence/phase17z-execution-preapply-dashboard-http-check-20260602.txt`

Result:

- Lambda was active before apply
- pre-apply Lambda `CodeSha256` was
  `ElgyDWfVG22HqYn8vx9hieJDenug/+AnmwINSjzB++g=`
- pre-apply package was still stale relative to the rebuilt local package
- EventBridge schedule was `DISABLED`
- live dashboard snapshot returned `200`
- pre-apply dashboard snapshot SHA-256 was
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`

## Terraform Apply

Saved plan evidence:

- `docs/evidence/phase17z-execution-terraform-apply-plan-20260602.txt`

Plan result:

- `Plan: 0 to add, 2 to change, 0 to destroy`
- planned in-place Lambda package update
- planned in-place Step Functions IAM role-policy re-render
- no state-machine definition change
- no schedule enablement
- no CloudFront or dashboard hosting change
- no destroy

Apply evidence:

- `docs/evidence/phase17z-execution-terraform-apply-20260602.txt`

Apply result:

- `Apply complete! Resources: 0 added, 1 changed, 0 destroyed.`
- changed resource: `aws_lambda_function.ai_orchestration[0]`
- the IAM policy document was re-read, but no IAM resource changed during apply

## Post-Apply Proof

Evidence:

- `docs/evidence/phase17z-execution-postapply-lambda-config-20260602.json`
- `docs/evidence/phase17z-execution-postapply-schedule-state-20260602.json`
- `docs/evidence/phase17z-execution-postapply-dashboard-http-check-20260602.txt`
- `docs/evidence/phase17z-execution-postapply-terraform-nochange-20260602.txt`
- `docs/evidence/phase17z-execution-state-machine-20260602.json`
- `docs/evidence/phase17z-execution-recent-executions-20260602.json`

Result:

- Lambda remains active
- post-apply Lambda `CodeSha256` is
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- post-apply Lambda code hash matches the rebuilt package hash
- EventBridge schedule remains `DISABLED`
- Step Functions state machine remains `ACTIVE`
- recent Step Functions executions show no new run after the Phase 17Y smoke
- live dashboard snapshot still returns `200`
- post-apply dashboard snapshot SHA-256 remains
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- post-apply Terraform plan reports `No changes`

## Red-Green Evidence

Red:

- Phase 17Y failed at `MergeAiInsightManaged` because the deployed Lambda
  package was stale.

Green:

- Phase 17Z execution refreshed the deployed Lambda package so the live
  `CodeSha256` now matches the rebuilt package that contains
  `MergeAiInsightManaged`.

Regression:

- no managed workflow retry was run
- no Bedrock invocation occurred
- no dashboard publish occurred
- EventBridge schedule remains disabled
- post-apply Terraform reports no changes

## Boundary

Phase 17Z execution is complete.

Next recommended state:

- Phase 17AA: managed workflow second-smoke decision
- review whether the refreshed Lambda package is sufficient for one controlled
  second managed workflow smoke
- keep the smoke as publish-capable and explicit-approval only
- keep schedules disabled
