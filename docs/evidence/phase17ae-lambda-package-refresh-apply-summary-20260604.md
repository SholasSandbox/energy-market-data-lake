# Phase 17AE Lambda Package Refresh Apply

<!-- markdownlint-disable MD013 -->

Date: 2026-06-04

## Scope

Phase 17AE execution refreshed the deployed AI orchestration Lambda package
after explicit approval to use the normal root Terraform plan.

Guardrails:

- normal root saved plan only
- no Step Functions execution
- no Bedrock invocation
- no EventBridge schedule enablement
- no S3 write or CloudFront invalidation
- no static-site rebuild or dashboard publish
- no managed workflow smoke run

## Package Rebuild

Evidence:

- `docs/evidence/phase17ae-execution-lambda-package-rebuild-command-20260604.txt`
- `docs/evidence/phase17ae-execution-lambda-package-rebuild-output-20260604.txt`
- `docs/evidence/phase17ae-execution-lambda-package-after-rebuild-20260604.txt`

Result:

- rebuilt package path:
  `infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip`
- rebuilt package base64 SHA-256:
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- rebuilt package contains `source_label_context`
- rebuilt package contains `PRIVATE_REFERENCE_DATE_RE`
- rebuilt package contains `MergeAiInsightManaged`
- rebuilt package contains `energy_market/news_ai.py`
- rebuilt package contains `news_ai_orchestration.py`

## Pre-Apply Evidence

Evidence:

- `docs/evidence/phase17ae-execution-aws-identity-sanitized-20260604.txt`
- `docs/evidence/phase17ae-execution-preapply-lambda-config-20260604.json`
- `docs/evidence/phase17ae-execution-preapply-schedule-state-20260604.json`
- `docs/evidence/phase17ae-execution-preapply-state-machine-20260604.json`
- `docs/evidence/phase17ae-execution-preapply-recent-executions-20260604.json`
- `docs/evidence/phase17ae-execution-preapply-dashboard-http-check-20260604.txt`

Result:

- Lambda was active before apply
- pre-apply Lambda `CodeSha256` was
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- pre-apply package was stale relative to the rebuilt local package
- EventBridge schedule was `DISABLED`
- recent Step Functions executions still showed the Phase 17AA smoke as the
  latest run
- live dashboard snapshot returned `200`
- pre-apply dashboard snapshot SHA-256 was
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`

## Terraform Apply

Saved plan evidence:

- `docs/evidence/phase17ae-execution-terraform-apply-plan-20260604.txt`

Plan result:

- `Plan: 0 to add, 2 to change, 0 to destroy`
- planned in-place Lambda package update
- planned in-place Step Functions IAM role-policy re-render
- no state-machine definition change
- no schedule enablement
- no CloudFront or dashboard hosting change
- no destroy

Apply evidence:

- `docs/evidence/phase17ae-execution-terraform-apply-20260604.txt`

Apply result:

- `Apply complete! Resources: 0 added, 1 changed, 0 destroyed.`
- changed resource: `aws_lambda_function.ai_orchestration[0]`
- the IAM policy document was re-read, but no IAM resource changed during apply

## Post-Apply Proof

Evidence:

- `docs/evidence/phase17ae-execution-postapply-lambda-config-20260604.json`
- `docs/evidence/phase17ae-execution-postapply-schedule-state-20260604.json`
- `docs/evidence/phase17ae-execution-postapply-state-machine-20260604.json`
- `docs/evidence/phase17ae-execution-postapply-recent-executions-20260604.json`
- `docs/evidence/phase17ae-execution-postapply-dashboard-http-check-20260604.txt`
- `docs/evidence/phase17ae-execution-postapply-terraform-nochange-20260604.txt`

Result:

- Lambda remains active
- post-apply Lambda `CodeSha256` is
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- post-apply Lambda code hash matches the rebuilt package hash
- EventBridge schedule remains `DISABLED`
- Step Functions state machine remains `ACTIVE`
- recent Step Functions executions show no new run after the Phase 17AA smoke
- live dashboard snapshot still returns `200`
- post-apply dashboard snapshot SHA-256 remains
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`
- post-apply Terraform plan reports `No changes`

## Red-Green Evidence

Red:

- Phase 17AD and Phase 17AE preflight showed the deployed Lambda package was
  stale relative to the Phase 17AC source-label sanitizer.

Green:

- Phase 17AE execution refreshed the deployed Lambda package so the live
  `CodeSha256` now matches the rebuilt package that contains
  `source_label_context`.

Regression:

- no managed workflow smoke was run
- no Bedrock invocation occurred
- no dashboard publish occurred
- EventBridge schedule remains disabled
- post-apply Terraform reports no changes

## Boundary

Phase 17AE execution is complete.

Next recommended state:

- Phase 17AF: managed workflow post-refresh smoke decision
- review whether the refreshed Lambda package is sufficient for one controlled
  managed workflow smoke
- keep the smoke explicit-approval only
- keep schedules disabled
