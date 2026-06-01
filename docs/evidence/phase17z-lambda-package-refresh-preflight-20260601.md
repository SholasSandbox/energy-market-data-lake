# Phase 17Z Lambda Package Refresh Preflight

Date: 2026-06-01

## Scope

Phase 17Z reviewed the Lambda package refresh boundary after Phase 17Y proved
that the managed workflow reaches `MergeAiInsightManaged`, but the deployed
Lambda package does not recognize that action.

Guardrails:

- no Step Functions execution
- no Bedrock invocation
- no Terraform apply
- no Lambda deploy
- no IAM mutation
- no EventBridge schedule enablement
- no S3 write or CloudFront invalidation
- no dashboard publish

## Finding

Evidence:

- `docs/evidence/phase17z-lambda-package-local-before-build-20260601.txt`
- `docs/evidence/phase17z-current-lambda-config-sanitized-20260601.json`
- `docs/evidence/phase17z-current-schedule-state-20260601.json`

Result:

- the deployed Lambda `CodeSha256` is
  `ElgyDWfVG22HqYn8vx9hieJDenug/+AnmwINSjzB++g=`
- the pre-rebuild local Terraform package had the same base64 SHA-256
- that stale package did not contain `MergeAiInsightManaged`
- the repo source `lambda/news_ai_orchestration.py` does contain
  `MergeAiInsightManaged`
- Lambda environment remains in managed mode
- EventBridge schedule remains `DISABLED`

## Local Package Rebuild

Evidence:

- `docs/evidence/phase17z-lambda-package-rebuild-command-20260601.txt`
- `docs/evidence/phase17z-lambda-package-local-after-build-20260601.txt`

Result:

- rebuilt package path:
  `infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip`
- rebuilt package base64 SHA-256 is
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- rebuilt package contains `energy_market/managed_ai.py`
- rebuilt package contains `MergeAiInsightManaged`
- rebuilt package handler actions are:
  - `InitializeRun`
  - `ExportEnergyInput`
  - `IngestNewsSummary`
  - `CreateAiInputBundle`
  - `MergeAiInsightDeterministic`
  - `MergeAiInsightManaged`
  - `PublishDashboardSnapshot`

## Terraform Plan Evidence

Root refresh-false plan evidence:

- `docs/evidence/phase17z-lambda-package-refresh-terraform-plan-refreshfalse-20260601.txt`

Result:

- `Plan: 0 to add, 2 to change, 0 to destroy`
- `aws_lambda_function.ai_orchestration[0]` would update in place
- Lambda `source_code_hash` would move from the stale package hash to the
  rebuilt package hash
- `aws_iam_role_policy.ai_orchestration_state_machine[0]` would re-render in
  place because the policy document depends on the Lambda function ARN
- no state-machine definition change appears
- no EventBridge schedule enablement appears
- no CloudFront or dashboard hosting change appears
- no destroy appears

Targeted comparison plan evidence:

- `docs/evidence/phase17z-lambda-package-refresh-targeted-terraform-plan-refreshfalse-20260601.txt`

Result:

- `Plan: 0 to add, 1 to change, 0 to destroy`
- only `aws_lambda_function.ai_orchestration[0]` would update in place
- Terraform warns that `-target` is exceptional and may not represent the full
  requested configuration
- the targeted plan is comparison evidence only, not an automatic apply path

## Decision

Phase 17Z is a go-candidate for a controlled Lambda package refresh execution
substate, not an apply-by-default state.

Execution remains blocked until explicit approval.

The next execution boundary must choose one of these apply shapes deliberately:

- normal root apply with the Lambda code update plus the derived IAM policy
  re-render; or
- targeted Lambda-only apply, accepting Terraform's `-target` warning because
  the goal is recovery from a stale package deployment.

## Red-Green Evidence

Red:

- Phase 17Y failed at `MergeAiInsightManaged` because the deployed Lambda code
  package was stale.

Green:

- Phase 17Z rebuilt the local Terraform Lambda package and proved it contains
  the managed action handler and managed AI module.

Regression:

- no workflow retry was run
- no Bedrock invocation occurred
- no dashboard publish occurred
- no Terraform apply occurred
- EventBridge schedule remains disabled

## Boundary

Phase 17Z preflight is complete.

Next recommended state:

- Phase 17Z execution substate: controlled Lambda package refresh apply
- require explicit approval before apply
- keep schedules disabled
- do not run Step Functions or Bedrock during the package refresh
- run a second managed workflow smoke only in a later explicit boundary after
  the deployed Lambda package is refreshed and verified
