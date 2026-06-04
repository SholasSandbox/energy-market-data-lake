# Phase 17AE Lambda Package Refresh Preflight

<!-- markdownlint-disable MD013 -->

Date: 2026-06-04

## Boundary

Phase 17AE reviewed the Lambda package refresh boundary for the Phase 17AC
managed workflow source-label sanitizer.

This was a preflight-only state. No Bedrock invocation, Step Functions
execution, Terraform apply, IAM mutation, Lambda deploy, Step Functions deploy,
EventBridge schedule enablement, S3 write, CloudFront invalidation,
static-site rebuild, or dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ae-lambda-package-refresh-preflight-20260604.md`
- `docs/evidence/phase17ae-current-lambda-config-sanitized-20260604.json`
- `docs/evidence/phase17ae-current-schedule-state-20260604.json`
- `docs/evidence/phase17ae-current-recent-executions-20260604.json`
- `docs/evidence/phase17ae-lambda-package-before-rebuild-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-rebuild-command-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-rebuild-output-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-after-rebuild-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-refresh-root-plan-preserve-refreshfalse-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-refresh-targeted-plan-refreshfalse-20260604.txt`

## Current Deployed State

- deployed Lambda is active and still in managed mode
- deployed Lambda `CodeSha256` is
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- EventBridge schedule remains `DISABLED`
- recent executions still show the Phase 17AA managed workflow smoke as the
  latest Step Functions execution

## Package Rebuild

Before rebuild:

- local Terraform package existed
- package base64 SHA-256 was
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- package matched the deployed Lambda hash
- package contained `MergeAiInsightManaged`
- package did **not** contain `source_label_context`

Rebuild command:

```bash
PYTHON_BIN=.venv/bin/python ./scripts/build_phase8_lambda_package.sh
```

After rebuild:

- rebuilt package path:
  `infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip`
- rebuilt package base64 SHA-256 is
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- rebuilt package SHA-256 is
  `57f3d91f6d98157cf261aad74fe76094dfc927409ab0bd06d7316a6d5164d597`
- rebuilt package no longer matches the deployed Lambda hash
- rebuilt package contains `source_label_context`
- rebuilt package contains `PRIVATE_REFERENCE_DATE_RE`
- rebuilt package contains `MergeAiInsightManaged`
- rebuilt package contains `energy_market/managed_ai.py`

## Terraform Plan Evidence

Root plan command preserved the live dashboard and managed AI posture:

- `create_dashboard_bucket=true`
- `dashboard_cloudfront_enabled=true`
- `ai_orchestration_enabled=true`
- `ai_orchestration_managed_ai_enabled=true`
- `ai_orchestration_schedule_enabled=false`

Root no-apply plan result:

- `Plan: 0 to add, 2 to change, 0 to destroy`
- `aws_lambda_function.ai_orchestration[0]` would update in place
- Lambda `source_code_hash` would change from
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=` to
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- `aws_iam_role_policy.ai_orchestration_state_machine[0]` would re-render in
  place because the policy document depends on the Lambda function ARN
- no CloudFront destroy appears
- no dashboard bucket or bucket-policy destroy appears
- no Bedrock IAM policy destroy appears
- no EventBridge schedule enablement appears

Targeted comparison plan result:

- `Plan: 0 to add, 1 to change, 0 to destroy`
- only `aws_lambda_function.ai_orchestration[0]` would update in place
- Terraform emitted the expected `-target` warning
- the targeted plan is comparison evidence only, not an apply recommendation

## Decision

Decision: **go-candidate** for a controlled Lambda package refresh execution
substate.

Reason:

- the rebuilt local package contains the Phase 17AC source-label sanitizer
- the preserved root plan avoids the unsafe CloudFront and Bedrock IAM removals
  seen in Phase 17AD
- the preserved root plan has no destroys
- schedule remains disabled

Execution remains blocked until explicit approval.

Recommended execution shape:

- use the normal root saved plan, not the targeted plan, unless a future
  blocker justifies targeted recovery
- keep schedule enablement disabled
- do not run Step Functions
- do not invoke Bedrock
- do not write S3 dashboard objects
- do not invalidate CloudFront
- capture post-apply Lambda config, schedule state, dashboard HTTP check, and
  Terraform no-change evidence

## Red-Green Evidence

Red:

- Phase 17AD confirmed the deployed Lambda package was stale relative to the
  Phase 17AC sanitizer.

Green:

- Phase 17AE rebuilt the local package with the sanitizer and captured a clean
  no-destroy root plan with live dashboard hosting and managed Bedrock IAM
  preserved.

Regression:

- no workflow execution occurred
- no dashboard mutation occurred
- schedule remains disabled
- managed workflow smoke and schedule enablement remain later explicit
  boundaries

## Next Boundary

Recommended next slice: **Phase 17AE execution substate: controlled Lambda
package refresh apply**, only after explicit approval.

## Proof Commands

```bash
.venv/bin/python scripts/check_phase17ac_source_label_sanitization.py

python3 -m json.tool \
  docs/evidence/phase17ae-current-lambda-config-sanitized-20260604.json

python3 -m json.tool \
  docs/evidence/phase17ae-current-schedule-state-20260604.json

python3 -m json.tool \
  docs/evidence/phase17ae-current-recent-executions-20260604.json

terraform -chdir=infra/terraform/lakehouse validate

npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ae-lambda-package-refresh-preflight-20260604.md

git diff --check
```
