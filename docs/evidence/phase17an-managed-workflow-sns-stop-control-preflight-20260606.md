# Phase 17AN Managed Workflow SNS Email And Stop-Control Preflight

<!-- markdownlint-disable MD013 -->

Date: 2026-06-06

## Boundary

Phase 17AN reviewed the SNS email subscription and stop-control posture required
before any managed workflow schedule enablement.

This was a decision-only, no-apply preflight.

No Terraform apply, SNS subscription creation, SNS confirmation, test publish,
Bedrock invocation, Step Functions execution, EventBridge schedule enablement,
IAM mutation, Lambda deploy, Step Functions deploy, DNS, ACM, alarm, budget, S3
write, CloudFront invalidation, static-site rebuild, or dashboard publish was
performed.

The fence for this slice allowed read-only AWS checks, dashboard HTTP/JSON
checks, Terraform plan evidence, and stop-control documentation only.

## Evidence

- `docs/evidence/phase17an-sns-stop-preflight-aws-identity-sanitized-20260606.txt`
- `docs/evidence/phase17an-sns-stop-preflight-failure-topic-sanitized-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-current-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-schedule-state-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-lambda-config-sanitized-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-recent-executions-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-dashboard-http-check-20260606.txt`
- `docs/evidence/phase17an-sns-stop-preflight-dashboard-json-check-20260606.json`
- `docs/evidence/phase17an-sns-stop-preflight-current-terraform-nochange-20260606.txt`
- `docs/evidence/phase17an-sns-stop-preflight-subscription-candidate-plan-20260606.txt`
- `docs/evidence/phase17an-sns-stop-preflight-stop-control-runbook-20260606.md`
- `docs/evidence/phase17an-sns-stop-preflight-readiness-summary-20260606.txt`

## Current State

- failure topic name is `energy-market-ai-orchestration-failures`.
- failure topic ARN is present and Terraform-managed.
- current failure topic subscription evidence shows:
  - confirmed subscriptions: `0`
  - pending subscriptions: `0`
  - listed subscriptions: `[]`
- EventBridge rule `energy-market-ai-orchestration-schedule` remains
  `DISABLED`.
- Lambda `energy-market-news-ai-orchestration` remains `Active` with
  `LastUpdateStatus: Successful`.
- Lambda environment remains in managed mode with Mistral configuration.
- latest Step Functions execution remains the successful Phase 17AG smoke.
- hosted `dashboard_snapshot_v1.json` returns `200`.
- dashboard snapshot remains `dashboard_snapshot_v1` with metadata status
  `watch`.
- dashboard snapshot SHA-256 remains
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- dashboard source labels remain public-safe with `0` private references.
- current preserved Terraform plan reports `No changes`.

## Subscription Candidate

Accepted alert receiver:

- endpoint: `<alert-email>` in committed evidence
- raw mailbox is documented in the Phase 17 runbook recommendation
- protocol: `email`
- topic: `energy-market-ai-orchestration-failures`

Terraform candidate plan with the live CloudFront and managed workflow flags
preserved, schedule disabled, and the accepted alert mailbox configured showed
exactly one add:

- `aws_sns_topic_subscription.ai_orchestration_failure_email[0]`
- `protocol = "email"`
- `endpoint = "<alert-email>"`
- `topic_arn = "arn:aws:sns:eu-west-2:<account-id>:energy-market-ai-orchestration-failures"`
- `Plan: 1 to add, 0 to change, 0 to destroy`

No Terraform apply was run, so no SNS subscription was created and no mailbox
confirmation was attempted.

## Stop-Control Posture

Stop-control evidence was documented in
`docs/evidence/phase17an-sns-stop-preflight-stop-control-runbook-20260606.md`.

Emergency schedule disable command:

```bash
aws events disable-rule \
  --name energy-market-ai-orchestration-schedule \
  --region eu-west-2
```

Terraform reconciliation keeps:

- `ai_orchestration_schedule_enabled=false`
- live dashboard bucket and CloudFront preservation flags
- managed workflow flag preserved

Stop criteria include one scheduled workflow failure, missing or pending SNS
subscription, failed test alert receipt, schema validation failure, private
references in the public snapshot, unhealthy dashboard path, unexpected
executions, cost-cap breach, non-no-change Terraform plan, throttling, or
persistent runtime errors.

## Decision

Decision: **go-candidate for a controlled SNS email subscription apply and
mailbox confirmation; no-go for schedule enablement**.

Rationale:

- the existing failure topic should be reused
- the candidate Terraform plan is narrow and no-destroy
- email-only alerting is acceptable for the next proof
- CloudWatch alarms remain deferred
- schedule remains disabled and schedule enablement must stay blocked until
  alert confirmation and test-publish evidence exist

## Red-Green Evidence

Red:

- Phase 17AM found the failure SNS topic had no subscriptions.

Green:

- Phase 17AN proves that adding the accepted email receiver is a narrow
  Terraform change and documents the emergency stop-control posture.

Regression:

- no Terraform apply occurred
- no SNS subscription was created
- no SNS confirmation was attempted
- no test publish occurred
- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write occurred
- no schedule enablement occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- dashboard snapshot remains public-safe

## Next Boundary

Recommended next slice: **Phase 17AN execution substate: controlled SNS email
subscription apply and confirmation**, only after explicit approval.

That execution substate should apply only the SNS email subscription, wait for
mailbox confirmation, verify the subscription is confirmed, send one test
publish, and keep EventBridge schedule enablement blocked.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/git-state-command-reference.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17an-managed-workflow-sns-stop-control-preflight-20260606.md \
  docs/evidence/phase17an-sns-stop-preflight-stop-control-runbook-20260606.md

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-failure-topic-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-current-subscriptions-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-schedule-state-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-lambda-config-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-recent-executions-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-sns-stop-preflight-dashboard-json-check-20260606.json

terraform -chdir=infra/terraform/lakehouse validate

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

git diff --check
```
