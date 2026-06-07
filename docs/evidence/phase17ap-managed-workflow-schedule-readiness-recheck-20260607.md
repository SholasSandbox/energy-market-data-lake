# Phase 17AP Managed Workflow Schedule Readiness Recheck

<!-- markdownlint-disable MD013 -->

Date: 2026-06-07

## Boundary

Phase 17AP is a decision-only/no-apply readiness recheck before any managed
workflow schedule enablement execution.

No Terraform apply, EventBridge schedule enablement, Step Functions execution,
Bedrock invocation, SNS publish, IAM mutation, Lambda deploy, Step Functions
deploy, S3 write, CloudFront invalidation, static-site rebuild, or dashboard
publish was performed.

## Evidence

- `docs/evidence/phase17ap-readiness-aws-identity-sanitized-20260607.json`
- `docs/evidence/phase17ap-readiness-sns-topic-sanitized-20260607.json`
- `docs/evidence/phase17ap-readiness-sns-subscriptions-sanitized-20260607.json`
- `docs/evidence/phase17ap-readiness-schedule-state-20260607.json`
- `docs/evidence/phase17ap-readiness-state-machine-routing-sanitized-20260607.json`
- `docs/evidence/phase17ap-readiness-lambda-config-sanitized-20260607.json`
- `docs/evidence/phase17ap-readiness-recent-executions-20260607.json`
- `docs/evidence/phase17ap-readiness-dashboard-http-json-check-20260607.txt`
- `docs/evidence/phase17ap-readiness-current-terraform-nochange-20260607.txt`
- `docs/evidence/phase17ap-readiness-schedule-enable-candidate-plan-20260607.txt`

## Result

- SNS failure topic has `SubscriptionsConfirmed: 1`.
- SNS failure topic has `SubscriptionsPending: 0`.
- SNS subscription list shows one real email subscription ARN.
- EventBridge schedule remains `DISABLED`.
- Schedule expression remains `cron(30 7 * * ? *)`.
- State machine is `ACTIVE`.
- State machine still routes workflow failures through SNS publish.
- Managed merge routing remains present in the deployed state machine.
- Lambda configuration remains active with the managed Mistral Bedrock
  environment values.
- Recent Step Functions executions still show the latest Phase 17AG managed
  workflow smoke as `SUCCEEDED`.
- Hosted `dashboard_snapshot_v1.json` returned `200`.
- Hosted dashboard snapshot SHA-256 remains
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- Current Terraform plan with schedules disabled reports `No changes`.
- Schedule-enable candidate plan is narrow:
  `Plan: 0 to add, 1 to change, 0 to destroy`.
- Candidate change is only
  `aws_cloudwatch_event_rule.ai_orchestration_schedule[0]`:
  `state = "DISABLED" -> "ENABLED"`.

## Decision

Decision: **go-candidate for controlled managed workflow schedule enablement,
pending explicit approval**.

This is not automatic approval to apply. The next execution must preserve the
same guardrails and apply only the schedule state change if approved.

## Stop Control

Emergency CLI stop command:

```bash
aws events disable-rule \
  --region eu-west-2 \
  --name energy-market-ai-orchestration-schedule
```

Terraform rollback command shape:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  -var 'create_dashboard_bucket=true' \
  -var 'dashboard_cloudfront_enabled=true' \
  -var 'ai_orchestration_enabled=true' \
  -var 'ai_orchestration_managed_ai_enabled=true' \
  -var 'ai_orchestration_schedule_enabled=false' \
  -var 'ai_orchestration_sns_email=<alert-email>'
```

Stop criteria:

- SNS alert delivery fails or the subscription becomes unconfirmed
- managed workflow execution fails unexpectedly
- managed workflow publishes invalid dashboard JSON
- public dashboard snapshot becomes unhealthy or stale beyond the accepted demo
  posture
- Bedrock cost or invocation count deviates from the one-scheduled-run
  expectation
- Terraform plan after enablement shows unrelated drift
- operator cannot monitor the next scheduled run window

## Red-Green Evidence

Red:

- Earlier schedule enablement was blocked because the failure topic had no
  evidenced subscriber.

Green:

- Phase 17AO proved the SNS email alert path end to end.
- Phase 17AP confirms the active topic still has one confirmed subscription.
- The enablement candidate is a single EventBridge schedule state change.

Regression:

- no Terraform apply occurred
- no schedule enablement occurred
- no workflow execution occurred
- no Bedrock invocation occurred
- no SNS publish occurred
- no dashboard publish or CloudFront invalidation occurred

## Next Boundary

Recommended next slice: **Phase 17AQ controlled managed workflow schedule
enablement apply**, only after explicit approval.

The execution substate should:

- apply only the candidate EventBridge schedule state change
- keep the SNS email subscription intact
- capture preapply and postapply schedule/SNS/state-machine evidence
- verify postapply Terraform no-change posture
- record the next expected scheduled run window
- keep DNS, ACM, CloudWatch alarms, budgets, dashboard publish, manual workflow
  run, and repeated SNS test publish out of scope

## Proof Commands

```bash
python3 -m json.tool \
  docs/evidence/phase17ap-readiness-sns-topic-sanitized-20260607.json

python3 -m json.tool \
  docs/evidence/phase17ap-readiness-sns-subscriptions-sanitized-20260607.json

python3 -m json.tool \
  docs/evidence/phase17ap-readiness-schedule-state-20260607.json

terraform -chdir=infra/terraform/lakehouse validate

git diff --check
```
