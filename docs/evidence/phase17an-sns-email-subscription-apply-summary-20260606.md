# Phase 17AN SNS Email Subscription Apply Summary

<!-- markdownlint-disable MD013 -->

Date: 2026-06-06

## Boundary

Phase 17AN execution applied the controlled SNS email subscription candidate
from the Phase 17AN preflight.

Terraform apply was explicitly limited to the accepted email subscription. The
EventBridge schedule stayed disabled.

No Bedrock invocation, Step Functions execution, EventBridge schedule
enablement, IAM broadening, Lambda deploy, Step Functions deploy, DNS, ACM,
alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `docs/evidence/phase17an-execution-aws-identity-sanitized-20260606.txt`
- `docs/evidence/phase17an-execution-preapply-failure-topic-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-preapply-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-preapply-schedule-state-20260606.json`
- `docs/evidence/phase17an-execution-preapply-lambda-config-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-preapply-recent-executions-20260606.json`
- `docs/evidence/phase17an-execution-preapply-dashboard-http-check-20260606.txt`
- `docs/evidence/phase17an-execution-preapply-dashboard-json-check-20260606.json`
- `docs/evidence/phase17an-execution-terraform-apply-plan-20260606.txt`
- `docs/evidence/phase17an-execution-terraform-apply-20260606.txt`
- `docs/evidence/phase17an-execution-postapply-failure-topic-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-postapply-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-postapply-schedule-state-20260606.json`
- `docs/evidence/phase17an-execution-confirmation-poll-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-test-publish-20260606.json`
- `docs/evidence/phase17an-execution-confirmed-failure-topic-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-confirmed-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-subscription-attributes-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-final-subscription-attributes-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-final-failure-topic-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-final-subscriptions-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-list-subscriptions-global-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-cloudtrail-confirm-subscription-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-cloudtrail-unsubscribe-sanitized-20260606.json`
- `docs/evidence/phase17an-execution-postconfirm-schedule-state-20260606.json`
- `docs/evidence/phase17an-execution-postconfirm-recent-executions-20260606.json`
- `docs/evidence/phase17an-execution-postconfirm-dashboard-http-check-20260606.txt`
- `docs/evidence/phase17an-execution-postconfirm-dashboard-json-check-20260606.json`
- `docs/evidence/phase17an-execution-postconfirm-terraform-nochange-20260606.txt`
- `docs/evidence/phase17an-execution-alert-receipt-summary-20260606.txt`
- `docs/evidence/phase17an-execution-subscription-state-summary-20260606.txt`

## Result

- Terraform apply completed successfully.
- Apply result: `1 added, 0 changed, 0 destroyed`.
- Applied resource:
  `aws_sns_topic_subscription.ai_orchestration_failure_email[0]`.
- Subscription protocol is `email`.
- Subscription endpoint is sanitized as `<alert-email>` in committed evidence.
- EventBridge schedule remained `DISABLED`.
- No Step Functions execution occurred.
- No Bedrock invocation occurred.
- hosted `dashboard_snapshot_v1.json` remained healthy.
- dashboard snapshot SHA-256 remained
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- dashboard source labels remained public-safe with `0` private references.
- post-confirm Terraform plan with the accepted email variable preserved
  reported `No changes`.

## Confirmation Evidence

Direct subscription attribute evidence reports:

- `PendingConfirmation=false`
- protocol `email`
- endpoint `<alert-email>`
- a real subscription ARN

However, SNS list and topic summary evidence did not settle cleanly:

- `list-subscriptions-by-topic` showed `SubscriptionArn: Deleted`
- topic attributes showed `SubscriptionsConfirmed: 0`
- topic attributes showed `SubscriptionsPending: 0`

CloudTrail lookup did not return recent `ConfirmSubscription` or `Unsubscribe`
events in the sampled evidence.

One SNS test publish was sent and AWS returned a `MessageId`. Human mailbox
receipt confirmation is still external to CLI evidence and should be recorded
after operator confirmation.

## Decision

Decision: **SNS subscription apply succeeded, but alert-readiness remains
not fully settled**.

Schedule enablement remains **no-go**.

Rationale:

- Terraform applied exactly the intended SNS subscription resource
- direct subscription attributes show the subscription is not pending
- test publish was accepted by AWS
- but SNS list/topic counters conflict with direct subscription attributes
- the operator has not yet confirmed mailbox receipt in committed evidence

## Red-Green Evidence

Red:

- Phase 17AN preflight found no active failure-topic subscriptions.

Green:

- Terraform added exactly one SNS email subscription resource and the
  subscription attribute API reports `PendingConfirmation=false`.

Yellow:

- SNS list/topic summary APIs still show a deleted or zero-subscription posture.
- mailbox receipt confirmation is pending operator response.

Regression:

- no schedule enablement occurred
- no workflow execution occurred
- no Bedrock invocation occurred
- no dashboard publish occurred
- no CloudFront invalidation occurred
- schedules remain disabled
- dashboard snapshot remains public-safe

## Next Boundary

Recommended next slice: **Phase 17AO SNS alert receipt and subscription
consistency verification**, read-only.

Phase 17AO should verify mailbox receipt, recheck SNS topic/list/subscription
attributes, and decide whether the alert path is settled enough to consider a
later schedule enablement decision. It should not enable schedules or run the
workflow.

## Proof Commands

```bash
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17an-sns-email-subscription-apply-summary-20260606.md

python3 -m json.tool \
  docs/evidence/phase17an-execution-final-subscription-attributes-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-execution-final-failure-topic-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-execution-final-subscriptions-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-execution-postconfirm-schedule-state-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-execution-postconfirm-recent-executions-20260606.json

python3 -m json.tool \
  docs/evidence/phase17an-execution-postconfirm-dashboard-json-check-20260606.json

terraform -chdir=infra/terraform/lakehouse validate

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

git diff --check
```
