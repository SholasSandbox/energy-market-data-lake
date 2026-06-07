# Phase 17AO SNS Subscription Replacement Apply Summary

<!-- markdownlint-disable MD013 -->

Date: 2026-06-07

## Boundary

Phase 17AO execution applied the approved Terraform replacement for the single
SNS email subscription resource while keeping the managed workflow schedule
disabled.

No Step Functions execution, Bedrock invocation, EventBridge schedule
enablement, IAM broadening, Lambda deploy, Step Functions deploy, S3 write,
CloudFront invalidation, static-site rebuild, dashboard publish, or SNS test
publish was performed.

## Evidence

- `docs/evidence/phase17ao-execution-preapply-aws-identity-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-subscriptions-by-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-subscriptions-global-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-direct-subscription-attributes-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-terraform-state-subscription-20260607.txt`
- `docs/evidence/phase17ao-execution-preapply-schedule-state-20260607.json`
- `docs/evidence/phase17ao-execution-preapply-recent-executions-20260607.json`
- `docs/evidence/phase17ao-execution-terraform-replace-plan-20260607.txt`
- `docs/evidence/phase17ao-execution-terraform-replace-apply-20260607.txt`
- `docs/evidence/phase17ao-execution-confirmation-poll-sanitized-20260607.jsonl`
- `docs/evidence/phase17ao-execution-confirmation-second-poll-sanitized-20260607.jsonl`
- `docs/evidence/phase17ao-execution-confirmed-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-confirmed-subscriptions-by-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-confirmed-subscriptions-global-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-confirmed-schedule-state-20260607.json`
- `docs/evidence/phase17ao-execution-test-publish-20260607.json`
- `docs/evidence/phase17ao-execution-posttest-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-posttest-subscriptions-by-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-posttest-schedule-state-20260607.json`
- `docs/evidence/phase17ao-execution-posttest-recent-executions-20260607.json`
- `docs/evidence/phase17ao-execution-operator-mailbox-receipt-20260607.txt`
- `docs/evidence/phase17ao-execution-postapply-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-subscriptions-by-topic-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-subscriptions-global-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-direct-subscription-attributes-sanitized-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-terraform-state-subscription-20260607.txt`
- `docs/evidence/phase17ao-execution-postapply-schedule-state-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-recent-executions-20260607.json`
- `docs/evidence/phase17ao-execution-postapply-terraform-nochange-20260607.txt`
- `docs/evidence/phase17ao-execution-confirmation-status-20260607.txt`

## Result

- Terraform replacement apply completed successfully:
  `Apply complete! Resources: 1 added, 0 changed, 1 destroyed.`
- Replaced resource:
  `aws_sns_topic_subscription.ai_orchestration_failure_email[0]`.
- Preapply SNS list evidence showed the accepted email endpoint as
  `SubscriptionArn: Deleted`.
- Postapply SNS list evidence shows the accepted email endpoint as
  `SubscriptionArn: PendingConfirmation`.
- Postapply topic attributes show `SubscriptionsPending: 1`.
- Postapply topic attributes show `SubscriptionsConfirmed: 0`.
- Terraform state tracks the new replacement subscription ARN.
- Terraform state shows `pending_confirmation = true`.
- Direct subscription attributes still return `PendingConfirmation=false`,
  so direct attributes remain insufficient as sole readiness proof.
- Two read-only confirmation polling windows ended with
  `SubscriptionArn: PendingConfirmation`.
- A later confirmation check showed the mailbox confirmation completed:
  `SubscriptionsConfirmed: 1` and `SubscriptionsPending: 0`.
- Confirmed subscription list evidence shows a real subscription ARN, not
  `Deleted` or `PendingConfirmation`.
- One SNS test publish was sent after AWS-side confirmation.
- Test publish returned MessageId
  `0b28d38c-59e4-5c27-bdd5-b1130e292285`.
- Operator mailbox receipt is confirmed.
- Received subject: `Phase 17AO SNS alert path test`.
- The received message matched the controlled alert-path test text.
- No unsubscribe or deactivation notice was reported for the test alert.
- EventBridge schedule remained `DISABLED`.
- No new Step Functions execution was started.
- Postapply Terraform plan with the accepted email variable preserved reported
  `No changes`.

## Decision

Decision: **replacement apply succeeded and the SNS email alert path is
evidenced end to end**.

Schedule enablement remains **no-go until a separate schedule enablement
decision explicitly approves automation**.

The next action must be a fresh schedule enablement readiness recheck. Do not
enable the schedule from this phase.

## Red-Green Evidence

Red:

- Phase 17AN showed the previous endpoint as deleted or inactive after the
  operator received unsubscribe confirmations.
- Phase 17AO execution did not receive mailbox confirmation during the initial
  controlled polling windows.

Green:

- Terraform replacement was narrow and successful.
- The SNS endpoint advanced from `Deleted` to `PendingConfirmation`, then to a
  real confirmed subscription ARN.
- One SNS test publish was sent only after AWS-side confirmation.
- Schedule state remained `DISABLED`.

Yellow:

- Schedule enablement remains blocked until a later phase reviews the now
  evidenced alert path alongside cost, stop-control, freshness, and rollback.

Regression:

- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write or dashboard publish occurred
- no CloudFront invalidation occurred
- no schedule enablement occurred

## Next Boundary

Recommended next slice: **Phase 17AP managed workflow schedule enablement
readiness recheck**, decision-only/no-apply.

The next slice should:

- confirm the SNS subscription still has one confirmed endpoint
- confirm the latest schedule state is still disabled
- review recent workflow executions, cost posture, stop-control criteria,
  rollback commands, and dashboard freshness
- produce a go/no-go decision for a future schedule enablement execution
- keep schedule enablement out of scope until that future execution is
  explicitly approved

## Proof Commands

```bash
python3 -m json.tool \
  docs/evidence/phase17ao-execution-postapply-topic-sanitized-20260607.json

python3 -m json.tool \
  docs/evidence/phase17ao-execution-postapply-subscriptions-by-topic-sanitized-20260607.json

python3 -m json.tool \
  docs/evidence/phase17ao-execution-postapply-direct-subscription-attributes-sanitized-20260607.json

python3 -m json.tool \
  docs/evidence/phase17ao-execution-postapply-schedule-state-20260607.json

terraform -chdir=infra/terraform/lakehouse validate

git diff --check
```
