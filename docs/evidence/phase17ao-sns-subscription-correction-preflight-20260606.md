# Phase 17AO SNS Subscription Correction Preflight

<!-- markdownlint-disable MD013 -->

Date: 2026-06-06

## Boundary

Phase 17AO is a decision-only, no-apply preflight for correcting the inactive
SNS email subscription path found in Phase 17AN.

No Terraform apply, SNS subscribe call, SNS unsubscribe call, mailbox
resubscribe click, SNS test publish, Step Functions execution, Bedrock
invocation, EventBridge schedule enablement, IAM mutation, Lambda deploy,
Step Functions deploy, S3 write, CloudFront invalidation, static-site rebuild,
or dashboard publish was performed.

## Evidence

- `docs/evidence/phase17ao-sns-correction-aws-identity-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-topic-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-subscriptions-by-topic-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-subscriptions-global-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-direct-subscription-attributes-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-terraform-state-subscription-20260606.txt`
- `docs/evidence/phase17ao-sns-correction-schedule-state-20260606.json`
- `docs/evidence/phase17ao-sns-correction-recent-executions-20260606.json`
- `docs/evidence/phase17ao-sns-correction-cloudtrail-confirm-subscription-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-cloudtrail-unsubscribe-sanitized-20260606.json`
- `docs/evidence/phase17ao-sns-correction-current-root-plan-20260606.txt`
- `docs/evidence/phase17ao-sns-correction-remove-email-plan-20260606.txt`
- `docs/evidence/phase17ao-sns-correction-replace-email-plan-20260606.txt`

## Result

- SNS topic `energy-market-ai-orchestration-failures` still exists.
- Topic attributes report `SubscriptionsConfirmed: 0`.
- Topic attributes report `SubscriptionsPending: 0`.
- `list-subscriptions-by-topic` shows the accepted email endpoint with
  `SubscriptionArn: Deleted`.
- Direct `get-subscription-attributes` against the Terraform-tracked ARN still
  returns `PendingConfirmation=false` and the accepted endpoint.
- Terraform state still tracks
  `aws_sns_topic_subscription.ai_orchestration_failure_email[0]`.
- Normal root Terraform plan with the accepted email variable reports
  `No changes`.
- Remove-email candidate plan reports
  `Plan: 0 to add, 0 to change, 1 to destroy`.
- Replace-email candidate plan reports
  `Plan: 1 to add, 0 to change, 1 to destroy`.
- EventBridge schedule remains `DISABLED`.
- No new Step Functions execution was started.
- CloudTrail lookup did not return recent sampled `ConfirmSubscription` or
  `Unsubscribe` events.

## Decision

Decision: **do not enable the schedule and do not treat the current SNS
subscription as alert-ready**.

Preferred correction path: use a controlled Terraform replacement of
`aws_sns_topic_subscription.ai_orchestration_failure_email[0]`, only after
explicit approval.

Do not use the mailbox `Resubscribe` link as the primary correction path,
because it would be an out-of-band mutation while Terraform still tracks the
old subscription ARN. The replacement path keeps the correction auditable and
lets the next execution verify whether SNS topic counters, subscription lists,
direct attributes, and mailbox receipt all converge.

## Candidate Paths

Preserve current state:

- normal root plan reports `No changes`
- rejected as a correction path because SNS topic/list evidence still shows no
  active deliverable subscription

Remove email subscription:

- candidate plan reports one destroy
- useful only as cleanup if the project decides to pause alerting work
- not preferred if the next goal is schedule-readiness

Replace email subscription:

- candidate plan reports one destroy and one add
- preferred if the next goal is to repair the alert path
- must keep `ai_orchestration_schedule_enabled=false`
- must require mailbox confirmation and a single separately approved test
  publish before any schedule decision

## Red-Green Evidence

Red:

- Phase 17AN mailbox receipt showed unsubscribe confirmations instead of the
  expected test alert.
- SNS topic/list evidence still shows zero confirmed subscriptions and a
  deleted email subscription row.

Green:

- Terraform can express a narrow replacement candidate for the single SNS
  email subscription resource.
- Schedule state remains `DISABLED`.

Regression:

- no Terraform apply occurred
- no SNS subscribe, unsubscribe, or test publish occurred
- no workflow execution occurred
- no Bedrock invocation occurred
- no S3 write or dashboard publish occurred
- no schedule enablement occurred

## Next Boundary

Recommended next slice: **Phase 17AO execution substate: controlled SNS email
subscription replacement apply and confirmation**, only after explicit
approval.

The execution substate should:

- apply only the Terraform replacement candidate
- keep schedules disabled
- avoid workflow execution and Bedrock invocation
- complete mailbox subscription confirmation
- verify topic attributes report one confirmed subscription
- verify subscription lists show a real subscription ARN, not `Deleted`
- send one approved SNS test publish
- confirm the mailbox receives the test alert, not an unsubscribe confirmation
- stop if the endpoint unsubscribes again or if SNS evidence remains
  contradictory

## Proof Commands

```bash
python3 -m json.tool \
  docs/evidence/phase17ao-sns-correction-topic-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17ao-sns-correction-subscriptions-by-topic-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17ao-sns-correction-direct-subscription-attributes-sanitized-20260606.json

python3 -m json.tool \
  docs/evidence/phase17ao-sns-correction-schedule-state-20260606.json

terraform -chdir=infra/terraform/lakehouse validate

git diff --check
```
