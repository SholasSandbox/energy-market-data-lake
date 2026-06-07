# Phase 17AQ Managed Workflow Schedule Enable Apply

Date: 2026-06-07

## Goal

Enable the managed AI workflow EventBridge schedule after Phase 17AP confirmed
the alerting, routing, rollback, and Terraform candidate posture.

## Approval

Explicit approval was granted for one controlled schedule enablement apply.

## Boundary

- Terraform apply was allowed only for the EventBridge schedule state change.
- No manual Step Functions execution was started.
- No manual Bedrock invocation was started.
- No SNS test publish was sent.
- No S3 write, CloudFront invalidation, static-site rebuild, or dashboard
  publish was performed.
- DNS, ACM, CloudWatch alarms, budgets, and additional operating changes
  remained out of scope.

## Evidence

- Preapply identity:
  `docs/evidence/phase17aq-execution-preapply-aws-identity-sanitized-20260607.json`
- Preapply SNS topic:
  `docs/evidence/phase17aq-execution-preapply-sns-topic-sanitized-20260607.json`
- Preapply SNS subscriptions:
  `docs/evidence/phase17aq-execution-preapply-sns-subscriptions-sanitized-20260607.json`
- Preapply schedule state:
  `docs/evidence/phase17aq-execution-preapply-schedule-state-20260607.json`
- Preapply state-machine routing:
  `docs/evidence/phase17aq-execution-preapply-state-machine-routing-sanitized-20260607.json`
- Preapply recent executions:
  `docs/evidence/phase17aq-execution-preapply-recent-executions-20260607.json`
- Preapply Lambda config:
  `docs/evidence/phase17aq-execution-preapply-lambda-config-sanitized-20260607.json`
- Preapply dashboard check:
  `docs/evidence/phase17aq-execution-preapply-dashboard-http-json-check-20260607.txt`
- Apply plan:
  `docs/evidence/phase17aq-execution-terraform-apply-plan-20260607.txt`
- Apply output:
  `docs/evidence/phase17aq-execution-terraform-apply-20260607.txt`
- Postapply schedule state:
  `docs/evidence/phase17aq-execution-postapply-schedule-state-20260607.json`
- Postapply SNS topic:
  `docs/evidence/phase17aq-execution-postapply-sns-topic-sanitized-20260607.json`
- Postapply SNS subscriptions:
  `docs/evidence/phase17aq-execution-postapply-sns-subscriptions-sanitized-20260607.json`
- Postapply state-machine routing:
  `docs/evidence/phase17aq-execution-postapply-state-machine-routing-sanitized-20260607.json`
- Postapply recent executions:
  `docs/evidence/phase17aq-execution-postapply-recent-executions-20260607.json`
- Postapply Lambda config:
  `docs/evidence/phase17aq-execution-postapply-lambda-config-sanitized-20260607.json`
- Postapply dashboard check:
  `docs/evidence/phase17aq-execution-postapply-dashboard-http-json-check-20260607.txt`
- Postapply Terraform no-change plan:
  `docs/evidence/phase17aq-execution-postapply-terraform-nochange-20260607.txt`
- Next scheduled run window:
  `docs/evidence/phase17aq-execution-next-scheduled-run-window-20260607.txt`
- Schedule enable status:
  `docs/evidence/phase17aq-execution-schedule-enable-status-20260607.txt`

## Result

- Normal root Terraform apply was used.
- The apply candidate was narrow:
  `Plan: 0 to add, 1 to change, 0 to destroy`.
- The only applied infrastructure change was
  `aws_cloudwatch_event_rule.ai_orchestration_schedule[0]`:
  `state = "DISABLED" -> "ENABLED"`.
- Terraform reported:
  `Apply complete! Resources: 0 added, 1 changed, 0 destroyed.`
- Postapply EventBridge schedule state is `ENABLED`.
- SNS failure topic remains healthy with `SubscriptionsConfirmed: 1` and
  `SubscriptionsPending: 0`.
- State machine remains `ACTIVE` and retains managed Lambda routing plus SNS
  failure publish routing.
- No manual workflow execution was started during this boundary.
- Hosted `dashboard_snapshot_v1.json` returned `200`.
- Dashboard snapshot SHA-256 remained
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`.
- Postapply Terraform plan with schedules enabled reports `No changes`.
- The next expected scheduled run is `2026-06-08T07:30:00Z`
  (`2026-06-08T08:30:00+0100` in Europe/London).

## Rollback

The rollback command is documented in:

`docs/evidence/phase17aq-execution-next-scheduled-run-window-20260607.txt`

Rollback means setting `ai_orchestration_schedule_enabled=false` through the
same normal root Terraform apply path.

## Decision

Phase 17AQ is complete as a controlled schedule enablement apply. The managed
workflow is no longer manual-only: it is now scheduled for the next daily
EventBridge trigger.

## Next Boundary

Phase 17AR should observe the first scheduled run read-only:

- no manual Step Functions execution unless a separate recovery decision is
  approved
- no Terraform apply unless rollback stop criteria are met
- capture EventBridge schedule state, execution history, S3 artifacts,
  dashboard impact, SNS failure status, and cost posture after the scheduled
  window
