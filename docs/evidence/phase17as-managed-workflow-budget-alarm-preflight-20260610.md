# Phase 17AS Managed Workflow Budget Alarm Preflight

Date: 2026-06-10

## Goal

Decide whether a managed workflow budget guardrail is ready to move toward a
separate apply boundary before continued scheduled observation.

## Boundary

This was a decision-only and plan-only preflight.

No Terraform apply, manual Step Functions execution, manual Bedrock invocation,
SNS publish, CloudFront invalidation, static-site rebuild, dashboard publish,
DNS change, ACM change, CloudWatch alarm creation, or AWS Budget creation was
performed.

## Evidence

- `docs/evidence/phase17as-budget-alarm-preflight-aws-identity-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-existing-budgets-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-monthly-budget-notifications-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-zero-spend-budget-notifications-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-month-to-date-cost-by-service-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-managed-services-cost-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-cost-forecast-20260610.txt`
- `docs/evidence/phase17as-budget-alarm-preflight-cloudwatch-alarms-euw2-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-billing-metrics-sanitized-20260610.json`
- `docs/evidence/phase17as-budget-alarm-preflight-default-terraform-nochange-sanitized-20260610.txt`
- `docs/evidence/phase17as-budget-alarm-preflight-budget-candidate-plan-sanitized-20260610.txt`

## Current State

- Account-level AWS Budgets already exist:
  - `My Monthly Cost Budget`: `$10.00` monthly, actual spend `$3.113`,
    notification states `OK` at actual 85%, actual 100%, and forecasted 100%.
  - `My Zero-Spend Budget`: `$1.00` monthly, actual spend `$3.113`,
    notification state `ALARM` at actual `$0.01`.
- No `energy-market` CloudWatch alarms were found in `eu-west-2`.
- No `AWS/Billing` CloudWatch metrics were listed in `eu-west-2`, so the
  actionable guardrail path for this slice is AWS Budgets rather than a
  CloudWatch billing metric alarm.
- Cost Explorer month-to-date account spend by service includes non-project
  costs such as EC2-Other, IAM Access Analyzer, Secrets Manager, Route 53, and
  tax.
- Cost Explorer month-to-date spend for the managed workflow service basket
  is `$0.0121570959` for Amazon Bedrock, Lambda, Step Functions, S3,
  CloudFront, SNS, and CloudWatch.
- Cost Explorer forecast in `eu-west-2` was unavailable because the account has
  insufficient historical data.

## Candidate

Phase 17AS adds an opt-in Terraform candidate for an AWS Budgets cost guardrail:

- resource: `aws_budgets_budget.managed_workflow_cost[0]`
- default state: disabled with `managed_workflow_cost_budget_enabled=false`
- budget name: `energy-market-managed-workflow-monthly-cost`
- budget type: `COST`
- limit: `$1.00` monthly
- cost filter: service basket for Amazon Bedrock, Lambda, Step Functions, S3,
  CloudFront, SNS, and CloudWatch
- notifications, when an email is supplied:
  - actual spend greater than 80% of budget
  - actual spend greater than 100% of budget
  - forecasted spend greater than 100% of budget

This is a service-filtered project guardrail, not exact per-project cost
attribution. It intentionally excludes unrelated account costs that currently
dominate month-to-date spend.

## Terraform Posture

- Default preserved plan with managed workflow, CloudFront, schedule-enabled,
  and SNS variables reports `No changes`.
- Candidate plan with `managed_workflow_cost_budget_enabled=true` and the
  accepted notification email set reports:
  `Plan: 1 to add, 0 to change, 0 to destroy`.
- The only planned resource is
  `aws_budgets_budget.managed_workflow_cost[0]`.

## Decision

Phase 17AS is a go-candidate for a controlled budget guardrail apply, not an
automatic apply.

The next execution boundary may apply only the Terraform-managed budget
candidate after explicit approval, then verify the budget and notification
configuration read-only. Continued scheduled observation should follow after
the budget guardrail is in place.
