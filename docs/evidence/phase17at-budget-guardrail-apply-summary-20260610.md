# Phase 17AT Budget Guardrail Apply

Date: 2026-06-10

## Goal

Apply only the Terraform-managed AWS Budget guardrail approved after Phase
17AS, then verify the budget and notification configuration read-only.

## Boundary

This was a controlled budget guardrail apply.

No manual Step Functions execution, manual Bedrock invocation, SNS publish,
CloudFront invalidation, static-site rebuild, dashboard publish, DNS change,
ACM change, or unrelated alarm work was performed.

## Evidence

- `docs/evidence/phase17at-budget-guardrail-apply-preapply-aws-identity-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-preapply-existing-budgets-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-preapply-target-budget-lookup-sanitized-20260610.txt`
- `docs/evidence/phase17at-budget-guardrail-apply-terraform-plan-sanitized-20260610.txt`
- `docs/evidence/phase17at-budget-guardrail-apply-terraform-apply-sanitized-20260610.txt`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-budget-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-budgets-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-budget-notifications-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-subscribers-actual80-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-subscribers-actual100-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-subscribers-forecast100-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-terraform-nochange-sanitized-20260610.txt`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-schedule-state-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-recent-executions-sanitized-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-dashboard-http-headers-20260610.txt`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-dashboard-json-check-20260610.json`
- `docs/evidence/phase17at-budget-guardrail-apply-postapply-dashboard-sha256-20260610.txt`

## Result

- Preapply target-budget lookup found that
  `energy-market-managed-workflow-monthly-cost` did not exist.
- Saved Terraform plan reported:
  `Plan: 1 to add, 0 to change, 0 to destroy`.
- The only planned resource was
  `aws_budgets_budget.managed_workflow_cost[0]`.
- Terraform apply completed successfully:
  `Apply complete! Resources: 1 added, 0 changed, 0 destroyed.`
- Postapply budget verification shows:
  - budget name: `energy-market-managed-workflow-monthly-cost`
  - budget type: `COST`
  - limit: `$1.00`
  - time unit: `MONTHLY`
  - health status: `HEALTHY`
  - cost filter: Amazon Bedrock, Lambda, Step Functions, S3, CloudFront, SNS,
    and CloudWatch
- Postapply notifications are present and `OK`:
  - actual spend greater than 80%
  - actual spend greater than 100%
  - forecasted spend greater than 100%
- Each notification has the accepted email subscriber.
- Postapply Terraform plan with managed workflow, CloudFront, schedule-enabled,
  SNS, and budget variables preserved reports `No changes`.
- EventBridge schedule remains `ENABLED` with `cron(30 7 * * ? *)`.
- No new Step Functions execution was started by this phase; recent executions
  remain the scheduled June 8, 9, and 10 successful runs.
- Hosted dashboard snapshot remains the scheduled Phase 17AR snapshot with
  SHA-256 `2891aaea0e44c3bc6d4e042d6037faf04d1a9fef942b4c0d6eebda89c96876da`.

## Decision

Phase 17AT is complete as the controlled budget guardrail apply. Continued
scheduled observation can proceed with the AWS Budget guardrail in place.
