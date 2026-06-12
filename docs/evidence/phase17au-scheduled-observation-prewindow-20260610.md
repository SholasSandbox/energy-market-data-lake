# Phase 17AU Scheduled Observation Pre-Window Check

Date: 2026-06-10

## Goal

Start continued scheduled observation with the AWS Budget guardrail in place,
and stop before the first post-guardrail scheduled run window.

## Boundary

This was a read-only pre-window check.

No manual Step Functions execution, manual Bedrock invocation, Terraform apply,
SNS publish, CloudFront invalidation, static-site rebuild, dashboard publish,
DNS change, ACM change, alarm change, or budget change was performed.

## Evidence

- `docs/evidence/phase17au-scheduled-observation-prewindow-aws-identity-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-budget-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-budget-notifications-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-schedule-state-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-recent-executions-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-failed-artifacts-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-dashboard-http-headers-20260610.txt`
- `docs/evidence/phase17au-scheduled-observation-prewindow-dashboard-json-check-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-dashboard-sha256-20260610.txt`
- `docs/evidence/phase17au-scheduled-observation-prewindow-managed-services-cost-sanitized-20260610.json`
- `docs/evidence/phase17au-scheduled-observation-prewindow-terraform-nochange-sanitized-20260610.txt`

## Result

- Current UTC time at boundary start was `2026-06-10T17:13:47Z`.
- The first scheduled run after the Phase 17AT budget guardrail apply is
  expected at `2026-06-11T07:30:00Z`.
- AWS Budget `energy-market-managed-workflow-monthly-cost` is `HEALTHY` with
  a `$1.00` monthly limit.
- Budget notifications remain `OK` for actual 80%, actual 100%, and forecasted
  100%.
- EventBridge schedule remains `ENABLED` with `cron(30 7 * * ? *)`.
- Recent Step Functions executions still show the scheduled June 8, 9, and 10
  successful runs; no post-guardrail scheduled run has occurred yet.
- No `failed/` artifacts modified since `2026-06-10T17:00:00Z` were found.
- Hosted dashboard snapshot remains the Phase 17AR scheduled snapshot with
  SHA-256 `2891aaea0e44c3bc6d4e042d6037faf04d1a9fef942b4c0d6eebda89c96876da`.
- Managed workflow service-basket month-to-date cost is `$0.0124457405`.
- Terraform plan with managed workflow, CloudFront, schedule-enabled, SNS, and
  budget variables preserved reports `No changes`.

## Decision

Phase 17AU is started but not complete. Resume after
`2026-06-11T07:30:00Z` to observe the first scheduled run that occurs with the
AWS Budget guardrail in place.
