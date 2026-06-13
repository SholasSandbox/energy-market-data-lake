# Phase 17AU Managed Workflow Scheduled Observation

Date: 2026-06-12

## Goal

Observe the first scheduled managed workflow run after the Phase 17AT AWS
Budget guardrail apply, then confirm the operating posture remains healthy.

## Boundary

This was a read-only observation.

No manual Step Functions execution, manual Bedrock invocation, Terraform apply,
SNS publish, CloudFront invalidation, static-site rebuild, dashboard publish,
DNS change, ACM change, alarm change, budget change, or cadence change was
performed.

## Evidence

Pre-window:

- `docs/evidence/phase17au-scheduled-observation-prewindow-20260610.md`
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

Post-run:

- `docs/evidence/phase17au-scheduled-observation-postrun-aws-identity-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-schedule-state-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-recent-executions-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-first-execution-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-latest-execution-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-artifacts-ai-insight-20260611T073010Z-c0977e3d-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-artifacts-ai-insight-20260612T073010Z-1bb3fff9-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-failed-artifacts-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-http-headers-20260612.txt`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-json-check-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-sha256-20260612.txt`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-s3-head-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-s3-json-check-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-immutable-http-headers-20260612.txt`
- `docs/evidence/phase17au-scheduled-observation-postrun-dashboard-immutable-json-check-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-budget-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-budget-notifications-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-sns-subscriptions-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-managed-services-cost-sanitized-20260612.json`
- `docs/evidence/phase17au-scheduled-observation-postrun-terraform-nochange-sanitized-20260612.txt`

## Result

- Current UTC time at post-run evidence capture was `2026-06-12T18:45:03Z`.
- EventBridge schedule remained `ENABLED` with `cron(30 7 * * ? *)`.
- The first post-guardrail scheduled event fired at
  `2026-06-11T07:30:00Z`.
- The first post-guardrail Step Functions execution started at
  `2026-06-11T08:30:08+01:00`, stopped at `2026-06-11T08:30:17+01:00`,
  and finished `SUCCEEDED`.
- The first post-guardrail workflow output reported
  `dashboard_snapshot_published` for run
  `ai-insight-20260611T073010Z-c0977e3d`.
- The June 12 scheduled execution also finished `SUCCEEDED` and published run
  `ai-insight-20260612T073010Z-1bb3fff9`.
- Each post-guardrail run wrote four lake artifacts and one immutable dashboard
  snapshot artifact.
- No `failed/` artifacts modified since `2026-06-11T07:30:00Z` were found.
- Hosted `dashboard_snapshot_v1.json` returned `200`; CloudFront still served
  the cached June 11 latest object with SHA-256
  `7e7938cfa7ee0980a47b588d7ba3bd66bf5d61fd4571a97b7c9c98128812188e`.
- S3 latest `dashboard_snapshot_v1.json` was updated by the June 12 scheduled
  run with `generated_at` `2026-06-12T07:30:16Z`.
- The immutable CloudFront path for run
  `ai-insight-20260612T073010Z-1bb3fff9` returned `200` and validated as
  `dashboard_snapshot_v1`.
- The dashboard snapshots remained `dashboard_snapshot_v1`, with three summary
  cards, one insight, 12 news articles, and public-safe source URLs.
- AWS Budget `energy-market-managed-workflow-monthly-cost` remained
  `HEALTHY`; actual spend was `$0.019` against the `$1.00` monthly budget.
- Budget notifications remained `OK` for actual 80%, actual 100%, and
  forecasted 100%.
- The SNS failure topic retained the accepted email subscription.
- Cost Explorer showed the managed-workflow service basket month-to-date cost
  was still below the `$1.00` budget.
- Terraform plan with managed workflow, CloudFront, schedule-enabled, SNS, and
  budget variables preserved reported `No changes`.

## Decision

Phase 17AU is complete. The first scheduled managed workflow run after the
Phase 17AT budget guardrail succeeded, the following scheduled run also
succeeded, S3 latest plus the immutable CloudFront dashboard path remained
reproducible, and the guardrail stayed healthy.
