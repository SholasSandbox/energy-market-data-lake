# Phase 17AR Managed Workflow Scheduled-Run Observation

Date: 2026-06-10

## Goal

Observe the managed workflow after the first scheduled EventBridge trigger,
without starting a manual workflow execution or applying Terraform.

## Boundary

This was a read-only operating observation.

No manual Step Functions execution, manual Bedrock invocation, Terraform apply,
SNS test publish, CloudFront invalidation, static-site rebuild, dashboard
publish, DNS change, ACM change, alarm change, or budget change was performed.

The scheduled workflow itself was already enabled by Phase 17AQ and was allowed
to run naturally.

## Evidence

- `docs/evidence/phase17ar-scheduled-observation-aws-identity-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-schedule-state-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-schedule-targets-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-recent-executions-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-first-execution-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-latest-execution-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-cloudwatch-succeeded-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-cloudwatch-failed-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-ai-input-bundle-artifact-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-ai-input-bundle-summary-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-news-summary-artifact-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-ai-insight-artifact-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-ai-insight-summary-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-immutable-dashboard-artifact-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-dashboard-http-headers-20260610.txt`
- `docs/evidence/phase17ar-scheduled-observation-dashboard-json-check-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-dashboard-sha256-20260610.txt`
- `docs/evidence/phase17ar-scheduled-observation-failed-artifacts-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-sns-topic-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-sns-subscriptions-sanitized-20260610.json`
- `docs/evidence/phase17ar-scheduled-observation-terraform-nochange-20260610.txt`
- `docs/evidence/phase17ar-scheduled-observation-cost-summary-20260610.txt`

## Result

- EventBridge rule `energy-market-ai-orchestration-schedule` remains
  `ENABLED` with `cron(30 7 * * ? *)`.
- The rule target still points at the managed workflow Step Functions state
  machine through the EventBridge orchestration role.
- The first expected scheduled run fired from EventBridge at
  `2026-06-08T07:30:00Z` and succeeded.
- Subsequent scheduled runs on `2026-06-09T07:30:00Z` and
  `2026-06-10T07:30:00Z` also succeeded.
- CloudWatch Step Functions metrics show one succeeded execution per observed
  day and zero failed executions for the June 8 through June 10 observation
  window.
- The latest observed run is
  `ai-insight-20260610T073010Z-4d8bb555`, with workflow status
  `dashboard_snapshot_published`.
- The latest run produced the expected curated artifacts for energy input,
  news summary, AI input bundle, and AI insight, plus an immutable dashboard
  snapshot.
- No `failed/` S3 artifacts were found for objects modified since
  `2026-06-08T00:00:00Z`.
- Hosted `dashboard_snapshot_v1.json` returned `200`.
- The latest hosted dashboard snapshot was last modified at
  `2026-06-10T07:30:17Z` and has SHA-256
  `2891aaea0e44c3bc6d4e042d6037faf04d1a9fef942b4c0d6eebda89c96876da`.
- The dashboard hash differs from the Phase 17AQ baseline
  `4c4871a2ff09f11ed097e4c03f637b34812d3893a1d2dbb97b0584cc7001d4c0`,
  which confirms scheduled workflow dashboard publication occurred.
- The latest dashboard snapshot was generated at `2026-06-10T07:30:16Z`,
  keeps metadata status `watch`, and contains one managed insight with
  risk level `high`.
- SNS failure topic remains healthy with `SubscriptionsConfirmed: 1`,
  `SubscriptionsPending: 0`, and `SubscriptionsDeleted: 0`.
- Terraform plan with managed workflow, CloudFront, schedule-enabled, and SNS
  variables preserved reports `No changes`.

## Cost Posture

The workflow output still does not emit token usage. Using the established
Phase 17AA and Phase 17AG direct-model estimate of `$0.00132618` per managed
run:

- observed scheduled runs: `3`
- estimated direct model cost for observed scheduled runs: `$0.00397854`
- estimated 30-day daily direct model cost: `$0.03978540`
- estimated 31-day daily direct model cost: `$0.04111158`

This remains below the previous `$0.10` direct-model cap for the observed
window, but scheduled operation is now recurring and should continue to be
watched.

## Decision

Phase 17AR is complete as a read-only observation. The managed workflow has
successfully transitioned from a manual proof to scheduled operation for the
observed June 8 through June 10 window.

Any rollback, cadence change, alerting change, budget change, manual rerun, or
additional dashboard operation should remain a separate explicit decision.
