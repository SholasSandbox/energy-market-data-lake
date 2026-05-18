# Phase 13: Dashboard Hosting Publish Runbook Proof

Use this checklist to prove the operator path for publishing dashboard build
artifacts to the Phase 12 private S3 plus CloudFront hosting boundary.

## Goal

Turn the Phase 12 hosting foundation into a repeatable publish procedure:

```text
React build -> approved public assets -> dashboard S3 bucket ->
CloudFront invalidation -> evidence note
```

The first implementation is plan-only by default. It proves commands, local
artifacts, contract checks, and evidence capture without writing to AWS unless
`--apply` is passed explicitly.

## Branch

```text
feature/phase13-dashboard-hosting-runbook-proof
```

## Current State

- Phase 12 added optional CloudFront and private S3 Terraform resources.
- CloudFront remains disabled until a live hosting decision.
- React dashboard assets are built locally under `dashboard-ui/dist`.
- Approved dashboard JSON lives under `dashboard-ui/public` before build.

## Target State

- A repeatable script builds and validates dashboard assets.
- The script renders or executes the S3 publish commands.
- The script renders or executes the CloudFront invalidation command.
- A Markdown evidence note records the publish inputs and commands.
- The live apply path is explicit and opt-in.

## Scope Boundary

In scope:

- Publish/runbook script.
- Plan-only dry-run evidence.
- S3 sync command path for HTML, JSON, and immutable assets.
- CloudFront invalidation command path.
- Docs and planning updates.

Out of scope:

- Terraform apply.
- Actual S3 upload or CloudFront invalidation during this repo change.
- DNS, ACM certificates, custom domains, alarms, budgets, schedules, or
  managed AI invocation.

## Implementation Checklist

### 1. Publish Script

- [x] Add `scripts/publish_dashboard_static_site.sh`.
- [x] Build React dashboard unless `--skip-build` is passed.
- [x] Run contract validation before publishing.
- [x] Read bucket and distribution values from Terraform outputs by default.
- [x] Support explicit `--bucket`, `--distribution-id`, and
  `--distribution-domain` overrides.
- [x] Default to plan-only mode.
- [x] Require `--apply` before AWS write commands execute.
- [x] Write Markdown evidence.

### 2. Runbook

- [x] Document plan-only command.
- [x] Document live apply command.
- [x] Document post-publish checks.
- [x] Keep Phase 13 separate from DNS, alarms, schedules, and managed AI.

### 3. Verification

- [x] Run shell syntax check.
- [x] Run plan-only script.
- [x] Run React build.
- [x] Run contract validation.
- [x] Run Terraform validation.
- [x] Run Markdown lint.
- [x] Run `git diff --check`.

Verification notes:

- `bash -n scripts/publish_dashboard_static_site.sh`
- `scripts/publish_dashboard_static_site.sh` with the Phase 13 evidence path
- `npm --prefix dashboard-ui run build`
- `.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures`
- `terraform validate`
- `npx markdownlint-cli2 README.md PLANS.md docs/target-operating-model.md`
- `npx markdownlint-cli2 docs/phase-12-dashboard-hosting-foundation.md`
- `npx markdownlint-cli2 docs/phase-13-dashboard-hosting-publish-runbook.md`
- `npx markdownlint-cli2 docs/evidence/dashboard-hosting-publish-runbook-proof-20260518.md`
- `git diff --check`

## Plan-Only Proof

Use plan-only mode before any live hosting action:

```bash
scripts/publish_dashboard_static_site.sh \
  --evidence-file docs/evidence/dashboard-hosting-publish-runbook-proof-20260518.md
```

This mode:

- builds the dashboard
- validates public contracts
- confirms required dashboard artifacts exist
- renders the S3 sync and CloudFront invalidation commands
- writes a Markdown evidence note
- does not write to AWS

## Live Apply Path

Use this only after Terraform has created the dashboard bucket and CloudFront
distribution:

```bash
scripts/publish_dashboard_static_site.sh --apply
```

Optional explicit values:

```bash
scripts/publish_dashboard_static_site.sh \
  --apply \
  --bucket energy-market-dashboard-public-<unique-suffix> \
  --distribution-id E123EXAMPLE \
  --distribution-domain d123example.cloudfront.net
```

## Post-Publish Checks

After a live apply, capture the checks in the evidence note:

```bash
aws s3 ls "s3://<dashboard-bucket>/"
aws cloudfront get-distribution --id "<distribution-id>"
curl -fsSI "https://<cloudfront-domain>/index.html"
curl -fsSI "https://<cloudfront-domain>/dashboard-data.json"
curl -fsSI "https://<cloudfront-domain>/dashboard_snapshot_v1.sample.json"
```

## Definition Of Done

- The publish path is scripted and repeatable.
- Plan-only evidence can be generated without AWS writes.
- Live apply requires an explicit `--apply`.
- Dashboard hosting proof remains separate from broader production operations.
