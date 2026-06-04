# Phase 17AC Managed Workflow Source-Label Sanitization

<!-- markdownlint-disable MD013 -->

Date: 2026-06-04

## Boundary

Phase 17AC locally hardened managed workflow dashboard source-label generation
after Phase 17AB found that a workflow-published snapshot could keep a private
lake S3 path in the rendered source label even though the source URL used the
safe `dashboard-data.json` fallback.

No Bedrock invocation, Step Functions execution, Terraform apply, IAM mutation,
Lambda deploy, Step Functions deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
dashboard publish was performed.

## Evidence

- `scripts/check_phase17ac_source_label_sanitization.py`
- `docs/evidence/phase17ac-managed-workflow-source-label-sanitization-candidate-20260604.json`

## Result

- `energy_market.news_ai.source_label_context` now treats private S3, ARN,
  local file, AWS account, Amazon-hosted, and curated lake-path references as
  non-public label context.
- private managed workflow source references now collapse to
  `curated dashboard evidence`.
- partition date context such as `date=2026-05-07` is preserved as
  `curated dashboard evidence for 2026-05-07`.
- existing public curated labels such as
  `curated electricity dashboard sample for 2026-05-07` remain unchanged.
- source URLs still use the existing Phase 17R `dashboard-data.json` fallback
  for private/non-public references.
- public news URLs remain preserved through the existing source-link path.
- the Phase 17AC candidate validates against `dashboard_snapshot_v1`.

Candidate proof:

- candidate SHA-256:
  `cad571711b2ad989c675c61daa880fdeae5dd3d1e3a098c48b26a40b6e6fce01`
- first source label:
  `elexon - demand_mw | curated dashboard evidence for 2026-05-07`
- first source URL: `dashboard-data.json`

## Red-Green Evidence

Red:

- Phase 17AB proved the hosted snapshot was healthy but found source-label
  public-surface drift from the managed workflow path.

Green:

- Phase 17AC proves the failure shape locally: a private lake reference no
  longer appears in the dashboard source label, while useful date context and
  the safe source URL are retained.

Regression:

- existing Phase 17R source-link hardening still passes
- managed AI adapter proof still passes
- deterministic fallback remains unchanged
- no live workflow execution or dashboard mutation occurred

## Next Boundary

Recommended next slice: **Phase 17AD managed workflow source-label publish
decision**.

Phase 17AD should decide whether to deploy the source-label sanitizer into the
managed workflow Lambda package and whether any new controlled workflow smoke
or dashboard publish is justified. It should remain decision/preflight-only
unless explicitly approved.

## Proof Commands

```bash
.venv/bin/python scripts/check_phase17ac_source_label_sanitization.py

.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

python3 -m json.tool \
  docs/evidence/phase17ac-managed-workflow-source-label-sanitization-candidate-20260604.json

npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17ac-managed-workflow-source-label-sanitization-20260604.md

.venv/bin/python scripts/validate_contracts.py --include-evidence \
  --check-failures

git diff --check
```
