# Phase 17R Managed AI Dashboard Source-Link Hardening

<!-- markdownlint-disable MD013 -->

Date: 2026-05-29

## Boundary

Phase 17R hardened local dashboard snapshot source-link generation after
Phase 17Q found that a schema-valid managed AI dashboard candidate could still
render a non-URL source as an anchor in the React dashboard.

No Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, S3 write, CloudFront invalidation, or public dashboard publish was
performed.

## Red-Green Evidence

Red:

- Phase 17Q produced a valid `dashboard_snapshot_v1` candidate, but the managed
  energy reference became a non-URL `href` value.
- Older local sample snapshots could also expose private `s3://` references as
  dashboard source links if rebuilt from unfiltered evidence.

Green:

- `energy_market.news_ai.dashboard_sources` now preserves public `http` and
  `https` news links as-is.
- Private, non-public, custom-scheme, or plain-text energy references now use
  the public dashboard fallback `dashboard-data.json`.
- The original managed energy reference context is retained in the source
  label instead of being used as the link target.
- The Phase 17R candidate snapshot validates against
  `dashboard_snapshot_v1.schema.json`.

Regression:

- Local managed AI adapter proof still passes.
- Dashboard source-link self-check covers the Phase 17P managed payload,
  private `s3://` reference neutralization, and public news URL preservation.
- Deterministic fallback remains unchanged.
- Public dashboard publish remains blocked until an explicit publish boundary.

## Candidate Snapshot

- Candidate path:
  `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`
- Candidate schema: `dashboard_snapshot_v1`
- Candidate SHA256:
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Managed energy source label:
  `elexon - demand_mw | curated electricity dashboard sample for 2026-05-07`
- Managed energy source URL: `dashboard-data.json`
- External news source URLs: preserved as `https://...`

## Decision

Phase 17R removes the source-link blocker found in Phase 17Q.

Do not publish in this state. The next state should be a final dashboard
publish decision/execution boundary with explicit approval before any S3 write
or CloudFront invalidation.

## Next Boundary

Recommended next slice: **Phase 17S: managed AI dashboard publish decision**.

Phase 17S should:

- compare the current live snapshot hash against the Phase 17Q baseline
- confirm the Phase 17R candidate is the intended publish payload
- decide whether to publish latest only, or latest plus immutable snapshot
- require explicit approval before any S3 write or CloudFront invalidation
- keep Bedrock invocation, Terraform, IAM, schedules, DNS, ACM, alarms, budgets,
  and managed workflow deployment out of scope

## Rollback Path

Because Phase 17R did not publish, no AWS rollback is required.

If a later publish needs rollback, restore the current live snapshot from the
Phase 16 evidence payload:

```bash
aws s3 cp docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json \
  s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control no-cache

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths "/dashboard_snapshot_v1.json"
```

## Proof Commands

```bash
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/publish_dashboard_snapshot_local.py \
  --ai-insight docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json \
  --output docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python - <<'PY'
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

schema = json.loads(Path("schemas/dashboard_snapshot_v1.schema.json").read_text())
payload = json.loads(
    Path(
        "docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json"
    ).read_text()
)
validator = Draft202012Validator(schema, format_checker=FormatChecker())
errors = sorted(validator.iter_errors(payload), key=lambda error: error.path)
if errors:
    raise SystemExit(errors[0].message)
print("phase17r candidate validates against dashboard_snapshot_v1.schema.json")
PY

python3 -m json.tool \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json

npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-hardening-20260529.md

git diff --check
```
