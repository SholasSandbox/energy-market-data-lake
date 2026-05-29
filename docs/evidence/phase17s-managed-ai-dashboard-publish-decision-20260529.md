# Phase 17S Managed AI Dashboard Publish Decision

<!-- markdownlint-disable MD013 -->

Date: 2026-05-29

## Boundary

Phase 17S reviewed whether the Phase 17R managed AI dashboard candidate is
ready for a public dashboard publish execution boundary.

No Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, S3 write, CloudFront invalidation, or public dashboard publish was
performed.

## Red-Green Evidence

Red:

- Phase 17Q blocked publish because the managed AI candidate could render a
  non-URL source as a React dashboard anchor.
- Public dashboard publish must preserve a known rollback path to the current
  live snapshot.

Green:

- Phase 17R locally hardened source links and produced a candidate whose
  managed energy source targets `dashboard-data.json`.
- Public `https://...` news source links are preserved.
- The Phase 17R candidate validates against `dashboard_snapshot_v1`.
- The current live CloudFront snapshot still matches the Phase 16 restored
  rollback payload by SHA256.

Regression:

- Local managed AI adapter proof remains green.
- Local Phase 17R dashboard source-link proof remains green.
- Deterministic fallback remains unchanged.
- Managed handler/state-machine deployment remains blocked.

## Decision

Phase 17S is a **go-candidate** for a later managed AI dashboard publish
execution substate, but no publish is approved in this state.

Recommended execution shape:

- upload the Phase 17R candidate as the latest
  `dashboard_snapshot_v1.json`
- upload the same payload as an immutable snapshot
- invalidate only the latest and immutable snapshot paths
- do not rebuild or sync the full static site
- do not run Bedrock again
- do not deploy managed handler/state-machine routing

## Publish Candidate

- Candidate:
  `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`
- Candidate SHA256:
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Candidate schema: `dashboard_snapshot_v1`
- Candidate generated at: `2026-05-29T12:29:09Z`
- Candidate insight:
  `GB Renewable Capacity Expansion Amid Industrial Decarbonisation Urgency`

Recommended immutable publish key:

```text
snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json
```

## Current Live Snapshot

Read-only CloudFront proof:
`docs/evidence/phase17s-current-live-dashboard-snapshot-http-check-20260529.txt`

- Status: `HTTP/2 200`
- Schema: `dashboard_snapshot_v1`
- Generated at: `2026-05-22T09:11:09Z`
- SHA256:
  `897f685faca6f30719a4c881fe51099a0bfc92df3debcfeb6556cd3f143ed30a`
- Matches rollback payload:
  `docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json`

## Execution Commands For Explicit Approval

Do not run these commands until Phase 17S execution is explicitly approved.

```bash
aws s3 cp \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json \
  s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control no-cache

aws s3 cp \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json \
  s3://energy-market-dashboard-public-464975959576-20260511/snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control public,max-age=31536000,immutable

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths \
    "/dashboard_snapshot_v1.json" \
    "/snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json"
```

## Rollback Path

If Phase 17S execution later publishes the candidate and rollback is needed,
restore the Phase 16 payload to the latest snapshot and invalidate the latest
snapshot path:

```bash
aws s3 cp docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json \
  s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control no-cache

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths "/dashboard_snapshot_v1.json"
```

The immutable Phase 17S snapshot should remain as audit evidence unless a
separate cleanup phase explicitly removes it.

## Proof Commands

```bash
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

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

curl -fsS https://d28yo76if4k3l1.cloudfront.net/dashboard_snapshot_v1.json \
  >/tmp/phase17s-current-live-dashboard-snapshot.json

python3 -m json.tool \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json

npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17s-managed-ai-dashboard-publish-decision-20260529.md

git diff --check
```
