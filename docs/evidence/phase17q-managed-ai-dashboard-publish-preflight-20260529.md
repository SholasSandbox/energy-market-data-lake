# Phase 17Q Managed AI Dashboard Publish Preflight

<!-- markdownlint-disable MD013 -->

Date: 2026-05-29

## Boundary

Phase 17Q converted the Phase 17P public-safe `ai_insight_v1` evidence payload
into a local candidate `dashboard_snapshot_v1` evidence file.

No Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, S3 write, CloudFront invalidation, or public dashboard publish was
performed.

## Red-Green Evidence

Red:

- Phase 17P proved a schema-valid managed AI payload could be captured as
  evidence, but did not prove that it was ready for the public dashboard
  snapshot contract.
- The live dashboard currently serves the restored Phase 16 snapshot, so any
  publish must preserve a known rollback path.

Green:

- The Phase 17P payload was converted locally into
  `docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json`.
- The candidate validates against `schemas/dashboard_snapshot_v1.schema.json`.
- The current live `dashboard_snapshot_v1.json` remains healthy over
  CloudFront.

Regression:

- The local managed AI adapter proof still passes.
- Deterministic fallback remains unchanged.
- The public dashboard snapshot remains unchanged.
- Managed handler/state-machine deployment remains blocked.

## Candidate Snapshot

- Candidate path:
  `docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json`
- Candidate schema: `dashboard_snapshot_v1`
- Candidate SHA256:
  `1d0ddd36118c224d1ee23c3dbd41e1dd3d2b455c621364a2f3e46e3e561a7aeb`
- Source payload:
  `docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json`
- Source payload SHA256:
  `7d302b64d27c15656f3e4cc8239730c984869bac01ab68d6662d56f676acb55f`

## Live Snapshot Baseline

Read-only CloudFront proof:
`docs/evidence/phase17q-current-live-dashboard-snapshot-http-check-20260529.txt`

- Status: `HTTP/2 200`
- Schema: `dashboard_snapshot_v1`
- Generated at: `2026-05-22T09:11:09Z`
- SHA256:
  `897f685faca6f30719a4c881fe51099a0bfc92df3debcfeb6556cd3f143ed30a`
- Cache control: `no-cache`

## Decision

Do not publish in Phase 17Q.

The candidate snapshot validates, but the preflight found a publish-quality
issue: the React dashboard renders every insight source as an anchor. The
managed AI energy reference converts to a non-URL `href` value:
`curated electricity dashboard sample for 2026-05-07`.

That is schema-valid because the dashboard source contract currently allows a
plain string, but it is not yet a clean public dashboard link. The safest next
step is local source-link hardening before any S3 write or CloudFront
invalidation.

## Next Boundary

Recommended next slice: **Phase 17R: local managed AI dashboard source-link
hardening**.

Phase 17R should:

- keep Bedrock invocation out of scope
- keep dashboard publish out of scope
- normalize managed energy references into dashboard-safe labels and links
- preserve external news URLs as-is
- reject or neutralize non-public/private source references before publish
- validate the candidate snapshot locally

## Publish Commands For Later Approval

If a later phase explicitly approves publishing a corrected candidate, use a
latest-object upload plus immutable snapshot upload, then invalidate only the
snapshot paths:

```bash
aws s3 cp docs/evidence/<approved-dashboard-snapshot>.json \
  s3://energy-market-dashboard-public-464975959576-20260511/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control no-cache

aws s3 cp docs/evidence/<approved-dashboard-snapshot>.json \
  s3://energy-market-dashboard-public-464975959576-20260511/snapshots/run_id=<approved-run-id>/dashboard_snapshot_v1.json \
  --content-type application/json \
  --cache-control public,max-age=31536000,immutable

aws cloudfront create-invalidation \
  --distribution-id E2H9BGRGYAHKPN \
  --paths \
    "/dashboard_snapshot_v1.json" \
    "/snapshots/run_id=<approved-run-id>/dashboard_snapshot_v1.json"
```

## Rollback Path

Because Phase 17Q did not publish, no AWS rollback is required.

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
.venv/bin/python scripts/publish_dashboard_snapshot_local.py \
  --ai-insight docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json \
  --output docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python - <<'PY'
import json
from pathlib import Path
from jsonschema import Draft202012Validator, FormatChecker

schema = json.loads(Path("schemas/dashboard_snapshot_v1.schema.json").read_text())
payload = json.loads(
    Path(
        "docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json"
    ).read_text()
)
validator = Draft202012Validator(schema, format_checker=FormatChecker())
errors = sorted(validator.iter_errors(payload), key=lambda error: error.path)
if errors:
    raise SystemExit(errors[0].message)
print("phase17q candidate validates against dashboard_snapshot_v1.schema.json")
PY

curl -fsS https://d28yo76if4k3l1.cloudfront.net/dashboard_snapshot_v1.json \
  >/tmp/phase17q-current-live-dashboard-snapshot.json

python3 -m json.tool \
  docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json

npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17q-managed-ai-dashboard-publish-preflight-20260529.md

git diff --check
```
