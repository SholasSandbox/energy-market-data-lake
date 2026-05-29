# Phase 17T Managed AI Dashboard Demo Verification

<!-- markdownlint-disable MD013 -->

Date: 2026-05-29

## Boundary

Phase 17T verified the hosted dashboard after the Phase 17S managed AI
dashboard snapshot publish.

This was a read-only verification slice. No Bedrock invocation, Terraform
apply, IAM change, state-machine deploy, EventBridge schedule enablement, DNS,
ACM, alarm, budget, S3 write, CloudFront invalidation, static-site rebuild, or
managed workflow deployment was performed.

## Red-Green Evidence

Red:

- Phase 17S execution published the managed AI dashboard snapshot, but the demo
  still needed read-only proof that the hosted dashboard and both snapshot
  paths were serving the expected payload.

Green:

- CloudFront returned `200` for `/`, `/index.html`, `/dashboard-data.json`,
  `/dashboard_snapshot_v1.json`, and the immutable managed AI snapshot path.
- The latest and immutable snapshot paths both match the approved Phase 17R
  candidate SHA256.
- Both snapshot paths validate against `dashboard_snapshot_v1`.
- The managed AI insight title, source-link hardening, and immutable cache
  policy are visible in read-only evidence.

Regression:

- No public dashboard mutation was performed in Phase 17T.
- Managed handler/state-machine deployment remains blocked.
- Deterministic fallback remains available.

## Evidence Files

- `docs/evidence/phase17t-managed-ai-dashboard-demo-http-check-20260529.txt`
- `docs/evidence/phase17t-managed-ai-dashboard-demo-json-check-20260529.txt`

## Verified Live Paths

- `https://d28yo76if4k3l1.cloudfront.net/`
- `https://d28yo76if4k3l1.cloudfront.net/index.html`
- `https://d28yo76if4k3l1.cloudfront.net/dashboard-data.json`
- `https://d28yo76if4k3l1.cloudfront.net/dashboard_snapshot_v1.json`
- `https://d28yo76if4k3l1.cloudfront.net/snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`

## Managed AI Snapshot

- SHA256:
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Schema: `dashboard_snapshot_v1`
- Generated at: `2026-05-29T12:29:09Z`
- Status: `watch`
- Primary insight:
  `GB Renewable Capacity Expansion Amid Industrial Decarbonisation Urgency`
- First source label:
  `elexon - demand_mw | curated electricity dashboard sample for 2026-05-07`
- First source URL: `dashboard-data.json`
- External news source URLs: preserved as `https://...`

## Demo Talking Point

The hosted dashboard now demonstrates the full controlled AI publish path:
managed Bedrock/Mistral output was validated, sanitized, converted into a
public-safe dashboard snapshot, source-link hardened, published to CloudFront,
and verified without enabling scheduled managed workflow deployment.

## Next Boundary

Recommended next slice: **Phase 17U: managed workflow deployment preflight**.

Phase 17U should remain preflight-only unless explicitly approved. It should
review IAM, Lambda environment, Step Functions routing, rollback, and
failure-path controls before any managed handler/state-machine deployment.

## Proof Commands

```bash
.venv/bin/python scripts/check_phase17r_dashboard_source_links.py

.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py

.venv/bin/python -m json.tool \
  docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json

npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17t-managed-ai-dashboard-demo-verification-20260529.md

git diff --check
```
