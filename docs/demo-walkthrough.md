# Demo Walkthrough

<!-- markdownlint-disable MD004 MD013 -->

Purpose: show a serverless energy data lake with proven electricity and ENTSOG
gas data paths, extended with validated news and AI insight outputs. Keep the
demo under 10 minutes.

Current demo state: the ENTSOG gas lakehouse path is proven through raw S3,
curated Parquet, Glue Catalog, Athena query, and validation evidence. The
news + AI extension is now live-proven in AWS as a manual Step Functions
workflow with Lambda handlers, validation gates, S3 artifacts, failed-run
quarantine, and a schedule that remains disabled by design. Phase 10 adds an
operator-focused React `Overview` page with alerts, executive KPIs, P&L
drivers, risk coverage, market/news context, AI insight, and data-quality
state. Phase 14F adds live CloudFront/S3 hosting for the React dashboard.

## 1. Business Problem

Energy teams need to connect market movement with portfolio risk and external
context. This project shows how energy data, RSS news, schema validation, and
an AI-style merge can produce a public-safe dashboard insight and an
operator-facing decision surface.

Say:

```text
This demo connects electricity and gas market facts with portfolio risk and
curated news context, validates every contract, rejects malformed output, and
publishes only approved dashboard JSON.
```

## 2. Architecture Story

Show:

- `diagrams/news-dashboard-high-level.svg`
- `diagrams/news-dashboard-detailed.svg`

Key points:

- Raw, curated, failed, and audit data stay private.
- The public dashboard reads only approved snapshot JSON.
- AI output must pass `ai_insight_v1.schema.json` before publishing.
- OpenClaw or Bedrock is a later cloud/runtime extension; this demo uses
  deterministic merge logic to prove orchestration and control boundaries first.

## 3. Run Local Pipeline

From repo root:

```bash
source .venv/bin/activate
python scripts/ingest_news_local.py
python scripts/export_energy_input_local.py
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence --check-failures
```

Expected result:

```text
All contracts are valid.
```

Explain:

```text
The important part is not that the sample insight is clever. The important part is that the output is controlled, source-linked, confidence-scored, and rejected if it breaks the contract.
```

## 4. Show Evidence Files

Good outputs:

- `docs/evidence/energy_input_v1.sample.json`
- `docs/evidence/curated/news_summary_v1.sample.json`
- `docs/evidence/ai/ai_input_bundle_v1.sample.json`
- `docs/evidence/curated/ai_insight_v1.sample.json`
- `dashboard-ui/public/dashboard_snapshot_v1.sample.json`

Gas lakehouse proof:

- `docs/evidence/run-entsog-gas-20260506.md`
- `docs/evidence/athena-gas-schema-20260506.md`
- `docs/evidence/athena-gas-query-summary-20260506.md`

Failure proof:

- `docs/evidence/failed/bad_energy_input_v1.sample.json`
- `docs/evidence/failed/bad_ai_insight_v1.sample.json`

The validator should reject bad files as expected.

Say:

```text
The gas slice is intentionally small: four ENTSOG pointDirections selected for live data, ingested to raw S3, transformed to curated Parquet, cataloged by Glue, and validated through Athena.
```

## 5. Show AWS AI Orchestration Proof

Show:

- `docs/evidence/phase8-aws-live-execution-20260511.md`
- `docs/phase-8-operational-runbook.md`

AWS proof state:

- Step Functions execution succeeded manually.
- Lambda wrote run-scoped curated artifacts to S3.
- The dashboard snapshot was published only after validation passed.
- A controlled failed run wrote to `failed/`.
- The previous dashboard snapshot was preserved after failure.
- EventBridge schedule remains disabled.

Evidence values:

```text
State machine: energy-market-ai-insight-orchestration
Successful run: ai-insight-20260511T114815Z-927685a3
Execution status: SUCCEEDED
Schedule state: DISABLED
```

Say:

```text
This is the AI orchestration hook: Step Functions gives an auditable execution
history, Lambda writes contract-bound S3 artifacts, validation gates the publish
step, and failed runs are quarantined without replacing the last good dashboard
snapshot.
```

## 6. Show Dashboard

Preferred live URL:

```text
https://d28yo76if4k3l1.cloudfront.net/
https://d28yo76if4k3l1.cloudfront.net/#overview
https://d28yo76if4k3l1.cloudfront.net/#portfolio-risk
https://d28yo76if4k3l1.cloudfront.net/#market-context
https://d28yo76if4k3l1.cloudfront.net/#quality
```

Live verification evidence:

- `docs/evidence/phase14f-dashboard-hosting-live-apply-summary-20260521.md`
- `docs/evidence/phase15-cloudfront-demo-http-check-20260521.txt`

Quick hosted checks:

```bash
python3 - <<'PY'
from http.client import HTTPSConnection

host = "d28yo76if4k3l1.cloudfront.net"
for path in ["/", "/dashboard-data.json", "/dashboard_snapshot_v1.sample.json"]:
    conn = HTTPSConnection(host, timeout=20)
    conn.request("HEAD", path)
    resp = conn.getresponse()
    print(path, resp.status, resp.reason)
    conn.close()
PY
```

Expected result:

```text
/ 200 OK
/dashboard-data.json 200 OK
/dashboard_snapshot_v1.sample.json 200 OK
```

Live AI snapshot check:

```bash
python3 - <<'PY'
import json
from http.client import HTTPSConnection

host = "d28yo76if4k3l1.cloudfront.net"
paths = [
    "/dashboard_snapshot_v1.json",
    "/snapshots/run_id=ai-insight-20260603T010744Z-4d89a62a/dashboard_snapshot_v1.json",
]
for path in paths:
    conn = HTTPSConnection(host, timeout=20)
    conn.request("GET", path)
    resp = conn.getresponse()
    payload = json.loads(resp.read().decode("utf-8"))
    print(path, resp.status, resp.reason, payload["schema_version"])
    conn.close()
PY
```

Expected result:

```text
/dashboard_snapshot_v1.json 200 OK dashboard_snapshot_v1
/snapshots/run_id=ai-insight-20260603T010744Z-4d89a62a/dashboard_snapshot_v1.json 200 OK dashboard_snapshot_v1
```

Say:

```text
The dashboard is now served through CloudFront with a private S3 origin and
Origin Access Control. The public surface is static and public-safe; private
raw, curated, failed, and audit data remain behind the lakehouse boundary.
```

Local fallback:

Run:

```bash
cd dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

```text
http://127.0.0.1:5173/
http://127.0.0.1:5173/#overview
http://127.0.0.1:5173/#portfolio-risk
http://127.0.0.1:5173/#market-context
http://127.0.0.1:5173/#quality
```

Quick checks:

```bash
curl -I http://127.0.0.1:5173/
curl -I http://127.0.0.1:5173/dashboard_snapshot_v1.sample.json
```

Expected result:

```text
HTTP/1.1 200 OK
```

Show the `Overview` tab:

- decision alert strip
- portfolio executive KPI summary
- P&L drivers ranked before market context
- hedge coverage versus policy band
- hedged versus open exposure
- exception-first investigation table
- compact power, gas, and news signals
- energy news insight snapshot
- data-quality and public contract state

Show the deterministic filter path:

- choose `Date Range: 7D`
- choose `Segment: EV`
- choose `Risk: breach`
- choose `Book: EV Flex Portfolio`
- reload or copy the URL and confirm the selected filters restore
- export the snapshot and note that `selected_filters` and `filtered_view`
  metadata are included in the JSON bundle

Say:

```text
The filters do not fetch private lake data. They deterministically reshape the
approved public dashboard JSON into a shareable operator view.
```

Show the `Energy News Insight Snapshot` section:

- power price
- power demand
- wider energy news count from the curated RSS evidence
- AI insight title and summary
- risk level
- confidence score
- source references
- curated market-news article grid for gas and electricity movement context
- data-quality checks
- freshness warning for old local demo data

Say:

```text
This panel links validated power-market evidence to curated wider energy news. The article grid gives gas and electricity movement context, while gas flow metrics stay in the separate ENTSOG gas section.
```

Show the `Portfolio Risk` view:

- portfolio P&L drivers
- coverage versus policy band
- hedged versus open exposure
- exception-first investigation table

Show the `Market Context` view:

- electricity-only Elexon and ENTSO-E market context
- gas data date
- total physical flow for the four selected pointDirections
- allocation proxy
- 4/4 completeness badge
- pointDirection table with flow, allocation proxy, delta, and status
- rolling 7-day charts for physical flow versus allocation, allocation delta, and completeness
- boundary note that gas context is separate from portfolio P&L

Show the `Data Quality` view:

- latest dashboard snapshot date
- freshness and completeness checks
- public snapshot contract status
- power evidence, news contract, and AI insight contract status

Screenshot artifact:

- `docs/evidence/screenshots/dashboard-phase10-overview-desktop-20260514.png`
- `docs/evidence/screenshots/dashboard-phase11-filters-desktop-20260516.png`
- `docs/evidence/screenshots/dashboard-phase11-filters-mobile-20260516.png`
- `docs/evidence/screenshots/dashboard-phase10-overview-tablet-20260514.png`
- `docs/evidence/screenshots/dashboard-phase10-overview-mobile-20260514.png`
- `docs/evidence/screenshots/dashboard-week4-local-mvp.png`
- `docs/evidence/screenshots/dashboard-phase7-gas-context-20260507.png`
- `docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png`
- `docs/evidence/screenshots/dashboard-power-tab-20260507.png`
- `docs/evidence/screenshots/dashboard-gas-tab-20260507.png`
- `docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png`
- `docs/evidence/screenshots/dashboard-news-expanded-20260507.png`

Say:

```text
The dashboard is deliberately reading the public snapshot, not private raw or curated evidence paths.
```

## 7. Close With Controls

Security:

- public dashboard does not read raw, curated, failed, or audit buckets directly
- malformed input is quarantined
- malformed AI output is rejected

Cost:

- static dashboard path
- scheduled/serverless ingestion
- no NAT Gateway
- no RDS
- no always-on EC2

Hiring signal:

- clear trust boundaries
- schema-controlled AI output
- failure handling evidence
- gas market data proven from source API to Athena validation
- operator-focused Overview rendered in the React dashboard from approved
  dashboard JSON
- gas context rendered in the React dashboard from Athena-backed dashboard data
- live AWS Step Functions proof for the deterministic AI insight workflow
- live CloudFront static dashboard delivery path
- cost-aware AWS architecture

## Known Limitations

- The local fallback AI merge is deterministic; the managed workflow path has
  now proven one controlled Bedrock/Mistral smoke.
- The energy evidence may be stale because it comes from the current local dashboard data.
- Gas metrics are rendered in the React dashboard context, but not in the public AI snapshot contract.
- RSS feed results change over time.
- Phase 8 schedule automation is intentionally disabled until a later operating
  decision.
- DNS, ACM certificate, CloudWatch alarms, managed workflow schedule
  automation, and repeated managed AI execution are intentionally separated
  into explicit operating boundaries.
- The live AI `dashboard_snapshot_v1.json` path now serves a managed workflow
  snapshot from Phase 17AA execution.
- Phase 17T read-only checks verify the hosted dashboard, latest managed AI
  snapshot, immutable managed AI snapshot, schema validity, and source-link
  hardening.
- Phase 17U keeps managed workflow deployment in preflight; the hosted demo can
  show the managed AI snapshot, but the scheduled workflow still remains
  deployment-blocked.
- Phase 17V models managed workflow deployment in Terraform only; do not demo
  it as deployed until a later apply boundary proves the state-machine switch.
- Phase 17W marks managed workflow deployment as a go-candidate only; the demo
  should still describe managed workflow deployment as pending explicit apply.
- Phase 17W execution applied managed workflow routing, but the demo should
  describe live managed workflow execution and schedule automation as pending
  later approval.
- Phase 17X keeps the managed workflow smoke as a decision-only go-candidate;
  do not demo it as executed until the controlled smoke run is evidenced.
- Phase 17Y proves managed workflow routing reaches the managed merge state,
  but live execution is blocked on a Lambda package refresh before retry.
- Phase 17Z execution refreshed the Lambda package that contains
  `MergeAiInsightManaged`.
- Phase 17AA execution proves one controlled managed workflow smoke can run
  through Bedrock/Mistral and publish latest plus immutable dashboard snapshots;
  schedules remain disabled.
- Phase 17AB read-only verification confirms the hosted dashboard and Phase
  17AA snapshot paths remain healthy after publication, but the managed
  workflow snapshot still needs source-label sanitization before it should be
  treated as fully demo-polished.
- Phase 17AC locally fixes managed workflow source-label sanitization; the live
  hosted snapshot remains unchanged until a later explicit deploy/publish
  boundary.
- Phase 17AD decides not to publish or deploy the source-label fix yet because
  the deployed Lambda package is still stale relative to the sanitizer and the
  current root Terraform plan is unsafe.
- Phase 17AE rebuilds the local Lambda package with the sanitizer and captures
  a safe no-destroy plan, but does not apply it or rerun the managed workflow.
- Phase 17AE execution refreshes the deployed Lambda package with the
  sanitizer while leaving schedules disabled and the dashboard snapshot
  unchanged; any workflow smoke remains a separate explicit decision.
- Phase 17AF records that a post-refresh managed workflow smoke is a
  go-candidate only; no smoke has run after the package refresh, and any run
  must preserve rollback and source-label evidence.
- Phase 17AG runs that controlled post-refresh smoke successfully; the
  immutable workflow snapshot is healthy and source labels are public-safe, but
  the normal CloudFront latest path still needs cache verification before it is
  treated as refreshed.
- Phase 17AH verifies the cache read-only: the immutable Phase 17AG snapshot is
  healthy, while the normal CloudFront latest path still serves the cached
  Phase 17AA snapshot.
- Phase 17AI makes the cache-resolution decision only: a single-path
  CloudFront invalidation for `/dashboard_snapshot_v1.json` is the candidate,
  but it still requires explicit execution approval.
- Phase 17AJ executes the approved single-path invalidation and verifies the
  normal latest and immutable Phase 17AG snapshot paths now match.
- Phase 17AK verifies the hosted demo after cache resolution: app routes,
  latest snapshot, immutable snapshot, source labels, and schedule-disabled
  proof are all healthy.
- Phase 17AL keeps that state manual-only: the managed workflow is proven and
  demo-ready, but schedules stay disabled until a dedicated operating preflight.
- Phase 17AM completes the schedule enablement preflight: the candidate plan is
  a single EventBridge rule state change, but schedule enablement remains no-go
  because the failure notification topic has no evidenced subscriber.
