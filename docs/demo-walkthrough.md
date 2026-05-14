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
state.

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
- static dashboard delivery path
- cost-aware AWS architecture

## Known Limitations

- The current AI merge is deterministic logic, not a live OpenClaw or Bedrock
  model call.
- The energy evidence may be stale because it comes from the current local dashboard data.
- Gas metrics are rendered in the React dashboard context, but not in the public AI snapshot contract.
- RSS feed results change over time.
- Terraform is scaffolded for the AWS lakehouse, but existing resources still need to be imported before Terraform manages them.
- Phase 8 schedule automation is intentionally disabled until a later operating
  decision.
- Phase 10 filter controls are currently display/readout controls until a later
  deterministic filtering slice wires the interactions.
