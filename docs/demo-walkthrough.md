# Demo Walkthrough

Purpose: show a serverless energy data lake extended with validated news and AI insight outputs. Keep the demo under 10 minutes.

Current demo state: local MVP evidence is implemented. AWS orchestration for the news + AI extension is target architecture, not yet a live cloud deployment.

## 1. Business Problem

Energy teams need to connect market movement with external context. This project shows how energy data, RSS news, schema validation, and an AI-style merge can produce a public-safe dashboard insight.

Say:

```text
This demo connects energy market facts with curated news context, validates every contract, rejects malformed output, and publishes only approved dashboard JSON.
```

## 2. Architecture Story

Show:

- `diagrams/news-dashboard-high-level.svg`
- `diagrams/news-dashboard-detailed.svg`

Key points:

- Raw, curated, failed, and audit data stay private.
- The public dashboard reads only approved snapshot JSON.
- AI output must pass `ai_insight_v1.schema.json` before publishing.
- OpenClaw or Bedrock is a later cloud/runtime extension; this demo uses deterministic local merge logic.

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

Failure proof:

- `docs/evidence/failed/bad_energy_input_v1.sample.json`
- `docs/evidence/failed/bad_ai_insight_v1.sample.json`

The validator should reject bad files as expected.

## 5. Show Dashboard

Run:

```bash
cd dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

```text
http://127.0.0.1:5173/
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

Show the `AI Insight Snapshot` section:

- market price
- demand
- news article count
- AI insight title and summary
- risk level
- confidence score
- source references
- data-quality checks
- freshness warning for old local demo data

Say:

```text
The dashboard is deliberately reading the public snapshot, not private raw or curated evidence paths.
```

## 6. Close With Controls

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
- static dashboard delivery path
- cost-aware AWS architecture

## Known Limitations

- The current AI merge is deterministic local logic, not a live OpenClaw or Bedrock model call.
- The energy evidence may be stale because it comes from the current local dashboard data.
- RSS feed results change over time.
- AWS resources are represented by implemented repo paths and prior evidence unless a live deploy is run.
