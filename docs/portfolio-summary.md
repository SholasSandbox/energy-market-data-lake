# Portfolio Summary

<!-- markdownlint-disable MD013 -->

## Project

Energy Market Data Lake + News Insight Dashboard

## Problem

Energy market teams need to understand not only what changed in price, demand, exposure, or portfolio risk, but also what external market context may explain that movement. Raw market data and news are usually separate, which makes investigation slower and increases the chance of publishing weak or untraceable insights.

## Solution

This project extends a serverless energy data lake into a news-aware insight
dashboard MVP. It combines electricity and gas market evidence, curated RSS
news summaries, schema validation, deterministic AI-style insight generation,
AWS Step Functions orchestration, and a React dashboard that reads only
approved public snapshot JSON.

The result is a portfolio-ready demonstration of how energy data and external
context can be merged safely without allowing malformed or unreviewed AI output
into the dashboard. The ENTSOG gas slice now has live
raw-to-curated-to-Athena evidence; the news + AI slice now has a live AWS
orchestration proof with manual Step Functions execution, S3 artifacts,
validation gates, failed-run quarantine, and previous-snapshot preservation.

## Architecture

Implemented baseline:

- EventBridge-scheduled Lambda ingestion for energy data
- S3 raw and curated lakehouse layout
- Glue crawler and ETL pattern
- Athena query layer
- ENTSOG gas ingestion, curated Parquet, Glue Catalog table, Athena query, and validation evidence
- ENTSOG gas context cards and pointDirection table in the React dashboard
- React dashboard
- local evidence generation under `docs/evidence/`

News + AI extension:

- RSS/news ingestion evidence
- public-safe market-news article grid for gas and electricity context
- JSON contracts for energy, news, AI insight, and dashboard snapshot outputs
- local AI input bundle
- deterministic AI-style merge
- validation of good samples
- rejection of known-bad samples
- public-safe dashboard snapshot
- dashboard freshness warning for old local demo evidence
- AWS Lambda handler for deterministic news/AI orchestration
- AWS Step Functions state machine for manual orchestration
- S3 curated, failed, audit, and dashboard snapshot artifact paths
- SNS and CloudWatch failure observability

Managed AWS state:

- Bedrock `InvokeModel` through the Lambda adapter, with Step Functions
  orchestration and deterministic fallback
- controlled EventBridge scheduling
- CloudFront/S3 static dashboard delivery
- SNS failure notification and an AWS Budget guardrail

## Security And Control Design

- Raw, curated, failed, and audit data remain private.
- The public dashboard reads only approved snapshot JSON.
- AI output must pass `ai_insight_v1` validation before publishing.
- Bad input and bad AI output are represented with failed evidence samples.
- The AWS orchestration publishes the dashboard snapshot only after validation
  passes.
- The deterministic AI merge is deliberate for demo reliability; model
  invocation is a future extension.

## Cost-Aware Design

- Scheduled/serverless ingestion first.
- Static dashboard delivery path.
- No NAT Gateway for the MVP.
- No RDS.
- No always-on EC2.
- S3 lifecycle and Parquet partitioning remain the intended lakehouse cost controls.

## Demo Evidence

- Walkthrough: `docs/demo-walkthrough.md`
- Screenshot: `docs/evidence/screenshots/dashboard-week4-local-mvp.png`
- Tabbed dashboard screenshots:
  - `docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png`
  - `docs/evidence/screenshots/dashboard-power-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png`
- PR/implementation summary: `docs/pr-description.md`
- Public dashboard snapshot: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`
- Expanded news refresh evidence: `docs/evidence/news-refresh-expanded-20260507.md`
- AI insight evidence: `docs/evidence/curated/ai_insight_v1.sample.json`
- ENTSOG gas run evidence: `docs/evidence/run-entsog-gas-20260506.md`
- ENTSOG gas Athena validation: `docs/evidence/athena-gas-schema-20260506.md`
- ENTSOG gas query summary: `docs/evidence/athena-gas-query-summary-20260506.md`
- ENTSOG gas dashboard evidence: `docs/evidence/phase7-dashboard-gas-20260507.md`
- ENTSOG gas 7-day trend evidence: `docs/evidence/gas-7day-trend-20260507.md`
- Phase 8 AWS execution evidence: `docs/evidence/phase8-aws-live-execution-20260511.md`
- Phase 8 operational runbook: `docs/phase-8-operational-runbook.md`

Run the local evidence pipeline:

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

Start the dashboard:

```bash
cd dashboard-ui
npm run dev -- --host 127.0.0.1
```

Open:

```text
http://127.0.0.1:5173/
```

## What This Demonstrates

- AWS data-lake architecture thinking
- trust boundaries between private processing and public dashboard delivery
- schema-first data contracts
- controlled AI output
- failure handling and evidence
- live AWS Step Functions orchestration proof
- cost-conscious serverless design
- end-to-end gas lakehouse proof from selected ENTSOG pointDirections to Athena validation
- practical frontend dashboard delivery
- clear local-to-cloud migration path

## Known Limitations

- The current managed path invokes Bedrock/Mistral through a Lambda adapter;
  deterministic logic remains the fallback and comparison baseline.
- The AI workflow schedule is enabled and has verified scheduled-run evidence
  through Phase 17AU.
- Demo energy evidence may be stale and is labelled as local demo evidence.
- ENTSOG gas is rendered in the React dashboard context, but not added to the public AI snapshot contract.
- Terraform has been scaffolded and locally validated, but existing AWS resources still need to be imported into Terraform state before Terraform should manage them.

## Next Steps

1. Import the existing AWS resources into Terraform state, then review `terraform plan`.
2. Refresh live electricity and gas evidence before any public demo.
3. Decide whether gas should later be included in the public AI snapshot contract.
4. Create the ADR 0006 P1 evaluation contract before selecting retrieval,
   models, or an optional orchestration framework.
5. Keep OpenClaw/ECS rejected and LangGraph deferred under ADR 0007 unless a
   measured requirement triggers a new decision.
6. Define any additional production identity, tenancy, SLO, or alarm needs
   from real usage rather than portfolio scope.
