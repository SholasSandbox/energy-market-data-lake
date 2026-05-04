# Portfolio Summary

## Project

Energy Market Data Lake + News Insight Dashboard

## Problem

Energy market teams need to understand not only what changed in price, demand, exposure, or portfolio risk, but also what external market context may explain that movement. Raw market data and news are usually separate, which makes investigation slower and increases the chance of publishing weak or untraceable insights.

## Solution

This project extends a serverless energy data lake into a local news-aware insight dashboard MVP. It combines energy evidence, curated RSS news summaries, schema validation, deterministic AI-style insight generation, and a React dashboard that reads only approved public snapshot JSON.

The result is a portfolio-ready demonstration of how energy data and external context can be merged safely without allowing malformed or unreviewed AI output into the dashboard.

## Architecture

Implemented baseline:

- EventBridge-scheduled Lambda ingestion for energy data
- S3 raw and curated lakehouse layout
- Glue crawler and ETL pattern
- Athena query layer
- React dashboard
- local evidence generation under `docs/evidence/`

Local MVP extension:

- RSS/news ingestion evidence
- JSON contracts for energy, news, AI insight, and dashboard snapshot outputs
- local AI input bundle
- deterministic AI-style merge
- validation of good samples
- rejection of known-bad samples
- public-safe dashboard snapshot
- dashboard freshness warning for old local demo evidence

Target AWS extension:

- Step Functions orchestration
- Lambda-based news ingestion
- OpenClaw in managed compute or Bedrock `InvokeModel`
- SNS and CloudWatch failure notifications
- CloudFront/S3 static dashboard delivery

## Security And Control Design

- Raw, curated, failed, and audit data remain private.
- The public dashboard reads only `dashboard_snapshot_v1.sample.json`.
- AI output must pass `ai_insight_v1` validation before publishing.
- Bad input and bad AI output are represented with failed evidence samples.
- The local AI merge is deterministic for demo reliability; cloud AI is a future extension.

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
- PR/implementation summary: `docs/pr-description.md`
- Public dashboard snapshot: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`
- AI insight evidence: `docs/evidence/curated/ai_insight_v1.sample.json`

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
- cost-conscious serverless design
- practical frontend dashboard delivery
- clear local-to-cloud migration path

## Known Limitations

- The news + AI path is implemented as a local MVP, not yet deployed as AWS orchestration.
- The current AI merge is deterministic local logic, not live OpenClaw or Bedrock.
- Demo energy evidence may be stale and is labelled as local demo evidence.
- ENTSOG gas remains a target extension for full curated gas analytics.

## Next Steps

1. Refresh live energy data before any public demo.
2. Select a small ENTSOG pointDirection set with `hasData=true`.
3. Implement curated gas parsing and Athena queries.
4. Move the local news + AI pipeline into Step Functions.
5. Add SNS/CloudWatch notifications for validation failures.
6. Publish the dashboard through CloudFront/S3.
