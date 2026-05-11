# Energy Market Data Lake + News Insight Dashboard

<!-- markdownlint-disable MD013 -->

A budget-conscious AWS portfolio project for ingesting energy market data,
transforming it into a queryable lakehouse, and presenting decision-ready
dashboard outputs. The implemented baseline is a serverless energy data lake
using Lambda, S3, Glue, Athena, EventBridge, and a React dashboard. The news +
AI extension adds RSS summaries, strict JSON contracts, a deterministic
AI-style merge, failure samples, a public-safe dashboard snapshot, and a
live-proven AWS Step Functions orchestration path.

Region: **eu-west-2 (London)**

## Project Scope

### Implemented Today

- Scheduled ingestion path using EventBridge and Lambda.
- Raw S3 landing zone for energy market payloads.
- Glue crawler and Glue ETL pattern for raw-to-curated transformation.
- Athena query layer over curated Parquet data.
- Evidence generation under `docs/evidence/`.
- HTML dashboard generation from Athena-backed data.
- React + TypeScript dashboard under `dashboard-ui/`.
- ENTSOG gas proof from raw ingestion through curated Parquet, Glue Catalog, Athena query, and validation evidence.
- ENTSOG gas context cards and selected pointDirection table in the React dashboard.
- Local RSS/news ingestion evidence.
- Expanded public-safe market-news article grid for gas and electricity context.
- JSON schema contracts for energy, news, AI insight, and dashboard snapshot outputs.
- Local AI input bundle and deterministic AI insight merge.
- Validator checks for good evidence and intentionally bad failure samples.
- Public-safe dashboard snapshot loaded by the React app.
- Visible data freshness warning for old local demo evidence.
- Phase 8 AWS AI insight orchestration with Lambda, Step Functions, S3
  artifacts, validation gates, failed-run quarantine, and manual execution
  evidence.

### Deferred AWS Extension

- Run OpenClaw in a clear runtime, or use Bedrock `InvokeModel` as the managed cloud AI path.
- Publish dashboard snapshot JSON to a CloudFront-fronted static site bucket.
- Enable the Phase 8 EventBridge schedule after another operating decision.
- Add CloudWatch alarms after the manual workflow has settled.
- Trust-boundary-aware architecture with private raw/curated/audit/failed zones and public dashboard-only output.

## Current Data Scope

- **UK electricity (Elexon)**: demand by bidding zone (GSP proxy) and system prices (SBP/SSP).
- **EU electricity (ENTSO-E)**: actual load and day-ahead prices for GB, FR, DE-LU, and NL.
- **EU gas (ENTSOG)**: raw and curated physical flows plus allocation-based demand proxy using selected pointDirection IDs.
- **News summaries**: local RSS evidence linked to energy market movements.

## Current Architecture

```text
External Energy APIs
  |-- Elexon
  |-- ENTSO-E
  `-- ENTSOG
        |
        v
EventBridge Scheduler
        |
        v
Lambda Ingestion
        |
        v
S3 Raw Zone
        |
        v
Glue Crawler + Glue ETL
        |
        v
S3 Curated Zone
        |
        v
Athena
        |
        v
Dashboard JSON / HTML / React Dashboard
```

## News + AI Orchestration Architecture

```text
Energy dashboard input + RSS Feeds
        |
        v
Private AWS Processing Boundary
  Manual Step Functions execution
        |
        v
  Lambda deterministic orchestration
        |
        v
  validate + write run-scoped S3 artifacts
        |
        v
  validate ai_insight_v1 and dashboard_snapshot_v1
        |
        +------------------+
        |                  |
      valid             invalid
        |                  |
        v                  v
 dashboard bucket     S3 failed/ + SNS + CloudWatch
```

The public dashboard must never read directly from raw, curated, audit, or failed lake data.

EventBridge scheduling is deployed but intentionally disabled. Manual Step
Functions execution is the current safe operating mode.

## Local MVP Flow

```text
Local energy evidence + RSS feeds
        |
        v
validated energy_input_v1 + curated news_summary_v1
        |
        v
AI input bundle
        |
        v
deterministic local AI merge
        |
        v
validated ai_insight_v1
        |
        v
public dashboard_snapshot_v1.sample.json
        |
        v
React dashboard
```

## Demo Evidence

Use these artifacts to review or present the local MVP:

- Walkthrough: `docs/demo-walkthrough.md`
- Screenshot: `docs/evidence/screenshots/dashboard-week4-local-mvp.png`
- Tabbed dashboard screenshots:
  - `docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png`
  - `docs/evidence/screenshots/dashboard-power-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png`
- Public dashboard snapshot: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`
- Expanded news refresh evidence: `docs/evidence/news-refresh-expanded-20260507.md`
- Curated AI insight evidence: `docs/evidence/curated/ai_insight_v1.sample.json`
- ENTSOG gas Phase 5 evidence: `docs/evidence/run-entsog-gas-20260506.md`
- ENTSOG Athena validation: `docs/evidence/athena-gas-schema-20260506.md`
- ENTSOG Athena query summary: `docs/evidence/athena-gas-query-summary-20260506.md`
- ENTSOG dashboard gas evidence: `docs/evidence/phase7-dashboard-gas-20260507.md`
- ENTSOG 7-day gas trend evidence: `docs/evidence/gas-7day-trend-20260507.md`
- Phase 8 AWS live execution evidence: `docs/evidence/phase8-aws-live-execution-20260511.md`
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

## Repository Layout

```text
athena/                Athena demo queries
config/                Sample environment settings
dashboard-ui/          React + TypeScript dashboard scaffold
diagrams/              Mermaid, SVG, PNG, and generated architecture diagrams
docs/                  Active documentation and implementation plans
docs/archive/          Historical completed plans and old demo artifacts
docs/evidence/         Generated run, schema, and dashboard evidence
docs/evidence/screenshots/
                       Dashboard screenshots for portfolio/demo use
glue/                  Glue ETL code
infra/terraform/       Terraform Infrastructure as Code for AWS lakehouse resources
lambda/                Lambda ingestion code
scripts/               Local/demo helper scripts
```

## S3 Layout

Current and target storage layout:

```text
s3://<bucket>/
  raw/
    source=elexon/
      dataset=atl/
        date=YYYY-MM-DD/
      dataset=system_prices/
        date=YYYY-MM-DD/
    source=entsoe/
      dataset=actual_load/
        zone=gb|fr|de|nl/
        date=YYYY-MM-DD/
      dataset=day_ahead_prices/
        zone=gb|fr|de|nl/
        date=YYYY-MM-DD/
    source=entsog/
      dataset=gas_flow/
        point_direction=<id>/
        date=YYYY-MM-DD/
      dataset=gas_demand/
        point_direction=<id>/
        date=YYYY-MM-DD/
    source=news/
      dataset=rss_summary/
        date=YYYY-MM-DD/
  curated/
    dataset=electricity/
      source=elexon|entsoe/
      region=gb|fr|de|nl/
      date=YYYY-MM-DD/
    dataset=gas/
      region=eu/
      date=YYYY-MM-DD/
    dataset=news/
      date=YYYY-MM-DD/
  audit/
  failed/
  archive/
```

## Cost Controls

- Keep ingestion scheduled rather than always on.
- Use S3 lifecycle rules for raw data.
- Store curated data as partitioned Parquet to reduce Athena scan costs.
- Run Glue jobs daily, weekly, or manually for demo needs.
- Keep Lambda payloads and runtimes small.
- Avoid NAT Gateway, RDS, and always-on EC2 for the MVP.
- Add AWS Budget alerts before any live demo period.

## Local And Demo Commands

Set up the local Python helper environment:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

Validate the JSON schema contracts:

```bash
python scripts/validate_contracts.py
```

Run the local news + energy + AI insight pipeline:

```bash
export AWS_REGION=eu-west-2
export S3_BUCKET=energy-market-lake-464975959576-20260405
python scripts/generate_dashboard.py \
  --region "${AWS_REGION}" \
  --bucket "${S3_BUCKET}" \
  --output-location "s3://${S3_BUCKET}/athena-results/" \
  --output-json dashboard-ui/public/dashboard-data.json
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

Run the full demo closeout flow:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
```

Generate a polished HTML dashboard from Athena curated data:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 scripts/generate_dashboard.py
```

Generate JSON for the React app:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 scripts/generate_dashboard.py \
  --bucket energy-market-lake-464975959576-20260405 \
  --output-json dashboard-ui/public/dashboard-data.json
```

Run the React dashboard locally:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Verify the app and public snapshot are served:

```bash
curl -I http://127.0.0.1:5173/
curl -I http://127.0.0.1:5173/dashboard_snapshot_v1.sample.json
```

Run a manual Phase 8 AWS orchestration proof:

```bash
export AWS_REGION=eu-west-2
export AI_ORCHESTRATION_STATE_MACHINE_ARN="arn:aws:states:eu-west-2:464975959576:stateMachine:energy-market-ai-insight-orchestration"
export EXECUTION_NAME="phase8-manual-$(date -u +%Y%m%dT%H%M%SZ)"

aws stepfunctions start-execution \
  --state-machine-arn "${AI_ORCHESTRATION_STATE_MACHINE_ARN}" \
  --name "${EXECUTION_NAME}" \
  --input '{}' \
  --region "${AWS_REGION}" \
  --query executionArn \
  --output text
```

The full operational runbook, including preflight, artifact verification, and
failure drill commands, lives in `docs/phase-8-operational-runbook.md`.

Find ENTSOG pointDirection IDs:

```bash
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --ids-only
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --ids-only --max-results 4
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --save-env
```

Seed set validated on 2026-05-03 for both `Physical Flow` and `Allocation`:

```text
BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
```

ENTSOG uses `UK` in operator-point metadata; the helper treats requested `GB` as `GB,UK` for gas selection.

Live-check a seed set:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit" \
  --date 2026-05-03
```

Validate the curated gas Athena table:

```bash
python3 scripts/validate_athena_schema.py \
  --region eu-west-2 \
  --database energy_market_lake \
  --table curated_dataset_gas \
  --output-location s3://energy-market-lake-464975959576-20260405/athena-results/ \
  --expected-sources entsog \
  --output-file docs/evidence/athena-gas-schema-$(date +%Y%m%d).md
```

## Active Documentation

- `PLANS.md`: current delivery sequence and implementation guardrails.
- `docs/setup.md`: setup guide for the serverless energy lakehouse path.
- `docs/phase-1-stabilize-ingestion-lakehouse.md`: active stabilization checklist.
- `docs/entsoe-operationalization-checklist.md`: ENTSO-E reliability checklist.
- `docs/gas-implementation-checklist.md`: ENTSOG gas implementation checklist.
- `docs/entsog-gas-build-plan.md`: time-budgeted ENTSOG gas build tracker.
- `infra/terraform/lakehouse/README.md`: Terraform rebuild path with S3 remote backend and optional data bucket creation.
- `docs/dashboard-ia-spec.md`: React dashboard redesign direction.
- `docs/four-week-project-plan.md`: delivery plan for the energy + news insight MVP.
- `docs/phase-8-aws-ai-insight-orchestration.md`: active plan and checklist for
  AWS AI insight orchestration.
- `docs/phase-8-operational-runbook.md`: manual run, proof, failure drill, and
  demo commands for Phase 8.
- `docs/demo-walkthrough.md`: concise demo script for the local MVP and live
  orchestration story.
- `docs/news-dashboard-merged-execution-model.md`: 4-week news + AI + dashboard expansion plan.

## Diagrams

- `diagrams/architecture.mmd`: compact current architecture.
- `diagrams/architecture_overview.png`: rendered AWS overview diagram.
- `diagrams/flow_diagram.png`: older data-flow diagram; useful as reference, but lower priority than current plans.
- `diagrams/news-dashboard-high-level.mmd`: high-level target diagram for news + dashboard.
- `diagrams/news-dashboard-high-level.svg`: rendered high-level target diagram.
- `diagrams/news-dashboard-detailed.mmd`: detailed target diagram with trust boundaries and failure paths.
- `diagrams/news-dashboard-detailed.svg`: rendered detailed target diagram.

## Archived Documentation

Older completed plans and demo artifacts have been moved to `docs/archive/`:

- `closeout-summary.md`
- `dashboard-wireframe-overview.html`
- `demo-checklist.md`
- `project-plan.md`

These are historical references, not the current delivery path.

## Current Delivery Priorities

1. Keep the local pipeline reproducible with schema validation and failure checks.
2. Keep the React dashboard focused on approved snapshot JSON.
3. Operationalize ENTSO-E electricity more reliably.
4. Keep Phase 8 manual orchestration proof reproducible and schedule-disabled.
5. Import or recreate the AWS lakehouse resources through Terraform after reviewing the import plan.
6. Decide whether gas metrics should enter the public AI snapshot contract after the orchestration boundary is proven.

## Notes

- Elexon base URL: `https://data.elexon.co.uk/bmrs/api/v1` (no API key).
- ENTSO-E requires registration and an API token stored in SSM or Secrets Manager.
- ENTSOG is public; the current gas proof uses a four-point seed and the `Physical Flow` plus `Allocation` indicators.
- OpenClaw/local model execution is outside AWS unless moved into Bedrock or managed compute.
- Phase 8 currently proves orchestration, validation, and publish controls with
  deterministic logic; model invocation remains deferred.
