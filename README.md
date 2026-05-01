# Energy Market Data Lake + News Insight Dashboard

A budget-conscious AWS portfolio project for ingesting energy market data, transforming it into a queryable lakehouse, and presenting decision-ready dashboard outputs. The current implementation is a serverless energy data lake using Lambda, S3, Glue, Athena, EventBridge, and a React dashboard scaffold. The next planned extension adds news summaries and an AI merge step so energy movements can be connected to external market context.

Region: **eu-west-2 (London)**

## Project Scope

### Implemented Today

- Scheduled ingestion path using EventBridge and Lambda.
- Raw S3 landing zone for energy market payloads.
- Glue crawler and Glue ETL pattern for raw-to-curated transformation.
- Athena query layer over curated Parquet data.
- Evidence generation under `docs/evidence/`.
- HTML dashboard generation from Athena-backed data.
- React + TypeScript dashboard scaffold under `dashboard-ui/`.

### Planned Target Extension

- RSS/news ingestion for energy-market-relevant articles.
- Curated news summaries with source references.
- AI merge path using local OpenClaw first, with Bedrock or managed compute as an optional later cloud path.
- Strict JSON schema validation before dashboard publishing.
- Public-safe dashboard snapshot JSON.
- Trust-boundary-aware architecture with private raw/curated/audit/failed zones and public dashboard-only output.

## Current Data Scope

- **UK electricity (Elexon)**: demand by bidding zone (GSP proxy) and system prices (SBP/SSP).
- **EU electricity (ENTSO-E)**: actual load and day-ahead prices for GB, FR, DE-LU, and NL.
- **EU gas (ENTSOG)**: target extension for physical flows and demand proxy using selected pointDirection IDs.
- **News summaries**: target extension for RSS/news context linked to energy market movements.

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

## Target News + AI Architecture

```text
Energy APIs + RSS Feeds
        |
        v
Private AWS Processing Boundary
  EventBridge -> Lambda ingest -> S3 raw/
                         |
                         v
                  validate + normalize
                         |
                         v
                    S3 curated/
                         |
                         v
Local OpenClaw MVP or optional Bedrock/managed compute
                         |
                         v
             validate ai_insight_v1.json
                         |
              +----------+----------+
              |                     |
            valid                invalid
              |                     |
              v                     v
 public dashboard JSON        S3 failed/ + alert
```

The public dashboard must never read directly from raw, curated, audit, or failed lake data.

## Repository Layout

```text
athena/                Athena demo queries
config/                Sample environment settings
dashboard-ui/          React + TypeScript dashboard scaffold
diagrams/              Mermaid, SVG, PNG, and generated architecture diagrams
docs/                  Active documentation and implementation plans
docs/archive/          Historical completed plans and old demo artifacts
docs/evidence/         Generated run, schema, and dashboard evidence
glue/                  Glue ETL code
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

Generate and validate local news summary evidence:

```bash
python scripts/ingest_news_local.py
python scripts/validate_contracts.py --include-evidence
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
  --output-json dashboard-ui/public/dashboard-data.json
```

Run the React dashboard locally:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui
npm install
npm run dev
```

Find ENTSOG pointDirection IDs:

```bash
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL --ids-only
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL --save-env
```

## Active Documentation

- `PLANS.md`: current delivery sequence and implementation guardrails.
- `docs/setup.md`: setup guide for the serverless energy lakehouse path.
- `docs/phase-1-stabilize-ingestion-lakehouse.md`: active stabilization checklist.
- `docs/entsoe-operationalization-checklist.md`: ENTSO-E reliability checklist.
- `docs/gas-implementation-checklist.md`: ENTSOG gas implementation checklist.
- `docs/dashboard-ia-spec.md`: React dashboard redesign direction.
- `docs/four-week-project-plan.md`: delivery plan for the energy + news insight MVP.
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

1. Stabilize the existing ingestion and lakehouse flow.
2. Operationalize ENTSO-E electricity more reliably.
3. Implement ENTSOG gas end-to-end.
4. Expand the React dashboard after the data layer is trustworthy.
5. Add news summaries and AI insight merging using the 4-week plan.

## Notes

- Elexon base URL: `https://data.elexon.co.uk/bmrs/api/v1` (no API key).
- ENTSO-E requires registration and an API token stored in SSM or Secrets Manager.
- ENTSOG is public; choose pointDirection IDs and indicators before running.
- OpenClaw/local model execution is outside AWS unless moved into Bedrock or managed compute.
