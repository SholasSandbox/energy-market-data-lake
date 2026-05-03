# Setup Guide (eu-west-2)

This is a lightweight, budget-conscious setup. Use the default settings unless
you need additional scale.

## Current Demo Scope

The local MVP is implemented and is the recommended path for demos and learning.
It proves:

- energy input evidence
- curated RSS/news evidence
- AI input bundle evidence
- deterministic local AI insight output
- schema validation for good samples
- rejection of known-bad samples
- public-safe dashboard snapshot JSON
- React dashboard display from `dashboard_snapshot_v1.sample.json`

The AWS/serverless extension remains the target deployment path for Step
Functions, CloudFront, SNS alerts, and a managed OpenClaw or Bedrock runtime.

## AWS Closeout Script (Optional)

If you have AWS credentials configured and want to run the existing serverless
energy lakehouse closeout flow:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
```

This script provisions S3 + Lambda + EventBridge + Glue + crawlers and stores
run evidence in `docs/evidence/`. It is separate from the local news + AI MVP
pipeline below.

## Local Python Setup

Use a virtual environment for local helper scripts. This keeps project
dependencies separate from your Mac's global Python installation.

Start here after restarting your terminal:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

If `.venv` already exists, you only need:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
source .venv/bin/activate
```

Validate the JSON schema contracts:

```bash
python scripts/validate_contracts.py
```

## Local MVP Fast Path

Run this from the repo root when resuming work or preparing the demo:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake
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

This command sequence creates or refreshes:

- `docs/evidence/energy_input_v1.sample.json`
- `docs/evidence/curated/news_summary_v1.sample.json`
- `docs/evidence/ai/ai_input_bundle_v1.sample.json`
- `docs/evidence/curated/ai_insight_v1.sample.json`
- `dashboard-ui/public/dashboard_snapshot_v1.sample.json`

It also confirms these known-bad files are rejected:

- `docs/evidence/failed/bad_energy_input_v1.sample.json`
- `docs/evidence/failed/bad_ai_insight_v1.sample.json`

Generate and validate the curated local news summary evidence:

```bash
python scripts/ingest_news_local.py
python scripts/validate_contracts.py --include-evidence
```

Generate and validate the local energy input evidence:

```bash
python scripts/export_energy_input_local.py
python scripts/validate_contracts.py --include-evidence
```

Publish a public-safe local dashboard snapshot:

```bash
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence
```

Create and validate local AI merge evidence:

```bash
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/validate_contracts.py --include-evidence
```

Confirm known-bad samples are rejected:

```bash
python scripts/validate_contracts.py --include-evidence --check-failures
```

Expected result:

```text
All contracts are valid.
```

## React Dashboard Setup

Start the dashboard from a second terminal:

```bash
cd /Users/[redacted-user]/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

```text
http://127.0.0.1:5173/
```

Verify the dashboard and public snapshot JSON are served:

```bash
curl -I http://127.0.0.1:5173/
curl -I http://127.0.0.1:5173/dashboard_snapshot_v1.sample.json
```

Expected result for both:

```text
HTTP/1.1 200 OK
```

The dashboard should read only `dashboard_snapshot_v1.sample.json`, not raw,
curated, failed, or audit evidence directly.

## 1) S3 Buckets

Create one bucket:

- `energy-market-lake-<your-unique-suffix>`

Enable:

- Block public access
- Server-side encryption (SSE-S3 or SSE-KMS)

## 2) S3 Lifecycle Policies

Add a lifecycle rule for the `raw/` prefix:

- Transition to Standard-IA after 30 days
- Transition to Glacier Flexible after 90 days
- Optional: Expire after 180 days

Keep `curated/` in Standard or Intelligent-Tiering if queried often.

## 3) IAM Roles

Create roles with least privilege:

- Lambda role: write to `s3://.../raw/*`, read secrets, write logs
- Glue role: read raw, write curated, read catalog
- Athena: default workgroup + query result location in S3

## 4) Lambda Ingestion (App 1)

Create a Lambda function (Python 3.x):

- Source: Elexon API (no key)
- Output: JSON in S3 `raw/` prefix
- Targets for this phase:
  - Demand by bidding zone (GSP proxy): `/datasets/ATL`
  - System prices (includes SBP/SSP imbalance prices): `/balancing/settlement/system-prices/{settlementDate}`

Required Lambda env vars:

- `S3_BUCKET`
- `ELEXON_BASE_URL` (default: `https://data.elexon.co.uk/bmrs/api/v1`)
- `BACKFILL_DAYS` (default: `30`)

Optional (future):

- `ENTSOE_BASE_URL` (default: `https://web-api.tp.entsoe.eu/api`)
- `ENTSOE_TOKEN` (stored in SSM/Secrets Manager)
- `ENTSOE_ZONES` (default: `GB,FR,DE,NL`)
- `ENTSOG_BASE_URL` (default: `https://transparency.entsog.eu/api/v1`)
- `ENTSOG_POINT_DIRECTIONS` (comma-separated pointDirection IDs)
- `ENTSOG_FLOW_INDICATOR` (default: `Physical Flow`)
- `ENTSOG_DEMAND_INDICATOR` (default: `Allocation`)
- `ENTSOG_PERIOD_TYPE` (default: `day`)
- `ENTSOG_TIMEZONE` (default: `WET`)
- `ENTSOG_LIMIT` (default: `1000`)

Schedule with EventBridge (daily at 02:00 UTC).

Code reference:

- Lambda handler: `lambda/ingest_elexon.py`
- S3 output keys:
  - `raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json`
  - `raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json`
  - `raw/source=entsoe/dataset=actual_load/zone=<zone>/date=YYYY-MM-DD/payload.xml`
  - `raw/source=entsoe/dataset=day_ahead_prices/zone=<zone>/date=YYYY-MM-DD/payload.xml`
  - `raw/source=entsog/dataset=gas_flow/point_direction=<id>/date=YYYY-MM-DD/payload.json`
  - `raw/source=entsog/dataset=gas_demand/point_direction=<id>/date=YYYY-MM-DD/payload.json`

ENTSO-E zone mapping (default):

- GB: `10YGB----------A`
- FR: `10YFR-RTE------C`
- DE (DE-LU bidding zone): `10Y1001A1001A82H`
- NL: `10YNL----------L`

ENTSO-E datasets:

- Actual load: `documentType=A65`, `processType=A16`, `outBiddingZone_Domain=<EIC>`
- Day-ahead prices: `documentType=A44`, `in_Domain=<EIC>`, `out_Domain=<EIC>`

ENTSOG pointDirection setup:

1) Query `operatorpointdirections` to list available IDs.  
2) Filter to the countries you want (GB, FR, DE, NL).  
3) Copy the `pointDirection` IDs into `ENTSOG_POINT_DIRECTIONS`.

Helper script:

```bash
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL
```

IDs-only output (ready to paste into `ENTSOG_POINT_DIRECTIONS`):

```bash
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL --ids-only
```

Write into `config/sample.env`:

```bash
python scripts/entsog_point_directions.py --countries GB,FR,DE,NL --save-env
```

Local test (optional):

```bash
python scripts/run_ingestion_local.py \
  --bucket energy-market-lake-<your-unique-suffix> \
  --backfill-days 2
```

EventBridge test payload (optional):

```json
{
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "detail": {},
  "region": "eu-west-2",
  "time": "2026-01-30T02:00:00Z"
}
```

Expected Lambda output (example):

```json
{
  "status": "ok",
  "s3_keys": [
    "raw/source=elexon/dataset=atl/date=2026-01-30/payload.json",
    "raw/source=elexon/dataset=system_prices/date=2026-01-30/payload.json",
    "raw/source=elexon/dataset=atl/date=2026-01-29/payload.json",
    "raw/source=elexon/dataset=system_prices/date=2026-01-29/payload.json"
  ]
}
```

Post-run S3 checklist:

- `raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json` exists
- `raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json` exists
- File size > 0 bytes for both datasets

## 5) Glue Crawler + ETL (App 2)

- Create a Glue Crawler on the `raw/` prefix
- Run crawler daily (or after ingestion)
- Create Glue ETL job to convert raw -> Parquet

Partition by:

- `source`
- `region`
- `date`

Write to `curated/` prefix.

## 6) Athena

- Set query result location to `s3://.../athena-results/`
- Create tables from Glue Data Catalog
- Run demo queries from `athena/queries.sql`

## 7) Cost Guardrails

- Keep backfill to 30-90 days
- Use Parquet + partitions
- Run Glue jobs only when needed
