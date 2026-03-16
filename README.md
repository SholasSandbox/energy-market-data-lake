# Energy Market Data Lake (UK + EU) - Cost-Optimized

A small, budget-friendly data lake that ingests **UK electricity data (Elexon)**
plus **EU electricity and gas (ENTSO-E / ENTSOG)**, stores raw data in S3 with
lifecycle policies, transforms to Parquet with Glue, and queries with Athena.

Region: **eu-west-2 (London)**

## Goals

- Practice SAA-C03 core services: **S3, Lambda, Glue, Athena, EventBridge, IAM**
- Demonstrate **cost-optimized storage** (lifecycle + Parquet + partitions)
- Create a **portfolio-ready** project with README + diagram + demo queries

## Data Scope

- **UK electricity (Elexon)**: demand by bidding zone (GSP proxy) +
  system prices (SBP/SSP)
- **EU electricity (ENTSO-E)**: actual load + day-ahead prices (GB, FR, DE-LU, NL)
- **EU gas (ENTSOG)**: physical flows + demand proxy (allocation) for selected points
- **Backfill**: short (30 days, half-hourly)

## Architecture (Two Apps)

### App 1 - Ingestion (Lambda)

- EventBridge schedule triggers Lambda
- Pulls data from Elexon + ENTSO-E + ENTSOG
- Writes raw files to S3

### App 2 - Lakehouse (Glue + Athena)

- Glue Crawler catalogs raw data
- Glue ETL job converts raw -> Parquet in curated zone
- Athena queries curated data

```text
Users -> Athena -> S3 (curated parquet)
                  ^
                  | Glue ETL
S3 (raw) <--------+
  ^
  | Lambda ingestion (EventBridge schedule)
  +-- Elexon API (UK)
  +-- ENTSO-E API (EU electricity)
  +-- ENTSOG API (EU gas)
```

## S3 Layout

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
  curated/
    dataset=electricity/
      region=gb|fr|de|nl/
      date=YYYY-MM-DD/
    dataset=gas/
      region=eu/
      date=YYYY-MM-DD/
  archive/
```

## Cost Controls (Target < $10/month)

- **S3 Lifecycle**: raw -> IA -> Glacier (short backfill only)
- **Parquet + partitioning** to reduce Athena scan costs
- Glue jobs run **daily or weekly** only
- Lambda uses free tier (small payloads)

## Setup Guide

See `docs/setup.md` for step-by-step build instructions.

## One-Command Closeout (Demo Ready)

Run this from the project folder:

```bash
cd /home/shola/cert-revision/energy-market-data-lake
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
```

What it does:

- creates/configures S3 bucket + lifecycle + encryption + block public access
- deploys ingestion Lambda + EventBridge schedule
- invokes ingestion to load raw Elexon ATL + system prices
- creates Glue database/crawlers
- runs Glue ETL job (`raw -> curated parquet`)
- writes run evidence into `docs/evidence/`

## Demo Status (Completed on 2026-03-06)

- Bucket: `energy-market-lake-464975959576-20260306`
- Glue database: `energy_market_lake`
- Raw tables: `raw_dataset_atl`, `raw_dataset_system_prices`
- Curated table: `curated_dataset_electricity`
- Evidence files:
  - `docs/evidence/run-20260306-231751.md`
  - `docs/evidence/run-summary-20260306-232202.md`
  - `docs/evidence/athena-run-20260306-231854.md`

## Demo Queries

See `athena/queries.sql` for sample Athena queries and expected outputs.

## Professional Dashboard

Generate a polished HTML dashboard from Athena curated data:

```bash
cd /home/shola/cert-revision/energy-market-data-lake
python3 scripts/generate_dashboard.py
```

The script writes an output file under `docs/evidence/dashboard-*.html`.
Open the generated file in your browser to present your demo visually.

Dashboard tabs:

- `Market Analytics`: demand, prices, settlement completeness, intraday profile.
- `NorthGrid Utilities (Fictional)`: electricity positions, hedge coverage, and book-level profit/loss view.

For the next iteration, see the redesign blueprint in `docs/dashboard-ia-spec.md`.

React + TypeScript scaffold:

```bash
cd /home/shola/cert-revision/energy-market-data-lake/dashboard-ui
npm install
npm run dev
```

Generate JSON for the React app from Athena-backed dashboard data:

```bash
cd /home/shola/cert-revision/energy-market-data-lake
python3 scripts/generate_dashboard.py \
  --output-json /home/shola/cert-revision/energy-market-data-lake/dashboard-ui/public/dashboard-data.json
```

## Diagram

- Mermaid diagram in `diagrams/architecture.mmd`
- PNG export optional

## Notes

- Elexon base URL: `https://data.elexon.co.uk/bmrs/api/v1` (no API key)
- ENTSO-E requires registration + API token (stored in SSM or Secrets Manager)
- ENTSOG is public; choose pointDirection IDs and indicators before running
