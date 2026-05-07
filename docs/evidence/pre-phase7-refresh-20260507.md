# Pre-Phase 7 Refresh And Validation

- Timestamp (UTC): 2026-05-07T08:55:00Z
- Region: eu-west-2
- Data lake bucket: `energy-market-lake-464975959576-20260405`
- Refresh date used for gas and electricity ingestion: `2026-05-05`
- Terminology: **pre-Phase 7 refresh and validation pass**

## Scope

This was a refresh, not a rebuild.

- Refresh: rerun the existing ingestion, curation, news, AI, and dashboard snapshot flows.
- Validation: prove raw S3, curated Parquet, Glue Catalog, Athena, JSON contracts, and the local dashboard are healthy before changing the dashboard for Phase 7.

## Date Selection

The initial live-check for `2026-05-06` was partial:

```text
BE-TSO-0001ITP-00061entry: Physical Flow pass, Allocation pass
BE-TSO-0001ITP-00115exit: Physical Flow pass, Allocation pass
CZ-TSO-0001ITP-00537entry: Physical Flow pass, Allocation 404
BE-TSO-0001ITP-00555exit: Physical Flow pass, Allocation pass
```

The refresh used `2026-05-05` because all four selected pointDirections passed both `Physical Flow` and `Allocation`.

## Raw Ingestion

Lambda function:

```text
energy-market-elexon-ingest
```

Evidence:

```text
docs/evidence/pre-phase7-lambda-invoke-20260507.json
```

Result:

```text
StatusCode: 200
status: ok
warnings: []
```

Raw S3 counts for `2026-05-05`:

```text
raw/source=elexon/dataset=atl/date=2026-05-05/: 1 payload
raw/source=elexon/dataset=system_prices/date=2026-05-05/: 1 payload
raw/source=entsog/dataset=gas_flow/.../date=2026-05-05/: 4 payloads
raw/source=entsog/dataset=gas_demand/.../date=2026-05-05/: 4 payloads
```

## Curation

Glue job:

```text
energy-market-etl-raw-to-parquet
```

Glue job run:

```text
jr_b2a330f142d63c6c2777477db937df8ee44c0560a6f4618782b4c57d127856d1
```

Result:

```text
State: SUCCEEDED
ExecutionTime: 153 seconds
```

Curated gas count for `2026-05-05`:

```text
curated/dataset=gas/region=eu/date=2026-05-05/: 1 parquet object
```

## Glue Catalog

Curated crawler:

```text
energy-market-curated-crawler
```

Result:

```text
Last crawl status: SUCCEEDED
Target: s3://energy-market-lake-464975959576-20260405/curated/
```

## Athena Validation

Schema evidence:

```text
docs/evidence/pre-phase7-athena-gas-schema-20260507.md
```

Query evidence:

```text
docs/evidence/pre-phase7-athena-gas-query-20260507.json
```

Athena query execution ID:

```text
b4f39512-0789-401e-96d4-9dfbbb6df87e
```

Validation result:

```text
Status: PASS
Source coverage: entsog = 16 rows
Latest source/region date: entsog / eu = 2026-05-05
```

Gas completeness for `2026-05-05`:

```text
BE-TSO-0001ITP-00061entry: 1 flow row, 1 demand row
BE-TSO-0001ITP-00115exit: 1 flow row, 1 demand row
BE-TSO-0001ITP-00555exit: 1 flow row, 1 demand row
CZ-TSO-0001ITP-00537entry: 1 flow row, 1 demand row
```

## News And Dashboard Snapshot

Visible dashboard data was refreshed from Athena before regenerating local evidence:

```text
dashboard-ui/public/dashboard-data.json
latestDate: 2026-05-07
asOf: 2026-05-07 10:03:10 UTC
bucket: energy-market-lake-464975959576-20260405
dataFreshness: Daily snapshot (5/48 settlement rows)
```

The latest electricity date is same-day and partial because the refresh ran during the morning. This is current rather than stale, but it is not a complete 48-settlement-period day.

Local refresh commands completed:

```text
python scripts/generate_dashboard.py --output-json dashboard-ui/public/dashboard-data.json
python scripts/ingest_news_local.py
python scripts/export_energy_input_local.py
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence --check-failures
```

Results:

```text
news_summary_v1.sample.json: 3 articles
energy_input_v1.sample.json: 1 energy record
ai_input_bundle_v1.sample.json: refreshed
ai_insight_v1.sample.json: refreshed
dashboard_snapshot_v1.sample.json: refreshed
contract validation: All contracts are valid
bad evidence samples: rejected as expected
```

Dashboard snapshot:

```text
dashboard-ui/public/dashboard_snapshot_v1.sample.json
generated_at: 2026-05-07T10:03:29Z
latest_date: 2026-05-07
```

## Dashboard Test

Local dev server:

```text
http://127.0.0.1:5173/
```

HTTP checks:

```text
GET /: 200 OK
GET /dashboard-data.json: 200 OK
GET /dashboard_snapshot_v1.sample.json: 200 OK
```

React production build:

```text
npm run build: passed
```

Vite emitted deprecation warnings for plugin configuration, but the dev server and production build both completed successfully.

## Outcome

Pre-Phase 7 refresh and validation passed.

The project is ready to begin Phase 7 dashboard gas follow-on work from a known-good baseline.
