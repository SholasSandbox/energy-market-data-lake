# ENTSOG Gas Build Plan

Use this plan to monitor the ENTSOG gas implementation from selected live point directions through raw ingestion, curated Parquet, Athena validation, evidence, and documentation.

## Summary

- Branch: `feature/entsog-gas-implementation`
- Estimated build time: 18-28 hours
- Recommended schedule: 3-5 focused working days
- Current implementation state: code path is scaffolded; live AWS raw, Glue, crawler, Athena, and evidence proof still need to be run.

## Seed Point Directions

Initial validated seed set:

```text
BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
```

These were live-checked for `Physical Flow` and `Allocation` on `2026-05-03`.

## Progress Tracker

### Phase 1: ENTSOG Selection And Runtime Config

Budget: 2-3 hours

- [x] Confirm final seed `ENTSOG_POINT_DIRECTIONS`.
- [x] Decide whether to keep the current 4-point seed or expand it.
- [x] Confirm `Physical Flow` and `Allocation` indicators.
- [x] Stage runtime env vars locally for Lambda/deploy flow.
- [x] Update Lambda runtime env vars and confirm readback.
- [x] Confirm `ENTSOG_INCLUDE_EXEMPTIONS=0` in local staged values.
- [x] Confirm `ENTSOG_INCLUDE_EXEMPTIONS=0` in Lambda readback.

Runtime update instructions are captured in `docs/setup.md` under `Update Lambda runtime environment for the first small ENTSOG run`.

Confirmed local runtime values:

```text
LAMBDA_FUNCTION_NAME=energy-market-elexon-ingest
S3_BUCKET=energy-market-lake-464975959576-20260405
BACKFILL_DAYS=1
HTTP_TIMEOUT_SECONDS=30
ENTSOG_BASE_URL=https://transparency.entsog.eu/api/v1
ENTSOG_POINT_DIRECTIONS=BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
ENTSOG_FLOW_INDICATOR=Physical Flow
ENTSOG_DEMAND_INDICATOR=Allocation
ENTSOG_PERIOD_TYPE=day
ENTSOG_TIMEZONE=WET
ENTSOG_LIMIT=1000
ENTSOG_INCLUDE_EXEMPTIONS=0
```

Saved Phase 1 evidence:

```text
docs/evidence/entsog-seed-check.md
```

Confirmed Lambda runtime readback:

```text
S3_BUCKET=energy-market-lake-464975959576-20260405
BACKFILL_DAYS=1
HTTP_TIMEOUT_SECONDS=30
ENTSOG_POINT_DIRECTIONS=BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
ENTSOG_FLOW_INDICATOR=Physical Flow
ENTSOG_DEMAND_INDICATOR=Allocation
ENTSOG_PERIOD_TYPE=day
ENTSOG_TIMEZONE=WET
ENTSOG_LIMIT=1000
ENTSOG_INCLUDE_EXEMPTIONS=0
```

Candidate retrieval command:

```bash
python3 scripts/entsog_point_directions.py \
  --countries GB,UK,FR,DE,NL \
  --ids-only \
  --max-results 20
```

Seed live-check command:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit" \
  --date 2026-05-03
```

Optional saved evidence:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit" \
  --date 2026-05-03 \
  --output-file docs/evidence/entsog-seed-check.md
```

Confirmed seed check:

```text
BE-TSO-0001ITP-00061entry: Physical Flow 83770955, Allocation 87050208
BE-TSO-0001ITP-00115exit: Physical Flow 52820231, Allocation 56000000
CZ-TSO-0001ITP-00537entry: Physical Flow 264675740, Allocation 265642169
BE-TSO-0001ITP-00555exit: Physical Flow 287851996, Allocation 288990500
```

Seed-size decision: keep the current 4-point seed for the first AWS raw ingestion proof. Do not expand until raw landing, Glue ETL, crawler, Athena, and evidence are passing end to end.

Deliverables:

- Final pointDirection list.
- Updated `config/sample.env`.
- Short note in `docs/gas-implementation-checklist.md`.

Acceptance:

- Each selected point returns rows for both `Physical Flow` and `Allocation`.
- Runtime environment has non-empty `ENTSOG_POINT_DIRECTIONS`.

### Phase 2: Raw Ingestion Proof

Budget: 3-5 hours

- [x] Deploy or update Lambda with current `lambda/ingest_elexon.py`.
- [x] Run a small single-day ingestion.
- [x] Verify `gas_flow` S3 keys land under `raw/source=entsog/dataset=gas_flow/`.
- [x] Verify `gas_demand` S3 keys land under `raw/source=entsog/dataset=gas_demand/`.
- [x] Inspect at least one `gas_flow` payload.
- [x] Inspect at least one `gas_demand` payload.
- [x] Confirm no ENTSOG warnings for selected points.

Deliverables:

- Raw S3 evidence.
- Example payload snippets or evidence file.
- Checklist updates.

Deployed-code verification:

```text
Deployed lambda/ingest_elexon.py contains:
- ENTSOG_INCLUDE_EXEMPTIONS
- includeExemptions

Deployed lambda/ingest_elexon.py does not contain:
- aggregatedData
```

Raw ingestion invocation:

```text
aws lambda invoke --payload '{"date":"2026-05-03"}' docs/evidence/entsog-lambda-invoke-result.json
```

Invocation result:

```text
status=ok
warnings=[]
raw/source=entsog/dataset=gas_flow/... wrote 4 payloads
raw/source=entsog/dataset=gas_demand/... wrote 4 payloads
```

Raw S3 verification:

```text
Bucket: energy-market-lake-464975959576-20260405
Date: 2026-05-03
gas_flow payload count: 4
gas_demand payload count: 4
```

Inspected sample payloads:

```text
gas_flow:
raw/source=entsog/dataset=gas_flow/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json
indicator=Physical Flow
pointLabel=Zeebrugge IZT
unit=kWh/d
value=83770955
flowStatus=Confirmed

gas_demand:
raw/source=entsog/dataset=gas_demand/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json
indicator=Allocation
pointLabel=Zeebrugge IZT
unit=kWh/d
value=87050208
flowStatus=Confirmed
```

Troubleshooting note: the first same-day invoke failed because Lambda `S3_BUCKET` was updated to `energy-market-lake-464975959576-20260405`, but the Lambda role policy still allowed only `energy-market-lake-464975959576-20260504`. Updating the role policy fixed S3 writes.

Acceptance:

- `gas_flow` and `gas_demand` payloads exist for each selected point/date.
- Payloads contain `operationaldatas`.
- Values are populated, not all null or empty.

### Phase 3: Curated Schema And Glue ETL

Budget: 4-6 hours

- [x] Run Glue ETL using `glue/etl_raw_to_parquet.py`.
- [x] Verify curated output lands at `curated/dataset=gas/region=eu/date=YYYY-MM-DD/`.
- [x] Inspect Parquet schema.
- [x] Confirm `flow_kwh_d` is populated.
- [x] Confirm `demand_kwh_d` is populated.
- [x] Capture Glue job run ID.

Expected curated fields:

- `source`
- `region`
- `date`
- `point_direction`
- `operator_key`
- `operator_label`
- `point_key`
- `point_label`
- `direction_key`
- `period_type`
- `period_from_utc`
- `period_to_utc`
- `flow_kwh_d`
- `demand_kwh_d`
- `unit`
- `indicator`
- `is_na`
- `last_update_time_utc`
- `item_remarks`
- `general_remarks`

Glue run evidence:

```text
Glue job: energy-market-etl-raw-to-parquet
Glue job run ID: jr_6585379162000a210b3158cc65c4976edab189aef64aafa7084c200cc8538b61
State: SUCCEEDED
Execution time: 135 seconds
Script: s3://energy-market-lake-464975959576-20260405/scripts/etl_raw_to_parquet.py
RAW_PATH: s3://energy-market-lake-464975959576-20260405/raw
CURATED_PATH: s3://energy-market-lake-464975959576-20260405/curated
```

Curated gas output:

```text
curated/dataset=gas/region=eu/date=2026-05-03/part-00000-fec8c57a-a0b8-4f0a-acbf-c549528fa307.c000.snappy.parquet
curated/dataset=gas/region=eu/date=2026-05-05/part-00000-fec8c57a-a0b8-4f0a-acbf-c549528fa307.c000.snappy.parquet
```

Curated gas inspection for `2026-05-03`:

```text
region=eu
date=2026-05-03
total_rows=8
flow_rows=4
demand_rows=4
point_directions=4
```

Sample rows:

```text
BE-TSO-0001ITP-00061entry | Allocation | Zeebrugge IZT | demand_kwh_d=87050208
BE-TSO-0001ITP-00061entry | Physical Flow | Zeebrugge IZT | flow_kwh_d=83770955
BE-TSO-0001ITP-00115exit | Allocation | Blaregnies L (BE) / Taisnieres B (FR) | demand_kwh_d=56000000
BE-TSO-0001ITP-00115exit | Physical Flow | Blaregnies L (BE) / Taisnieres B (FR) | flow_kwh_d=52820231
BE-TSO-0001ITP-00555exit | Allocation | VIP BENE | demand_kwh_d=288990500
BE-TSO-0001ITP-00555exit | Physical Flow | VIP BENE | flow_kwh_d=287851996
CZ-TSO-0001ITP-00537entry | Allocation | VIP Brandov | demand_kwh_d=265642169
CZ-TSO-0001ITP-00537entry | Physical Flow | VIP Brandov | flow_kwh_d=264675740
```

Acceptance:

- Glue job succeeds.
- Curated gas has non-zero rows.
- Both `flow_kwh_d` and `demand_kwh_d` have populated rows.

### Phase 4: Glue Catalog And Athena Exposure

Budget: 3-4 hours

- [x] Run or update curated crawler against `s3://<bucket>/curated/`.
- [x] Confirm `curated_dataset_gas` table exists.
- [x] Run gas queries from `athena/queries.sql`.
- [x] Validate schema using `scripts/validate_athena_schema.py`.

Deliverables:

- Athena table proof.
- Athena query result evidence.
- Gas schema validation file.

Curated crawler evidence:

```text
Crawler: energy-market-curated-crawler
Target: s3://energy-market-lake-464975959576-20260405/curated/
Last crawl status: SUCCEEDED
Start time: 2026-05-06T21:24:15+01:00
```

Glue catalog table:

```text
Database: energy_market_lake
Table: curated_dataset_gas
Location: s3://energy-market-lake-464975959576-20260405/curated/dataset=gas/
Partition keys: region string, date string
```

Athena schema validation:

```text
Evidence file: docs/evidence/athena-gas-schema-20260506.md
Status: PASS
Source coverage: entsog 12 rows
Latest date by source and region: entsog / eu = 2026-05-05
Completeness for deterministic proof date 2026-05-03:
- 4 point directions
- each has 1 flow row
- each has 1 demand row
```

Athena query evidence:

```text
Evidence file: docs/evidence/athena-gas-query-20260506.txt
Summary file: docs/evidence/athena-gas-query-summary-20260506.md
Query execution ID: c115051a-287d-4257-8c6f-6e3a0f7ddfd1
Query status: SUCCEEDED
Rows returned for 2026-05-03: 4
```

Athena result summary for `2026-05-03`:

```text
BE-TSO-0001ITP-00061entry | Zeebrugge IZT | flow=83770955 | demand=87050208
BE-TSO-0001ITP-00115exit | Blaregnies L (BE) / Taisnieres B (FR) | flow=52820231 | demand=56000000
BE-TSO-0001ITP-00555exit | VIP BENE | flow=287851996 | demand=288990500
CZ-TSO-0001ITP-00537entry | VIP Brandov | flow=264675740 | demand=265642169
```

Acceptance:

- Athena can query `curated_dataset_gas`.
- Freshness query returns latest gas date.
- Completeness query shows flow and demand rows.

### Phase 5: Validation And Evidence

Budget: 3-5 hours

- [x] Run closeout evidence flow.
- [x] Capture raw ENTSOG prefix count.
- [x] Capture curated gas prefix count.
- [x] Capture Athena gas schema validation.
- [x] Capture source coverage.
- [x] Capture freshness.
- [x] Capture flow/demand completeness.
- [x] Document any selected points with missing data.

Deliverables:

- New `docs/evidence/run-*.md`.
- New `docs/evidence/athena-gas-schema-*.md`.

Acceptance:

- Evidence clearly proves raw to curated to Athena.
- Validation status is pass.
- Any warnings are documented with cause and next action.

#### Phase 5 Evidence Summary

Run evidence:

```text
docs/evidence/run-entsog-gas-20260506.md
```

Raw proof counts for deterministic date `2026-05-03`:

```text
gas_flow payloads: 4
gas_demand payloads: 4
```

Curated proof count for deterministic date `2026-05-03`:

```text
curated/dataset=gas/region=eu/date=2026-05-03/: 1 parquet object
```

Validation evidence:

```text
docs/evidence/athena-gas-schema-20260506.md
Status: PASS
Source coverage: entsog = 12 rows
Freshness: entsog / eu latest date = 2026-05-05
```

Completeness gate for deterministic date `2026-05-03`:

```text
4 pointDirections checked
each has 1 flow row
each has 1 demand row
missing selected points: none
```

Note: `2026-05-05` contains partial rows from later ingestion residue. It is useful freshness evidence, but the completeness gate remains the deterministic proof date `2026-05-03`.

### Phase 6: Docs And Portfolio Update

Budget: 2-3 hours

- [x] Finalize README gas wording.
- [x] Update `docs/setup.md`.
- [x] Update `docs/gas-implementation-checklist.md`.
- [x] Update `docs/portfolio-summary.md`.
- [x] Add a gas talking point to `docs/demo-walkthrough.md`.
- [x] Add Terraform Infrastructure as Code rebuild path.

Deliverables:

- Docs reflect the implemented gas path, not target-only language.
- Terraform configuration can recreate or configure the serverless lakehouse resources.

Acceptance:

- README says gas is implemented only where AWS evidence supports it.
- Checklist separates completed code work from live AWS proof.
- Demo walkthrough has a clean gas story.
- Terraform supports S3 remote state and keeps the Terraform state bucket separate from the data lake bucket.
- Creating a new data lake bucket is optional.

#### Phase 6 Documentation Summary

Updated documentation:

```text
README.md
docs/setup.md
docs/gas-implementation-checklist.md
docs/portfolio-summary.md
docs/demo-walkthrough.md
diagrams/architecture.mmd
```

Gas dashboard decision:

```text
Do not add gas metrics to the public dashboard snapshot in Phase 6.
Treat gas dashboard cards as Phase 7 follow-on work after the data layer proof.
Candidate metrics: total daily flow, allocation proxy, flow/allocation delta, and top pointDirections by flow.
```

Portfolio wording now says:

```text
ENTSOG gas is proven through raw S3, curated Parquet, Glue Catalog, Athena query, and validation evidence.
Gas is not yet published into the public dashboard snapshot.
```

#### Phase 6B: Terraform Infrastructure As Code

Status: scaffolded and locally validated.

Terraform root:

```text
infra/terraform/lakehouse/
```

Import runbook:

```text
docs/terraform-import-checklist.md
```

Implemented resources:

```text
Optional data lake S3 bucket
Lambda function, package, execution role, and log group
EventBridge daily ingestion schedule
Glue role, database, raw crawler, curated crawler, and ETL job
Glue script upload to S3
Athena workgroup and query result location
```

Backend pattern:

```text
Terraform remote backend using S3
State bucket is separate from the data lake bucket
Backend example: infra/terraform/lakehouse/backend.hcl.example
```

Data bucket modes:

```text
create_data_bucket = false
data_bucket_name   = existing bucket

create_data_bucket = true
data_bucket_name   = new bucket name
```

Validation run:

```text
terraform fmt -recursive .
terraform init -backend=false
terraform validate
Result: Success
```

### Phase 7: Optional Dashboard Follow-On

Budget: 4-8 hours

- [x] Decide dashboard gas metrics.
- [x] Add gas data to the Athena-backed dashboard data generator.
- [x] Extend React dashboard cards and pointDirection table.
- [x] Load six additional gas days for a seven-day trend window.
- [x] Add rolling 7-day Gas tab charts below the pointDirection table.
- [x] Tidy dashboard information architecture into Energy Overview, Power, Gas, and Data Quality tabs.
- [x] Expand public-safe news context so gas and electricity movement articles are visible without mixing commodity metrics.
- [x] Validate frontend build and existing JSON contracts.
- [x] Capture visual QA screenshot evidence for the rendered gas dashboard section.

Candidate dashboard metrics:

- Total daily flow.
- Allocation proxy.
- Top point directions.
- Flow versus allocation delta.
- Rolling 7-day flow versus allocation.
- Rolling 7-day allocation delta.
- Rolling 7-day completeness.
- Latest gas date.
- Completeness across selected pointDirections.

Acceptance:

- Dashboard can display gas metrics from curated or Athena-derived data.
- Public dashboard still avoids direct raw or curated S3 access.

#### Phase 7 Evidence Summary

Chosen option:

```text
Option B: Gas Context + PointDirection Table
```

Evidence:

```text
docs/evidence/phase7-dashboard-gas-20260507.md
docs/evidence/screenshots/dashboard-phase7-gas-context-20260507.png
docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png
docs/evidence/screenshots/dashboard-power-tab-20260507.png
docs/evidence/screenshots/dashboard-gas-tab-20260507.png
docs/evidence/gas-7day-trend-20260507.md
docs/evidence/gas-7day-athena-coverage-20260507.json
docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png
docs/evidence/news-refresh-expanded-20260507.md
docs/evidence/screenshots/dashboard-news-expanded-20260507.png
```

Generated dashboard gas context:

```text
gas latestDate: 2026-05-05
total flow: 784.0 GWh/d
allocation proxy: 790.1 GWh/d
completeness: 4/4
pointDirection rows: 4
completenessStatus: healthy
trendPoints: 7
trendRange: 2026-04-29 to 2026-05-05
latest trend delta: +6.1 GWh/d
```

Validation:

```text
npm run build: passed
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures: passed
GET /: 200
GET /dashboard-data.json: 200
GET /dashboard_snapshot_v1.sample.json: 200
visual QA screenshot: captured at 1920x1800
tabbed UI screenshots: overview, power, and gas captured
7-day gas trend screenshot: captured at 1920x1500
expanded news screenshot: captured at 1920x1900
```

Implementation boundary:

```text
Gas appears in dashboard-ui/public/dashboard-data.json and the React dashboard.
Gas is not added to dashboard_snapshot_v1.sample.json or AI insight semantics.
The public dashboard snapshot includes only public-safe news article fields for market context.
```

#### Phase 7 Closeout

Status:

```text
Phase 7 Task 1 is complete.
```

Closeout notes:

```text
Rebuild/setup documentation has been updated in docs/setup.md.
Demo and portfolio docs point to the Phase 7 evidence and screenshots.
Terraform rebuild/import guidance remains in docs/terraform-import-checklist.md and infra/terraform/lakehouse/.
The UI tidy-up is recorded as intentional scope because it separates power, gas, data-quality, and cross-energy news context.
```

## Recommended Execution Order

1. Lock seed pointDirections.
2. Run raw ingestion with `BACKFILL_DAYS=1`.
3. Inspect raw payloads.
4. Run Glue ETL.
5. Run curated crawler.
6. Query Athena.
7. Run validation/evidence.
8. Update docs.
9. Decide dashboard gas scope.

## Critical Risks

- ENTSOG point IDs are awkward: `operatorpointdirections.id` is not always the string that works against `operationaldatas`.
- `GB` appears as `UK` in ENTSOG gas metadata.
- Some `hasData=true` points can still return N/A or empty values for a given day.
- Glue crawler table naming can drift with partitions unless old `curated_dataset_gas*` tables are cleaned.
- Athena proof is incomplete until the real crawler and query have run.
