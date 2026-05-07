# ENTSOG Gas 7-Day Trend Evidence

- Date: 2026-05-07
- Scope: load six additional ENTSOG gas days and render rolling 7-day gas charts in the dashboard Gas tab.
- Bucket: `energy-market-lake-464975959576-20260405`
- Lambda: `energy-market-elexon-ingest`
- Glue job: `energy-market-etl-raw-to-parquet`
- Glue crawler: `energy-market-curated-crawler`
- Athena table: `curated_dataset_gas`

## Loaded Dates

The existing curated dashboard date was `2026-05-05`. Six earlier gas dates were loaded to create a seven-day trend window:

```text
2026-04-29
2026-04-30
2026-05-01
2026-05-02
2026-05-03
2026-05-04
```

Lambda invoke evidence:

```text
docs/evidence/entsog-lambda-invoke-2026-04-29.json
docs/evidence/entsog-lambda-invoke-2026-04-30.json
docs/evidence/entsog-lambda-invoke-2026-05-01.json
docs/evidence/entsog-lambda-invoke-2026-05-02.json
docs/evidence/entsog-lambda-invoke-2026-05-03.json
docs/evidence/entsog-lambda-invoke-2026-05-04.json
```

Each invoke returned `StatusCode=200`, `status=ok`, and no warnings.

## Raw Coverage

Raw S3 payload counts for the seven-day window:

```text
raw/source=entsog/dataset=gas_flow/: 28 payloads
raw/source=entsog/dataset=gas_demand/: 28 payloads
```

That is four selected pointDirections across seven dates for both physical flow and allocation proxy payloads.

## Curated Refresh

Glue ETL run:

```text
job run id: jr_ee934b795a3e2233cb15d8141d202595057a8e08eec5e966d172a9a61e621557
status: SUCCEEDED
```

Glue crawler:

```text
energy-market-curated-crawler: SUCCEEDED
```

Athena coverage evidence:

```text
docs/evidence/gas-7day-athena-coverage-20260507.json
```

Coverage readback:

| date | points | flow rows | demand rows |
| --- | ---: | ---: | ---: |
| 2026-05-05 | 4 | 4 | 4 |
| 2026-05-04 | 4 | 4 | 4 |
| 2026-05-03 | 4 | 4 | 4 |
| 2026-05-02 | 4 | 4 | 4 |
| 2026-05-01 | 4 | 4 | 4 |
| 2026-04-30 | 4 | 4 | 4 |
| 2026-04-29 | 4 | 4 | 4 |

## Dashboard Readback

`dashboard-ui/public/dashboard-data.json` now contains seven ENTSOG gas trend points.

```text
latestDate: 2026-05-05
pointDirections: 4
trendPoints: 7
trendRange: 2026-04-29 to 2026-05-05
latestFlow: 784.0 GWh/d
latestAllocation: 790.1 GWh/d
latestDelta: +6.1 GWh/d
latestComplete: 4/4
```

The Gas tab renders three rolling charts below the selected pointDirection table:

```text
Physical Flow vs Allocation
Allocation Delta
Completeness
```

## Visual Evidence

Updated Gas tab screenshot:

```text
docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png
```

The screenshot confirms:

```text
Gas tab is selected.
Gas summary cards use 2026-05-05 latest curated gas data.
The selected pointDirection table remains visible.
The rolling 7-day trend section appears below the table.
The trend range is 2026-04-29 to 2026-05-05.
```

## Validation

Commands:

```text
python3 -m py_compile scripts/generate_dashboard.py
npm run build
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
git diff --check
```

Result:

```text
All checks passed.
```
