# Athena Schema Validation

- Timestamp (UTC): 2026-05-06T21:34:49Z
- Region: eu-west-2
- Database: energy_market_lake
- Requested table: curated_dataset_gas
- Resolved table: curated_dataset_gas
- Output location: s3://energy-market-lake-464975959576-20260405/athena-results/
- Status: **PASS**

## Required Columns

- OK `source` -> expected `string`, actual `varchar`
- OK `region` -> expected `string`, actual `varchar`
- OK `date` -> expected `string`, actual `varchar`
- OK `point_direction` -> expected `string`, actual `varchar`
- OK `operator_key` -> expected `string`, actual `varchar`
- OK `point_key` -> expected `string`, actual `varchar`
- OK `direction_key` -> expected `string`, actual `varchar`
- OK `period_type` -> expected `string`, actual `varchar`
- OK `period_from_utc` -> expected `timestamp`, actual `timestamp(3)`
- OK `period_to_utc` -> expected `timestamp`, actual `timestamp(3)`
- OK `flow_kwh_d` -> expected `double`, actual `double`
- OK `demand_kwh_d` -> expected `double`, actual `double`
- OK `unit` -> expected `string`, actual `varchar`
- OK `indicator` -> expected `string`, actual `varchar`
- OK `is_na` -> expected `boolean`, actual `boolean`

## Source Coverage

- `entsog`: 12 rows

## Latest Dates By Source and Region

- `entsog` / `eu`: 2026-05-05

## Gas Completeness

- `2026-05-05` / `BE-TSO-0001ITP-00061entry`: 0 flow rows, 1 demand rows
- `2026-05-05` / `BE-TSO-0001ITP-00115exit`: 0 flow rows, 1 demand rows
- `2026-05-05` / `BE-TSO-0001ITP-00555exit`: 0 flow rows, 1 demand rows
- `2026-05-05` / `CZ-TSO-0001ITP-00537entry`: 1 flow rows, 0 demand rows
- `2026-05-03` / `BE-TSO-0001ITP-00061entry`: 1 flow rows, 1 demand rows
- `2026-05-03` / `BE-TSO-0001ITP-00115exit`: 1 flow rows, 1 demand rows
- `2026-05-03` / `BE-TSO-0001ITP-00555exit`: 1 flow rows, 1 demand rows
- `2026-05-03` / `CZ-TSO-0001ITP-00537entry`: 1 flow rows, 1 demand rows
