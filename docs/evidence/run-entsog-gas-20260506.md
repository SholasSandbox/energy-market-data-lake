# ENTSOG Gas Phase 5 Evidence Run

- Timestamp (UTC): 2026-05-06T21:34:49Z
- Region: eu-west-2
- Data lake bucket: `energy-market-lake-464975959576-20260405`
- Deterministic proof date: `2026-05-03`
- Glue database: `energy_market_lake`
- Athena table: `curated_dataset_gas`
- Validation status: **PASS**

## Seed Point Directions

The Phase 5 validation uses the small four-point ENTSOG seed selected in Phase 1:

```text
BE-TSO-0001ITP-00061entry
BE-TSO-0001ITP-00115exit
CZ-TSO-0001ITP-00537entry
BE-TSO-0001ITP-00555exit
```

Seed live-check evidence:

```text
docs/evidence/entsog-seed-check.md
```

## Raw Ingestion Evidence

Lambda invoke result:

```text
docs/evidence/entsog-lambda-invoke-result.json
```

Invoke status:

```text
status: ok
warnings: []
```

Raw ENTSOG S3 counts for `2026-05-03`:

```text
raw/source=entsog/dataset=gas_flow/:   4 payloads
raw/source=entsog/dataset=gas_demand/: 4 payloads
```

Sample raw payloads inspected:

```text
raw/source=entsog/dataset=gas_flow/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json
raw/source=entsog/dataset=gas_demand/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json
```

Observed payload shape:

```text
top-level array field: operationaldatas
flow indicator: Physical Flow
demand proxy indicator: Allocation
unit: kWh/d
flow status: Confirmed
```

## Curated Output Evidence

Curated S3 count for deterministic proof date:

```text
curated/dataset=gas/region=eu/date=2026-05-03/: 1 parquet object
```

Curated location:

```text
s3://energy-market-lake-464975959576-20260405/curated/dataset=gas/region=eu/date=2026-05-03/
```

Local Parquet inspection from Phase 3 showed:

```text
total_rows: 8
flow_rows: 4
demand_rows: 4
point_directions: 4
```

## Athena And Catalog Evidence

Glue catalog table:

```text
database: energy_market_lake
table: curated_dataset_gas
location: s3://energy-market-lake-464975959576-20260405/curated/dataset=gas/
partition keys: region, date
```

Athena schema and validation evidence:

```text
docs/evidence/athena-gas-schema-20260506.md
```

Validation summary:

```text
Status: PASS
Source coverage: entsog = 12 rows
Latest source/region date: entsog / eu = 2026-05-05
```

Deterministic proof date completeness:

```text
2026-05-03 / BE-TSO-0001ITP-00061entry: 1 flow row, 1 demand row
2026-05-03 / BE-TSO-0001ITP-00115exit: 1 flow row, 1 demand row
2026-05-03 / BE-TSO-0001ITP-00555exit: 1 flow row, 1 demand row
2026-05-03 / CZ-TSO-0001ITP-00537entry: 1 flow row, 1 demand row
```

Athena query evidence:

```text
docs/evidence/athena-gas-query-20260506.txt
docs/evidence/athena-gas-query-summary-20260506.md
```

Athena query result summary for `2026-05-03`:

| point_direction | point_label | flow_kwh_d | demand_kwh_d |
| --- | --- | ---: | ---: |
| `BE-TSO-0001ITP-00061entry` | Zeebrugge IZT | 83770955 | 87050208 |
| `BE-TSO-0001ITP-00115exit` | Blaregnies L (BE) / Taisnieres B (FR) | 52820231 | 56000000 |
| `BE-TSO-0001ITP-00555exit` | VIP BENE | 287851996 | 288990500 |
| `CZ-TSO-0001ITP-00537entry` | VIP Brandov | 264675740 | 265642169 |

## Missing Data Notes

No selected pointDirection is missing flow or demand for the deterministic proof date `2026-05-03`.

The validation output also sees partial rows for `2026-05-05`, which came from same-day or later ingestion residue. Those rows prove freshness but are not used as the deterministic completeness gate because the selected gas day can be incomplete while ENTSOG is still publishing updates.

## Phase 5 Acceptance

- Raw ENTSOG proof exists for gas flow and gas demand.
- Curated gas Parquet exists under the expected partitioned S3 prefix.
- Glue Catalog exposes `curated_dataset_gas`.
- Athena schema validation passes.
- Source coverage, freshness, and flow/demand completeness are captured.
- Missing-data notes are documented.
