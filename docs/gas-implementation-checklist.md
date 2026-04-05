# Gas Implementation Checklist

Use this checklist to build the missing gas lakehouse path on top of the existing ENTSOG raw-ingestion support.

## Goal

Implement gas end-to-end so ENTSOG data moves from raw ingestion into curated storage, Athena, validation, and later dashboard use.

## Checklist

- [ ] Finalize the initial ENTSOG `pointDirection` set for the target countries in scope.
- [ ] Confirm `ENTSOG_POINT_DIRECTIONS` is populated in the runtime environment.
- [ ] Confirm the chosen flow and demand indicators are correct for the intended gas datasets.
- [ ] Re-run raw ENTSOG ingestion and verify `gas_flow` payloads land in the expected S3 layout.
- [ ] Re-run raw ENTSOG ingestion and verify `gas_demand` payloads land in the expected S3 layout.
- [ ] Inspect example raw ENTSOG payloads and document the fields needed in curated gas outputs.
- [ ] Define the curated gas schema to match the README target model under `curated/dataset=gas/region=eu/date=YYYY-MM-DD/`.
- [ ] Decide whether gas should use one curated table or separate flow/demand curated tables before Athena exposure.
- [ ] Implement Glue ETL parsing for raw ENTSOG flow payloads.
- [ ] Implement Glue ETL parsing for raw ENTSOG demand payloads.
- [ ] Write curated gas Parquet outputs into the expected partitioned S3 layout.
- [ ] Add Glue crawler/catalog support so curated gas is queryable.
- [ ] Add Athena queries or validation logic for curated gas tables.
- [ ] Add schema, freshness, completeness, and source coverage checks for gas.
- [ ] Extend evidence generation so gas ingestion, ETL, and Athena outputs are captured explicitly.
- [ ] Update README, diagrams, and setup docs so gas is described consistently from raw to curated to Athena.
- [ ] Decide what gas-aware dashboard outputs should exist after the data layer is complete.

## Validation Targets

- Raw S3 prefixes:
  - `raw/source=entsog/dataset=gas_flow/point_direction=<id>/date=YYYY-MM-DD/payload.json`
  - `raw/source=entsog/dataset=gas_demand/point_direction=<id>/date=YYYY-MM-DD/payload.json`
- Curated target:
  - `curated/dataset=gas/region=eu/date=YYYY-MM-DD/`
- Downstream requirement:
  - Athena can query curated gas outputs without manual intervention

## Done Gate

Gas is implemented when ENTSOG raw data lands reliably, curated gas outputs are written and cataloged, Athena can query them, validation covers schema and freshness, and the docs/diagrams reflect the implemented gas path.
