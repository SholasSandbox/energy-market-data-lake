# Gas Implementation Checklist

Use this checklist to track the completed ENTSOG gas lakehouse path on top of the existing energy ingestion platform.

## Goal

Implement gas end-to-end so ENTSOG data moves from raw ingestion into curated storage, Athena, validation, and later dashboard use.

## Checklist

- [x] Finalize the initial ENTSOG `pointDirection` set for the target countries in scope.
- [x] Confirm `ENTSOG_POINT_DIRECTIONS` is populated in the runtime environment.
- [x] Confirm the chosen flow and demand indicators are correct for the intended gas datasets.
- [x] Re-run raw ENTSOG ingestion and verify `gas_flow` payloads land in the expected S3 layout.
- [x] Re-run raw ENTSOG ingestion and verify `gas_demand` payloads land in the expected S3 layout.
- [x] Inspect example raw ENTSOG payloads and document the fields needed in curated gas outputs.
- [x] Define the curated gas schema to match the README target model under `curated/dataset=gas/region=eu/date=YYYY-MM-DD/`.
- [x] Decide whether gas should use one curated table or separate flow/demand curated tables before Athena exposure.
- [x] Implement Glue ETL parsing for raw ENTSOG flow payloads.
- [x] Implement Glue ETL parsing for raw ENTSOG demand payloads.
- [x] Write curated gas Parquet outputs into the expected partitioned S3 layout.
- [x] Add Glue crawler/catalog support so curated gas is queryable.
- [x] Add Athena queries or validation logic for curated gas tables.
- [x] Add schema, freshness, completeness, and source coverage checks for gas.
- [x] Extend evidence generation so gas ingestion, ETL, and Athena outputs are captured explicitly.
- [x] Update README, diagrams, and setup docs so gas is described consistently from raw to curated to Athena.
- [x] Decide what gas-aware dashboard outputs should exist after the data layer is complete.

## Implementation Notes

- Initial pointDirection seed set:
  `BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit`
- Seed set was live-checked on `2026-05-03` for both `Physical Flow` and `Allocation`.
- ENTSOG metadata uses `UK` rather than `GB` for gas point directions; the helper expands requested `GB` to include `UK`.
- Curated gas uses one table under `curated/dataset=gas/region=eu/date=YYYY-MM-DD/`, with `flow_kwh_d` and `demand_kwh_d` columns.
- Phase 5 evidence is captured in `docs/evidence/run-entsog-gas-20260506.md`, with Athena validation in `docs/evidence/athena-gas-schema-20260506.md`.
- Gas-aware dashboard output is implemented in the Athena-backed React dashboard data. The public AI snapshot remains unchanged. Current dashboard metrics are total daily flow, allocation proxy, flow/allocation delta, latest gas date, completeness, top pointDirections by flow, and rolling 7-day gas trends.

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

Current status: **done for the data layer and React dashboard context**. The gas data is not surfaced in the public AI snapshot. The dashboard separates `Energy Overview`, `Power`, `Gas`, and `Data Quality` views so gas and power metrics are not mixed unless explicitly framed as cross-energy news context.
