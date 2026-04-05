# ENTSO-E Operationalization Checklist

Use this checklist to move ENTSO-E from partially implemented support into a dependable, repeatable part of the electricity pipeline.

## Goal

Make ENTSO-E ingestion, transformation, validation, and downstream use reliable enough to be treated as part of the stable electricity path.

## Checklist

- [ ] Confirm a valid `ENTSOE_TOKEN` is available in the runtime environment.
- [ ] Confirm the configured ENTSO-E zones are correct for the project scope (`GB`, `FR`, `DE`, `NL`).
- [ ] Re-run raw ENTSO-E ingestion and confirm `actual_load` XML files land in the expected S3 paths.
- [ ] Re-run raw ENTSO-E ingestion and confirm `day_ahead_prices` XML files land in the expected S3 paths.
- [ ] Confirm raw file sizes are non-zero for each enabled zone and date.
- [ ] Improve visibility of ENTSO-E failures so skipped zones and failed dates are easy to diagnose from logs and evidence.
- [ ] Verify Glue ETL correctly parses ENTSO-E actual load XML into curated electricity records.
- [ ] Verify Glue ETL correctly parses ENTSO-E day-ahead price XML into curated electricity records.
- [ ] Verify the ETL join between ENTSO-E load and price data behaves correctly when one side is missing.
- [ ] Verify curated electricity contains ENTSO-E rows for all enabled regions after ETL.
- [ ] Verify Athena queries return ENTSO-E price and load outputs as expected.
- [ ] Verify dashboard generation includes ENTSO-E-derived market context panels and quality signals.
- [ ] Add or improve evidence capture for ENTSO-E-specific raw counts, curated counts, and latest available dates.
- [ ] Update README or setup notes if the real ENTSO-E operational path differs from the current wording.

## Validation Targets

- Raw S3 prefixes:
  - `raw/source=entsoe/dataset=actual_load/zone=<zone>/date=YYYY-MM-DD/payload.xml`
  - `raw/source=entsoe/dataset=day_ahead_prices/zone=<zone>/date=YYYY-MM-DD/payload.xml`
- Curated output:
  - `curated/dataset=electricity/source=entsoe/region=<region>/date=YYYY-MM-DD/`
- Athena/table expectations:
  - `source = 'entsoe'`
  - populated `demand_mw`
  - populated `day_ahead_price_eur_mwh`

## Done Gate

ENTSO-E is operationalized when all enabled regions ingest reliably, ETL produces curated electricity rows consistently, Athena validation passes with ENTSO-E source coverage, and dashboard outputs reflect ENTSO-E data without manual repair.
