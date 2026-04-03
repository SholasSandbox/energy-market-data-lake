# Phase 1 Checklist: Stabilize Ingestion And Lakehouse

Use this file for the active implementation checklist behind Phase 1 in `PLANS.md`.

## Goal

Make the ingestion and transformation flow dependable before expanding the analytical surface area.

## Checklist

- [ ] Re-run the clean end-to-end path from scheduled ingestion through Athena validation.
- [ ] Confirm the expected raw S3 outputs still match the README storage layout.
- [ ] Confirm crawler creation, execution, and readiness handling still work cleanly.
- [ ] Confirm Glue ETL completes successfully and writes curated outputs in the expected layout.
- [ ] Confirm Athena queries and schema validation still succeed after crawler and ETL runs.
- [ ] Confirm dashboard generation still uses validated Athena-backed outputs.
- [ ] Confirm evidence files are written for ingestion, validation, Athena, and dashboard outputs.
- [ ] Review failure points in the ingestion path and make retries, errors, and logs easier to understand.
- [ ] Tighten freshness checks so stale data is obvious.
- [ ] Tighten completeness checks so missing intervals are obvious.
- [ ] Tighten source coverage checks across the currently supported electricity datasets.
- [ ] Verify the React `Data Quality` view still matches the actual generated quality signals.
- [ ] Update README and diagrams if implementation behavior changed during stabilization.

## Validation Commands

Run these when validating the current electricity-focused flow:

```bash
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
python3 scripts/validate_athena_schema.py --help
python3 scripts/generate_dashboard.py
python3 scripts/generate_dashboard.py \
  --output-json /home/shola/cert-revision/energy-market-data-lake/dashboard-ui/public/dashboard-data.json
```

## Evidence To Check

- `docs/evidence/run-*.md`
- `docs/evidence/athena-schema-*.md`
- `docs/evidence/athena-run-*.md`
- `docs/evidence/dashboard-*.html`

## Done Gate

Phase 1 is complete when all of the following are true:

- scheduled ingestion runs consistently without manual repair
- crawler and ETL outputs are reproducible
- Athena-backed outputs are trustworthy enough to feed dashboard pages
- data quality checks make incomplete or stale data obvious
- README and diagrams still match actual implemented behavior

## Notes

- Keep this checklist operational and current.
- Keep `PLANS.md` strategic. If the delivery order changes, update `PLANS.md` first and then adjust this file.
