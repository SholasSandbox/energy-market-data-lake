# Demo Checklist

- [x] S3 bucket + lifecycle policy configured
- [x] Raw data in S3 (Elexon ATL + system prices)
- [x] 30-day backfill confirmed (half-hourly)
- [x] Glue Crawler created + run
- [x] Glue ETL job ran successfully
- [x] Parquet data in curated zone
- [x] Athena queries run (captured outputs in `docs/evidence/athena-run-20260306-231854.md`)
- [x] README + diagram updated
- [x] Professional dashboard generated from curated dataset

## Evidence

- Deployment/ingestion/crawler/job evidence: `docs/evidence/run-20260306-231751.md`
- Post-validation summary evidence: `docs/evidence/run-summary-20260306-232202.md`
- Athena query evidence (query IDs + result snippets): `docs/evidence/athena-run-20260306-231854.md`
- Dashboard artifact: `docs/evidence/dashboard-20260310-224038.html`
