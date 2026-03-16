# Project Plan (5-8 hrs/week)

Status: Completed on 2026-03-06. See `docs/closeout-summary.md`.

## Week 1 (3-4 hrs)

- Finalize data endpoints (Elexon ATL + system prices)
- Create S3 bucket + lifecycle policies
- Create Glue Data Catalog + Crawler
- Document S3 layout

## Week 2 (4-5 hrs)

- Build Lambda ingestion (Elexon ATL + system prices)
- Add 30-day backfill + half-hourly window
- Add ENTSO-E ingestion (token in SSM/Secrets Manager)
- Write raw data to S3
- Validate raw schema

## Week 3 (2-4 hrs)

- Glue ETL: raw -> Parquet
- Athena demo queries
- README + diagram + screenshots
