# Project Closeout Summary

Date closed: **2026-03-06**
Region: **eu-west-2**
Account: **464975959576**

## What was completed

- S3 bucket provisioned and configured:
  - block public access
  - SSE-S3 encryption
  - lifecycle on `raw/` (IA/Glacier/expiry)
- Lambda ingestion deployed (`energy-market-elexon-ingest`) and invoked
- 30-day Elexon backfill written to S3 raw zone
- Glue database + crawlers created and run successfully
- Glue ETL job (`energy-market-etl-raw-to-parquet`) completed successfully
- Curated parquet generated and cataloged
- Athena queries executed and evidence captured

## Evidence files

- `docs/evidence/run-20260306-231751.md`
- `docs/evidence/run-summary-20260306-232202.md`
- `docs/evidence/athena-run-20260306-231854.md`

## Key resources

- Bucket: `energy-market-lake-464975959576-20260306`
- Glue database: `energy_market_lake`
- Tables:
  - `raw_dataset_atl`
  - `raw_dataset_system_prices`
  - `curated_dataset_electricity`

## Cost notes

- Keep only short backfill windows unless needed.
- Destroy demo resources after presenting.
- Bucket lifecycle is configured to reduce long-term storage cost.

## Cleanup commands

```bash
aws events remove-targets --rule energy-market-daily-ingestion --ids 1 --region eu-west-2 || true
aws events delete-rule --name energy-market-daily-ingestion --region eu-west-2 || true
aws lambda delete-function --function-name energy-market-elexon-ingest --region eu-west-2 || true

aws glue delete-crawler --name energy-market-raw-crawler --region eu-west-2 || true
aws glue delete-crawler --name energy-market-curated-crawler --region eu-west-2 || true
aws glue delete-job --job-name energy-market-etl-raw-to-parquet --region eu-west-2 || true
aws glue delete-database --name energy_market_lake --region eu-west-2 || true

aws iam delete-role-policy --role-name energy-market-lambda-role --policy-name energy-market-lambda-s3-policy || true
aws iam detach-role-policy --role-name energy-market-lambda-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole || true
aws iam delete-role --role-name energy-market-lambda-role || true

aws iam delete-role-policy --role-name energy-market-glue-role --policy-name energy-market-glue-s3-policy || true
aws iam detach-role-policy --role-name energy-market-glue-role --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole || true
aws iam delete-role --role-name energy-market-glue-role || true

aws s3 rm s3://energy-market-lake-464975959576-20260306 --recursive
aws s3 rb s3://energy-market-lake-464975959576-20260306
```
