# Previous Two-Day Data Run - 2026-07-09

## Scope

- Request: run the previous two data days across the lakehouse data pipelines.
- Dates treated as the previous two completed calendar days: `2026-07-07` and
  `2026-07-08`.
- AWS account: `464975959576`.
- Region: `eu-west-2`.
- Data bucket: `energy-market-lake-464975959576-20260405`.
- Function used for the authoritative run: `energy-market-elexon-ingest`.

No Terraform apply, IAM change, service enablement/disablement, account change,
dashboard publish, or CloudFront invalidation was performed.

## Evidence Files

- `docs/evidence/data-run-ingest-lambda-config-sanitized-20260709.json`
  records the live Lambda state, timeout, runtime, memory size, and environment
  variable names only. Secret values were not captured.
- `docs/evidence/data-run-ingest-lambda-log-events-20260709.json` records the
  CloudWatch START/END/REPORT events for the live Lambda invocations.
- `docs/evidence/data-run-20260707-ingestion-20260709.json` and
  `docs/evidence/data-run-20260708-ingestion-20260709.json` record the first
  local handler attempts. Both were partial because public ENTSOG calls timed
  out or returned a transient upstream error.
- `docs/evidence/data-run-20260708-lambda-invoke-20260709.json` and
  `docs/evidence/data-run-20260708-lambda-response-20260709.json` record the
  successful deployed Lambda invocation for `2026-07-08`.
- `docs/evidence/data-run-s3-heads-20260709.json` records the S3 object-level
  verification for Elexon, ENTSOG, and expected ENTSO-E raw paths.
- `docs/evidence/data-run-contract-validation-20260709.txt` records JSON
  contract validation after the run.

## Result Summary

| Date | Source | Expected raw objects | Present raw objects | Result |
|---|---:|---:|---:|---|
| `2026-07-07` | Elexon | 2 | 2 | Complete |
| `2026-07-07` | ENTSOG | 8 | 8 | Complete after deployed Lambda retries |
| `2026-07-07` | ENTSO-E | 8 | 0 | No raw objects produced |
| `2026-07-08` | Elexon | 2 | 2 | Complete |
| `2026-07-08` | ENTSOG | 8 | 8 | Complete |
| `2026-07-08` | ENTSO-E | 8 | 0 | No raw objects produced |

The deployed Lambda invocation for `2026-07-08` returned `status: ok` with
10 raw objects and no warnings. The returned keys covered Elexon and ENTSOG.
It did not return ENTSO-E keys. The Lambda environment contains an
`ENTSOE_TOKEN` key name, but this evidence intentionally does not inspect or
record the secret value.

The first deployed Lambda invoke for `2026-07-07` hit an AWS CLI read timeout.
CloudWatch showed three Lambda request IDs because the CLI retried the invoke.
All request IDs ended successfully, and S3 verification confirms the expected
Elexon and ENTSOG raw objects are present for `2026-07-07`.

## Curated ETL Boundary

The Glue ETL job `energy-market-etl-raw-to-parquet` was inspected but not run.
Its current arguments read from `s3://energy-market-lake-464975959576-20260405/raw`
and write to `s3://energy-market-lake-464975959576-20260405/curated`. The job is
not date-scoped, so running it for this two-day admin refresh would rewrite
curated partitions beyond the requested dates.

## Follow-Up Gaps

1. Add a date-scoped ETL/runbook path so future two-day refreshes can update
   curated Parquet without rewriting unrelated historical partitions.
2. Make ENTSO-E runtime configuration observable without exposing the token,
   and make the ingestion response warn when ENTSO-E is configured but produces
   no raw objects.
3. Use `AWS_MAX_ATTEMPTS=1` and an explicit long `--cli-read-timeout` for manual
   Lambda invokes to avoid duplicate invocations when upstream APIs are slow.

## Tracker Mapping

This supports the Energy Data Lakehouse case study and SAP-C02 Domain 3
operations by exercising the live ingestion path, verifying S3 evidence, and
identifying a resilience/operability gap in date-scoped curated refreshes.
