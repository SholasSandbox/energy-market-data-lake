# Glue And Athena IAM Live Verification - 2026-06-15

## Scope

This evidence records the approved live deployment and service verification for
ADR 0004.

## Command Documentation

The reusable command sequence is now documented in
`docs/glue-athena-iam-deployment-runbook.md`. It explains the Terraform plan
and apply commands, live IAM capture commands, Glue verification commands,
Athena role-assumption commands, positive S3 prefix checks, the raw-prefix deny
check, Athena query execution, cleanup, and rollback boundary.

## Deployment

The normal root Terraform plan was created first and saved to:

```text
docs/evidence/glue-athena-iam-normal-plan-blocked-20260615.txt
```

That plan was not applied because it included unrelated budget, CloudFront,
dashboard policy, SNS, Lambda environment, and Step Functions changes. This was
outside the explicit Glue/Athena IAM approval.

An isolated IAM-only plan was then created and saved to:

```text
docs/evidence/glue-athena-iam-targeted-apply-plan-20260615.txt
```

The targeted plan contained only:

- create `aws_iam_role.athena_query`;
- create `aws_iam_role_policy.athena_query`; and
- update `aws_iam_role_policy.glue_s3` in place.

It applied successfully:

```text
Apply complete! Resources: 2 added, 1 changed, 0 destroyed.
```

Apply output:

```text
docs/evidence/glue-athena-iam-apply-20260615.txt
```

## Live IAM Evidence

Captured live AWS IAM documents:

- `docs/evidence/glue-s3-policy-live-20260615.json`
- `docs/evidence/athena-query-role-live-20260615.json`
- `docs/evidence/athena-query-policy-live-20260615.json`

## Glue Verification

Raw crawler:

```text
energy-market-raw-crawler
Status: SUCCEEDED
StartTime: 2026-06-15T23:58:51+01:00
Evidence: docs/evidence/glue-raw-crawler-final-20260615.json
```

Glue ETL job:

```text
Job: energy-market-etl-raw-to-parquet
Run ID: jr_9319c5f2b342596eb0405f1850495267b13eeb6adc7c5c85901c20ff14ad75c3
State: SUCCEEDED
ExecutionTime: 203 seconds
DPUSeconds: 407.0
Evidence: docs/evidence/glue-etl-job-run-final-20260615.json
```

Curated crawler:

```text
energy-market-curated-crawler
Status: SUCCEEDED
StartTime: 2026-06-16T00:05:21+01:00
Evidence: docs/evidence/glue-curated-crawler-final-20260615.json
```

Together these prove the restricted Glue role can list/read required raw,
curated, and script prefixes, run the ETL script, write curated output, and
refresh the Glue Catalog.

## Athena Verification

The dedicated Athena role was assumed successfully:

```text
arn:aws:sts::464975959576:assumed-role/energy-market-athena-query-role/lakehouse-athena-verify-20260615T230827Z
Evidence: docs/evidence/athena-assumed-role-identity-20260615.json
```

S3 prefix checks with the assumed role:

- `curated/` list succeeded:
  `docs/evidence/athena-role-s3-curated-list-20260615.json`
- `athena-results/` list succeeded:
  `docs/evidence/athena-role-s3-results-list-20260615.json`
- `raw/` list failed with `AccessDenied`, as intended:
  `docs/evidence/athena-role-s3-raw-list-20260615.err`

Athena query:

```sql
SELECT count(*) AS row_count FROM curated_dataset_gas
```

Result:

```text
QueryExecutionId: 8869d2e5-9642-446c-804f-906fee97dbbb
State: SUCCEEDED
WorkGroup: energy-market-workgroup
OutputLocation: s3://energy-market-lake-464975959576-20260405/athena-results/8869d2e5-9642-446c-804f-906fee97dbbb.csv
Encryption: SSE_S3
row_count: 56
DataScannedInBytes: 0
```

Evidence:

- `docs/evidence/athena-query-execution-final-20260615.json`
- `docs/evidence/athena-query-results-20260615.json`

## Terraform Post-Apply State

The first post-apply targeted plan attempted a live refresh and failed because
of a transient DNS lookup failure for the Glue endpoint:

```text
lookup glue.eu-west-2.amazonaws.com: no such host
```

Evidence:

```text
docs/evidence/glue-athena-iam-targeted-postapply-nochange-20260615.txt
```

The IAM-only post-apply plan was rerun with `-refresh=false` and returned no
changes:

```text
No changes. Your infrastructure matches the configuration.
```

Evidence:

```text
docs/evidence/glue-athena-iam-targeted-postapply-refreshfalse-20260615.txt
```

The normal root plan remains intentionally blocked until unrelated
live-preservation variables are reconciled. Do not run a normal root apply from
the blocked plan.

## Outcome

The Glue least-privilege policy and dedicated Athena query role are deployed and
live verified. The current raw -> Glue -> curated Parquet -> Athena validation
chain is also verified through the successful Glue run and Athena query.
