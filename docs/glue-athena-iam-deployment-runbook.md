# Glue And Athena IAM Deployment Runbook

<!-- markdownlint-disable MD013 -->

## Purpose

Deploy and verify the June-July lakehouse IAM closure change:

- update the Glue S3 inline policy from whole-bucket object access to required
  prefixes only;
- create the dedicated Athena query role; and
- prove the live Glue and Athena paths still work after the IAM change.

This runbook implements ADR 0004 and supports the SAP-C02 tracker lakehouse
closure checklist. It must not be used for unrelated infrastructure changes.

## Preconditions

- Explicit approval has been granted for the IAM deployment and live Glue/Athena
  verification.
- The working tree has been reviewed so unrelated Terraform changes are not
  bundled into the apply.
- AWS credentials point at account `464975959576` in Region `eu-west-2`.
- The Terraform backend is initialized for `infra/terraform/lakehouse`.
- `terraform.tfvars` preserves the current live dashboard, managed workflow,
  budget, SNS, and schedule settings.

## Command Sequence

Set common shell variables:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake

TF_ROOT=infra/terraform/lakehouse
PLAN_FILE=/tmp/lakehouse-glue-athena-iam-live.tfplan
AWS_REGION=eu-west-2
DATA_BUCKET=energy-market-lake-464975959576-20260405
GLUE_ROLE=energy-market-glue-role
GLUE_POLICY=energy-market-glue-s3-policy
GLUE_JOB=energy-market-etl-raw-to-parquet
RAW_CRAWLER=energy-market-raw-crawler
CURATED_CRAWLER=energy-market-curated-crawler
GLUE_DATABASE=energy_market_lake
ATHENA_WORKGROUP=energy-market-workgroup
ATHENA_ROLE_NAME=energy-market-athena-query-role
```

Validate local policy contracts:

```bash
python3 scripts/check_lakehouse_iam_policies.py
terraform -chdir="${TF_ROOT}" fmt -check
terraform -chdir="${TF_ROOT}" validate
```

These commands prove the Terraform policy shape still matches the documented
Glue and Athena boundaries before a live apply is considered.

Create and review the normal root plan:

```bash
terraform -chdir="${TF_ROOT}" plan -out="${PLAN_FILE}"
terraform -chdir="${TF_ROOT}" show -no-color "${PLAN_FILE}" \
  > docs/evidence/glue-athena-iam-apply-plan-20260615.txt
```

The plan should show only the intended IAM delta: create
`aws_iam_role.athena_query`, create `aws_iam_role_policy.athena_query`, and
update `aws_iam_role_policy.glue_s3` in place. Stop if it includes destroys,
unrelated resource changes, schedule changes, dashboard changes, Lambda
deployment changes, Step Functions changes, or Bedrock changes.

If the normal root plan is blocked by unrelated live-preservation drift, save
that blocked plan as evidence. A targeted IAM-only plan may then be used for
this closure step only when it contains exactly the approved IAM resources and
no destroys:

```bash
terraform -chdir="${TF_ROOT}" plan \
  -target=aws_iam_role_policy.glue_s3 \
  -target=aws_iam_role.athena_query \
  -target=aws_iam_role_policy.athena_query \
  -out="${PLAN_FILE}"

terraform -chdir="${TF_ROOT}" show -no-color "${PLAN_FILE}" \
  > docs/evidence/glue-athena-iam-targeted-apply-plan-20260615.txt
```

This is an exception path for isolating the approved IAM change from unrelated
root drift. Do not use the targeted plan if it includes non-IAM resources,
destroys, or replacement actions.

Apply the reviewed plan:

```bash
terraform -chdir="${TF_ROOT}" apply "${PLAN_FILE}" \
  | tee docs/evidence/glue-athena-iam-apply-20260615.txt
```

This is the only mutating infrastructure command in the runbook. It applies the
saved plan, not a freshly generated plan.

Capture the live IAM policy and role state:

```bash
aws iam get-role-policy \
  --role-name "${GLUE_ROLE}" \
  --policy-name "${GLUE_POLICY}" \
  --output json \
  > docs/evidence/glue-s3-policy-live-20260615.json

aws iam get-role \
  --role-name "${ATHENA_ROLE_NAME}" \
  --output json \
  > docs/evidence/athena-query-role-live-20260615.json

aws iam get-role-policy \
  --role-name "${ATHENA_ROLE_NAME}" \
  --policy-name "energy-market-athena-query-policy" \
  --output json \
  > docs/evidence/athena-query-policy-live-20260615.json
```

These commands prove which policy documents are attached in AWS after the apply.

Verify Glue can still use the restricted role:

```bash
aws glue start-crawler \
  --name "${RAW_CRAWLER}" \
  --region "${AWS_REGION}" || true

aws glue start-job-run \
  --job-name "${GLUE_JOB}" \
  --region "${AWS_REGION}" \
  --query JobRunId \
  --output text

aws glue start-crawler \
  --name "${CURATED_CRAWLER}" \
  --region "${AWS_REGION}" || true
```

`start-crawler` can return `CrawlerRunningException` if a crawler is already
active; that is not an IAM failure. The Glue job run is the stronger test
because it loads the script from `scripts/`, reads `raw/`, and writes
`curated/`.

For each Glue job or crawler run, capture final state with:

```bash
aws glue get-job-run \
  --job-name "${GLUE_JOB}" \
  --run-id "<job-run-id>" \
  --region "${AWS_REGION}" \
  --output json

aws glue get-crawler \
  --name "${CURATED_CRAWLER}" \
  --region "${AWS_REGION}" \
  --query 'Crawler.{Name:Name,State:State,LastCrawl:LastCrawl}' \
  --output json
```

Assume the dedicated Athena query role:

```bash
ATHENA_ROLE_ARN="$(terraform -chdir="${TF_ROOT}" output -raw athena_query_role_arn)"

aws sts assume-role \
  --role-arn "${ATHENA_ROLE_ARN}" \
  --role-session-name "lakehouse-athena-verify-$(date -u +%Y%m%dT%H%M%SZ)" \
  --duration-seconds 900 \
  > /tmp/lakehouse-athena-query-role-credentials.json

export AWS_ACCESS_KEY_ID="$(jq -r '.Credentials.AccessKeyId' /tmp/lakehouse-athena-query-role-credentials.json)"
export AWS_SECRET_ACCESS_KEY="$(jq -r '.Credentials.SecretAccessKey' /tmp/lakehouse-athena-query-role-credentials.json)"
export AWS_SESSION_TOKEN="$(jq -r '.Credentials.SessionToken' /tmp/lakehouse-athena-query-role-credentials.json)"

aws sts get-caller-identity --output json
```

This proves the role can be assumed by an approved same-account principal. The
temporary credentials file must stay outside the repository and be deleted after
verification.

Verify the Athena role can use only the intended S3 prefixes:

```bash
aws s3api list-objects-v2 \
  --bucket "${DATA_BUCKET}" \
  --prefix curated/ \
  --max-keys 1 \
  --region "${AWS_REGION}" \
  --output json

aws s3api list-objects-v2 \
  --bucket "${DATA_BUCKET}" \
  --prefix athena-results/ \
  --max-keys 1 \
  --region "${AWS_REGION}" \
  --output json

aws s3api list-objects-v2 \
  --bucket "${DATA_BUCKET}" \
  --prefix raw/ \
  --max-keys 1 \
  --region "${AWS_REGION}" \
  --output json
```

The `curated/` and `athena-results/` checks should succeed. The `raw/` check
should fail with `AccessDenied`, proving the query role cannot list the raw
zone.

Run a small Athena query with the assumed role:

```bash
QUERY_ID="$(aws athena start-query-execution \
  --work-group "${ATHENA_WORKGROUP}" \
  --query-execution-context Database="${GLUE_DATABASE}" \
  --query-string "SELECT count(*) AS row_count FROM curated_dataset_gas" \
  --region "${AWS_REGION}" \
  --query QueryExecutionId \
  --output text)"

aws athena get-query-execution \
  --query-execution-id "${QUERY_ID}" \
  --region "${AWS_REGION}" \
  --output json

aws athena get-query-results \
  --query-execution-id "${QUERY_ID}" \
  --region "${AWS_REGION}" \
  --output json
```

This proves the dedicated role can read the Glue catalog, read curated Parquet
through Athena, and write/read the bounded Athena result object.

Unset and remove temporary credentials:

```bash
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
rm -f /tmp/lakehouse-athena-query-role-credentials.json
```

Confirm Terraform post-apply state:

```bash
terraform -chdir="${TF_ROOT}" plan -detailed-exitcode -no-color \
  > docs/evidence/glue-athena-iam-postapply-terraform-nochange-20260615.txt
```

Exit code `0` means Terraform sees no further changes. Exit code `2` means a
diff remains and must be reviewed before the closure item is marked live
verified.

## Rollback

Rollback requires explicit approval. Use a reviewed saved plan; do not broaden
another role as a shortcut. If rollback is approved, restore the previous Glue
policy in Terraform, remove or disable the Athena query role only after
confirming it has no approved dependants, run a normal plan, and apply the
saved rollback plan.
