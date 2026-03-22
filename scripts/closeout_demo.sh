#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-eu-west-2}"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
DATE_TAG="$(date +%Y%m%d)"

PROJECT_PREFIX="energy-market"
BUCKET="${S3_BUCKET:-energy-market-lake-${ACCOUNT_ID}-${DATE_TAG}}"
LAMBDA_ROLE_NAME="${PROJECT_PREFIX}-lambda-role"
LAMBDA_FUNCTION_NAME="${PROJECT_PREFIX}-elexon-ingest"
EVENTBRIDGE_RULE_NAME="${PROJECT_PREFIX}-daily-ingestion"
GLUE_ROLE_NAME="${PROJECT_PREFIX}-glue-role"
GLUE_DATABASE_NAME="energy_market_lake"
RAW_CRAWLER_NAME="${PROJECT_PREFIX}-raw-crawler"
CURATED_CRAWLER_NAME="${PROJECT_PREFIX}-curated-crawler"
GLUE_JOB_NAME="${PROJECT_PREFIX}-etl-raw-to-parquet"
ATHENA_RESULTS_PREFIX="athena-results/"
BACKFILL_DAYS="${BACKFILL_DAYS:-30}"
ELEXON_BASE_URL="${ELEXON_BASE_URL:-https://data.elexon.co.uk/bmrs/api/v1}"
HTTP_TIMEOUT_SECONDS="${HTTP_TIMEOUT_SECONDS:-30}"

ENTSOE_BASE_URL="${ENTSOE_BASE_URL:-https://web-api.tp.entsoe.eu/api}"
ENTSOE_TOKEN="${ENTSOE_TOKEN:-}"
ENTSOE_ZONES="${ENTSOE_ZONES:-GB,FR,DE,NL}"

ENTSOG_BASE_URL="${ENTSOG_BASE_URL:-https://transparency.entsog.eu/api/v1}"
ENTSOG_POINT_DIRECTIONS="${ENTSOG_POINT_DIRECTIONS:-}"
ENTSOG_FLOW_INDICATOR="${ENTSOG_FLOW_INDICATOR:-Physical Flow}"
ENTSOG_DEMAND_INDICATOR="${ENTSOG_DEMAND_INDICATOR:-Allocation}"
ENTSOG_PERIOD_TYPE="${ENTSOG_PERIOD_TYPE:-day}"
ENTSOG_TIMEZONE="${ENTSOG_TIMEZONE:-WET}"
ENTSOG_LIMIT="${ENTSOG_LIMIT:-1000}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP_DIR="/tmp/${PROJECT_PREFIX}-closeout"
mkdir -p "${TMP_DIR}"

echo "Using bucket: ${BUCKET}"

export BUCKET
export ELEXON_BASE_URL
export BACKFILL_DAYS
export HTTP_TIMEOUT_SECONDS
export ENTSOE_BASE_URL
export ENTSOE_TOKEN
export ENTSOE_ZONES
export ENTSOG_BASE_URL
export ENTSOG_POINT_DIRECTIONS
export ENTSOG_FLOW_INDICATOR
export ENTSOG_DEMAND_INDICATOR
export ENTSOG_PERIOD_TYPE
export ENTSOG_TIMEZONE
export ENTSOG_LIMIT

python3 - <<'PY' > "${TMP_DIR}/lambda-environment.json"
import json
import os

environment = {
    "Variables": {
        "S3_BUCKET": os.environ["BUCKET"],
        "ELEXON_BASE_URL": os.environ["ELEXON_BASE_URL"],
        "BACKFILL_DAYS": os.environ["BACKFILL_DAYS"],
        "HTTP_TIMEOUT_SECONDS": os.environ["HTTP_TIMEOUT_SECONDS"],
        "ENTSOE_BASE_URL": os.environ["ENTSOE_BASE_URL"],
        "ENTSOE_TOKEN": os.environ["ENTSOE_TOKEN"],
        "ENTSOE_ZONES": os.environ["ENTSOE_ZONES"],
        "ENTSOG_BASE_URL": os.environ["ENTSOG_BASE_URL"],
        "ENTSOG_POINT_DIRECTIONS": os.environ["ENTSOG_POINT_DIRECTIONS"],
        "ENTSOG_FLOW_INDICATOR": os.environ["ENTSOG_FLOW_INDICATOR"],
        "ENTSOG_DEMAND_INDICATOR": os.environ["ENTSOG_DEMAND_INDICATOR"],
        "ENTSOG_PERIOD_TYPE": os.environ["ENTSOG_PERIOD_TYPE"],
        "ENTSOG_TIMEZONE": os.environ["ENTSOG_TIMEZONE"],
        "ENTSOG_LIMIT": os.environ["ENTSOG_LIMIT"],
    }
}

print(json.dumps(environment))
PY

if ! aws s3api head-bucket --bucket "${BUCKET}" 2>/dev/null; then
  aws s3api create-bucket \
    --bucket "${BUCKET}" \
    --region "${REGION}" \
    --create-bucket-configuration LocationConstraint="${REGION}"
fi

aws s3api put-public-access-block \
  --bucket "${BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3api put-bucket-encryption \
  --bucket "${BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'

cat > "${TMP_DIR}/lifecycle.json" <<JSON
{
  "Rules": [
    {
      "ID": "raw-lifecycle",
      "Prefix": "raw/",
      "Status": "Enabled",
      "Transitions": [
        { "Days": 30, "StorageClass": "STANDARD_IA" },
        { "Days": 90, "StorageClass": "GLACIER" }
      ],
      "Expiration": { "Days": 180 }
    }
  ]
}
JSON
aws s3api put-bucket-lifecycle-configuration \
  --bucket "${BUCKET}" \
  --lifecycle-configuration "file://${TMP_DIR}/lifecycle.json"

cat > "${TMP_DIR}/lambda-trust.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "lambda.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

if ! aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --assume-role-policy-document "file://${TMP_DIR}/lambda-trust.json" >/dev/null
fi

aws iam attach-role-policy \
  --role-name "${LAMBDA_ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole >/dev/null

cat > "${TMP_DIR}/lambda-s3-policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectAcl", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${BUCKET}",
        "arn:aws:s3:::${BUCKET}/*"
      ]
    }
  ]
}
JSON
aws iam put-role-policy \
  --role-name "${LAMBDA_ROLE_NAME}" \
  --policy-name "${PROJECT_PREFIX}-lambda-s3-policy" \
  --policy-document "file://${TMP_DIR}/lambda-s3-policy.json"

LAMBDA_ROLE_ARN="$(aws iam get-role --role-name "${LAMBDA_ROLE_NAME}" --query 'Role.Arn' --output text)"

python3 -m zipfile -c "${TMP_DIR}/ingest_elexon.zip" "${ROOT_DIR}/lambda/ingest_elexon.py"

if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws lambda update-function-code \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --zip-file "fileb://${TMP_DIR}/ingest_elexon.zip" \
    --region "${REGION}" >/dev/null

  aws lambda update-function-configuration \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --role "${LAMBDA_ROLE_ARN}" \
    --runtime python3.11 \
    --handler ingest_elexon.lambda_handler \
    --timeout 900 \
    --memory-size 256 \
    --environment "file://${TMP_DIR}/lambda-environment.json" \
    --region "${REGION}" >/dev/null
else
  aws lambda create-function \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --runtime python3.11 \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler ingest_elexon.lambda_handler \
    --timeout 900 \
    --memory-size 256 \
    --zip-file "fileb://${TMP_DIR}/ingest_elexon.zip" \
    --environment "file://${TMP_DIR}/lambda-environment.json" \
    --region "${REGION}" >/dev/null
fi

aws events put-rule \
  --name "${EVENTBRIDGE_RULE_NAME}" \
  --schedule-expression "cron(0 2 * * ? *)" \
  --state ENABLED \
  --region "${REGION}" >/dev/null

aws events put-targets \
  --rule "${EVENTBRIDGE_RULE_NAME}" \
  --targets "Id"="1","Arn"="$(aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${REGION}" --query 'Configuration.FunctionArn' --output text)" \
  --region "${REGION}" >/dev/null

aws lambda add-permission \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --statement-id "${EVENTBRIDGE_RULE_NAME}-invoke" \
  --action lambda:InvokeFunction \
  --principal events.amazonaws.com \
  --source-arn "arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${EVENTBRIDGE_RULE_NAME}" \
  --region "${REGION}" >/dev/null 2>&1 || true

aws lambda invoke \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --payload '{}' \
  --cli-binary-format raw-in-base64-out \
  --region "${REGION}" \
  "${TMP_DIR}/ingest-result.json" >/dev/null

cat > "${TMP_DIR}/glue-trust.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "glue.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

if ! aws iam get-role --role-name "${GLUE_ROLE_NAME}" >/dev/null 2>&1; then
  aws iam create-role \
    --role-name "${GLUE_ROLE_NAME}" \
    --assume-role-policy-document "file://${TMP_DIR}/glue-trust.json" >/dev/null
fi

aws iam attach-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole >/dev/null

cat > "${TMP_DIR}/glue-s3-policy.json" <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${BUCKET}"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::${BUCKET}/*"]
    }
  ]
}
JSON
aws iam put-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-name "${PROJECT_PREFIX}-glue-s3-policy" \
  --policy-document "file://${TMP_DIR}/glue-s3-policy.json"

GLUE_ROLE_ARN="$(aws iam get-role --role-name "${GLUE_ROLE_NAME}" --query 'Role.Arn' --output text)"

aws glue create-database \
  --database-input "{\"Name\":\"${GLUE_DATABASE_NAME}\"}" \
  --region "${REGION}" >/dev/null 2>&1 || true

aws s3 cp "${ROOT_DIR}/glue/etl_raw_to_parquet.py" "s3://${BUCKET}/scripts/etl_raw_to_parquet.py" --region "${REGION}" >/dev/null

if aws glue get-job --job-name "${GLUE_JOB_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws glue update-job \
    --job-name "${GLUE_JOB_NAME}" \
    --job-update "Role=${GLUE_ROLE_ARN},Command={Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/etl_raw_to_parquet.py,PythonVersion=3},GlueVersion=4.0,NumberOfWorkers=2,WorkerType=G.1X,ExecutionProperty={MaxConcurrentRuns=1}" \
    --region "${REGION}" >/dev/null
else
  aws glue create-job \
    --name "${GLUE_JOB_NAME}" \
    --role "${GLUE_ROLE_ARN}" \
    --command "Name=glueetl,ScriptLocation=s3://${BUCKET}/scripts/etl_raw_to_parquet.py,PythonVersion=3" \
    --glue-version "4.0" \
    --number-of-workers 2 \
    --worker-type G.1X \
    --execution-property MaxConcurrentRuns=1 \
    --region "${REGION}" >/dev/null
fi

if aws glue get-crawler --name "${RAW_CRAWLER_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws glue update-crawler \
    --name "${RAW_CRAWLER_NAME}" \
    --role "${GLUE_ROLE_ARN}" \
    --database-name "${GLUE_DATABASE_NAME}" \
    --targets "S3Targets=[{Path=s3://${BUCKET}/raw/}]" \
    --table-prefix "raw_" \
    --region "${REGION}" >/dev/null
else
  aws glue create-crawler \
    --name "${RAW_CRAWLER_NAME}" \
    --role "${GLUE_ROLE_ARN}" \
    --database-name "${GLUE_DATABASE_NAME}" \
    --targets "S3Targets=[{Path=s3://${BUCKET}/raw/}]" \
    --table-prefix "raw_" \
    --region "${REGION}" >/dev/null
fi

aws glue start-crawler --name "${RAW_CRAWLER_NAME}" --region "${REGION}" >/dev/null 2>&1 || true
while true; do
  CRAWLER_STATE="$(aws glue get-crawler --name "${RAW_CRAWLER_NAME}" --region "${REGION}" --query 'Crawler.State' --output text)"
  [[ "${CRAWLER_STATE}" == "READY" ]] && break
  sleep 10
done

JOB_RUN_ID="$(aws glue start-job-run \
  --job-name "${GLUE_JOB_NAME}" \
  --arguments "{\"--RAW_PATH\":\"s3://${BUCKET}/raw\",\"--CURATED_PATH\":\"s3://${BUCKET}/curated\"}" \
  --region "${REGION}" \
  --query 'JobRunId' --output text)"

while true; do
  JOB_STATE="$(aws glue get-job-run --job-name "${GLUE_JOB_NAME}" --run-id "${JOB_RUN_ID}" --region "${REGION}" --query 'JobRun.JobRunState' --output text)"
  if [[ "${JOB_STATE}" == "SUCCEEDED" ]]; then
    break
  fi
  if [[ "${JOB_STATE}" == "FAILED" || "${JOB_STATE}" == "STOPPED" || "${JOB_STATE}" == "TIMEOUT" ]]; then
    echo "Glue job ended with state: ${JOB_STATE}" >&2
    exit 1
  fi
  sleep 15
done

if aws glue get-crawler --name "${CURATED_CRAWLER_NAME}" --region "${REGION}" >/dev/null 2>&1; then
  aws glue update-crawler \
    --name "${CURATED_CRAWLER_NAME}" \
    --role "${GLUE_ROLE_ARN}" \
    --database-name "${GLUE_DATABASE_NAME}" \
    --targets "S3Targets=[{Path=s3://${BUCKET}/curated/dataset=electricity/}]" \
    --table-prefix "curated_" \
    --region "${REGION}" >/dev/null
else
  aws glue create-crawler \
    --name "${CURATED_CRAWLER_NAME}" \
    --role "${GLUE_ROLE_ARN}" \
    --database-name "${GLUE_DATABASE_NAME}" \
    --targets "S3Targets=[{Path=s3://${BUCKET}/curated/dataset=electricity/}]" \
    --table-prefix "curated_" \
    --region "${REGION}" >/dev/null
fi

mapfile -t CURATED_TABLES < <(
  aws glue get-tables \
    --database-name "${GLUE_DATABASE_NAME}" \
    --region "${REGION}" \
    --query "TableList[?starts_with(Name, \`curated_dataset_electricity\`)].Name" \
    --output text
)
for CURATED_TABLE in "${CURATED_TABLES[@]}"; do
  [[ -z "${CURATED_TABLE}" ]] && continue
  aws glue delete-table \
    --database-name "${GLUE_DATABASE_NAME}" \
    --name "${CURATED_TABLE}" \
    --region "${REGION}" >/dev/null 2>&1 || true
done

aws glue start-crawler --name "${CURATED_CRAWLER_NAME}" --region "${REGION}" >/dev/null 2>&1 || true
while true; do
  CRAWLER_STATE="$(aws glue get-crawler --name "${CURATED_CRAWLER_NAME}" --region "${REGION}" --query 'Crawler.State' --output text)"
  [[ "${CRAWLER_STATE}" == "READY" ]] && break
  sleep 10
done

mkdir -p "${ROOT_DIR}/docs/evidence"
EVIDENCE_FILE="${ROOT_DIR}/docs/evidence/run-$(date +%Y%m%d-%H%M%S).md"
SCHEMA_EVIDENCE_FILE="${ROOT_DIR}/docs/evidence/athena-schema-$(date +%Y%m%d-%H%M%S).md"
EXPECTED_SOURCES="elexon"
if [[ -n "${ENTSOE_TOKEN}" ]]; then
  EXPECTED_SOURCES="${EXPECTED_SOURCES},entsoe"
fi

python3 "${ROOT_DIR}/scripts/validate_athena_schema.py" \
  --region "${REGION}" \
  --database "${GLUE_DATABASE_NAME}" \
  --table "curated_dataset_electricity" \
  --output-location "s3://${BUCKET}/${ATHENA_RESULTS_PREFIX}" \
  --expected-sources "${EXPECTED_SOURCES}" \
  --output-file "${SCHEMA_EVIDENCE_FILE}"

{
  echo "# Demo Run Evidence"
  echo
  echo "- Timestamp (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Region: ${REGION}"
  echo "- Bucket: ${BUCKET}"
  echo "- Lambda: ${LAMBDA_FUNCTION_NAME}"
  echo "- Glue DB: ${GLUE_DATABASE_NAME}"
  echo "- Glue Job: ${GLUE_JOB_NAME}"
  echo "- Glue Job Run ID: ${JOB_RUN_ID}"
  echo "- Athena schema validation: ${SCHEMA_EVIDENCE_FILE}"
  echo
  echo "## Ingestion Result"
  cat "${TMP_DIR}/ingest-result.json"
  echo
  echo "## Raw Prefix Count (All Sources)"
  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "raw/" --query 'length(Contents)' --output text
  echo
  echo "## Raw Prefix Count (Elexon)"
  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "raw/source=elexon/" --query 'length(Contents)' --output text
  echo
  echo "## Raw Prefix Count (ENTSO-E)"
  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "raw/source=entsoe/" --query 'length(Contents)' --output text
  echo
  echo "## Raw Prefix Count (ENTSOG)"
  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "raw/source=entsog/" --query 'length(Contents)' --output text
  echo
  echo "## Curated Prefix Count (Electricity)"
  aws s3api list-objects-v2 --bucket "${BUCKET}" --prefix "curated/dataset=electricity/" --query 'length(Contents)' --output text
} > "${EVIDENCE_FILE}"

echo "Closeout complete. Evidence file:"
echo "${EVIDENCE_FILE}"
echo "Bucket: ${BUCKET}"
