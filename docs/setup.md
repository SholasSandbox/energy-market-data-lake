# Setup Guide (eu-west-2)

<!-- markdownlint-disable MD013 -->

This is a lightweight, budget-conscious setup. Use the default settings unless
you need additional scale.

## Current Demo Scope

The local MVP is implemented and is the recommended path for demos and learning.
It proves:

- energy input evidence
- curated RSS/news evidence
- AI input bundle evidence
- deterministic local AI insight output
- schema validation for good samples
- rejection of known-bad samples
- public-safe dashboard snapshot JSON
- React dashboard display from `dashboard_snapshot_v1.sample.json`

The AWS/serverless extension remains the target deployment path for Step
Functions, CloudFront, SNS alerts, and a managed OpenClaw or Bedrock runtime.

## AWS Closeout Script (Optional)

If you have AWS credentials configured and want to run the existing serverless
energy lakehouse closeout flow:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
```

This script provisions S3 + Lambda + EventBridge + Glue + crawlers and stores
run evidence in `docs/evidence/`. It is separate from the local news + AI MVP
pipeline below.

## Local Python Setup

Use a virtual environment for local helper scripts. This keeps project
dependencies separate from your Mac's global Python installation.

Start here after restarting your terminal:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

If `.venv` already exists, you only need:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
source .venv/bin/activate
```

Validate the JSON schema contracts:

```bash
python scripts/validate_contracts.py
```

## Local MVP Fast Path

Run this from the repo root when resuming work or preparing the demo:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
source .venv/bin/activate
export AWS_REGION=eu-west-2
export S3_BUCKET=energy-market-lake-464975959576-20260405
python scripts/generate_dashboard.py \
  --region "${AWS_REGION}" \
  --bucket "${S3_BUCKET}" \
  --output-location "s3://${S3_BUCKET}/athena-results/" \
  --output-json dashboard-ui/public/dashboard-data.json
python scripts/ingest_news_local.py
python scripts/export_energy_input_local.py
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence --check-failures
```

Expected result:

```text
All contracts are valid.
```

This command sequence creates or refreshes:

- `dashboard-ui/public/dashboard-data.json`
- `docs/evidence/energy_input_v1.sample.json`
- `docs/evidence/curated/news_summary_v1.sample.json`
- `docs/evidence/ai/ai_input_bundle_v1.sample.json`
- `docs/evidence/curated/ai_insight_v1.sample.json`
- `dashboard-ui/public/dashboard_snapshot_v1.sample.json`

It also confirms these known-bad files are rejected:

- `docs/evidence/failed/bad_energy_input_v1.sample.json`
- `docs/evidence/failed/bad_ai_insight_v1.sample.json`

Generate and validate the curated local news summary evidence:

```bash
python scripts/ingest_news_local.py
python scripts/validate_contracts.py --include-evidence
```

The default local RSS refresh pulls a broader mixed-energy feed set and caps the curated evidence at 18 articles. The public dashboard snapshot exposes the top 12 article cards as dashboard-safe `news_articles`.

To tune the local news refresh:

```bash
python scripts/ingest_news_local.py \
  --limit-per-feed 4 \
  --max-articles 18
```

Generate and validate the local energy input evidence:

```bash
python scripts/generate_dashboard.py \
  --bucket "${S3_BUCKET}" \
  --output-json dashboard-ui/public/dashboard-data.json
python scripts/export_energy_input_local.py
python scripts/validate_contracts.py --include-evidence
```

Publish a public-safe local dashboard snapshot:

```bash
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence
```

Create and validate local AI merge evidence:

```bash
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/validate_contracts.py --include-evidence
```

Confirm known-bad samples are rejected:

```bash
python scripts/validate_contracts.py --include-evidence --check-failures
```

Expected result:

```text
All contracts are valid.
```

## React Dashboard Setup

Start the dashboard from a second terminal:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Open:

```text
http://127.0.0.1:5173/
```

Verify the dashboard and public snapshot JSON are served:

```bash
curl -I http://127.0.0.1:5173/
curl -I http://127.0.0.1:5173/dashboard_snapshot_v1.sample.json
```

Expected result for both:

```text
HTTP/1.1 200 OK
```

The dashboard should read only `dashboard_snapshot_v1.sample.json`, not raw,
curated, failed, or audit evidence directly.

## 1) S3 Buckets

Create one bucket:

- `energy-market-lake-<your-unique-suffix>`

Enable:

- Block public access
- Server-side encryption (SSE-S3 or SSE-KMS)

## 2) S3 Lifecycle Policies

Add a lifecycle rule for the `raw/` prefix:

- Transition to Standard-IA after 30 days
- Transition to Glacier Flexible after 90 days
- Optional: Expire after 180 days

Keep `curated/` in Standard or Intelligent-Tiering if queried often.

## 3) IAM Roles

Create roles with least privilege:

- Lambda role: write to `s3://.../raw/*`, read secrets, write logs
- Glue role: read raw, write curated, read catalog
- Athena: default workgroup + query result location in S3

## 4) Lambda Ingestion (App 1)

Create a Lambda function (Python 3.x):

- Source: Elexon API (no key)
- Output: JSON in S3 `raw/` prefix
- Targets for this phase:
  - Demand by bidding zone (GSP proxy): `/datasets/ATL`
  - System prices (includes SBP/SSP imbalance prices): `/balancing/settlement/system-prices/{settlementDate}`

Required Lambda env vars:

- `S3_BUCKET`
- `ELEXON_BASE_URL` (default: `https://data.elexon.co.uk/bmrs/api/v1`)
- `BACKFILL_DAYS` (default: `30`)

Find the target S3 bucket from the CLI when the AWS Console is unavailable:

```bash
aws s3api list-buckets \
  --query 'Buckets[?starts_with(Name, `energy-market-lake-`)].Name' \
  --output table
```

Optional (future):

- `ENTSOE_BASE_URL` (default: `https://web-api.tp.entsoe.eu/api`)
- `ENTSOE_TOKEN` (stored in SSM/Secrets Manager)
- `ENTSOE_ZONES` (default: `GB,FR,DE,NL`)
- `ENTSOG_BASE_URL` (default: `https://transparency.entsog.eu/api/v1`)
- `ENTSOG_POINT_DIRECTIONS` (comma-separated pointDirection IDs)
- `ENTSOG_FLOW_INDICATOR` (default: `Physical Flow`)
- `ENTSOG_DEMAND_INDICATOR` (default: `Allocation`)
- `ENTSOG_PERIOD_TYPE` (default: `day`)
- `ENTSOG_TIMEZONE` (default: `WET`)
- `ENTSOG_LIMIT` (default: `1000`)
- `ENTSOG_INCLUDE_EXEMPTIONS` (default: `0`)

Schedule with EventBridge (daily at 02:00 UTC).

Code reference:

- Lambda handler: `lambda/ingest_elexon.py`
- S3 output keys:
  - `raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json`
  - `raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json`
  - `raw/source=entsoe/dataset=actual_load/zone=<zone>/date=YYYY-MM-DD/payload.xml`
  - `raw/source=entsoe/dataset=day_ahead_prices/zone=<zone>/date=YYYY-MM-DD/payload.xml`
  - `raw/source=entsog/dataset=gas_flow/point_direction=<id>/date=YYYY-MM-DD/payload.json`
  - `raw/source=entsog/dataset=gas_demand/point_direction=<id>/date=YYYY-MM-DD/payload.json`

ENTSO-E zone mapping (default):

- GB: `10YGB----------A`
- FR: `10YFR-RTE------C`
- DE (DE-LU bidding zone): `10Y1001A1001A82H`
- NL: `10YNL----------L`

ENTSO-E datasets:

- Actual load: `documentType=A65`, `processType=A16`, `outBiddingZone_Domain=<EIC>`
- Day-ahead prices: `documentType=A44`, `in_Domain=<EIC>`, `out_Domain=<EIC>`

ENTSOG pointDirection setup:

1) Query `operatorpointdirections` to list available IDs with `hasData=true`.
2) Filter to the countries you want (GB, FR, DE, NL). The helper expands GB to UK because ENTSOG uses `UK` in gas metadata.
3) Copy the queryable `pointDirection` IDs into `ENTSOG_POINT_DIRECTIONS`.

Helper script:

```bash
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL
```

IDs-only output (ready to paste into `ENTSOG_POINT_DIRECTIONS`):

```bash
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --ids-only
```

Small validated seed set:

```bash
ENTSOG_POINT_DIRECTIONS=BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
```

Live-check a seed set before using it in Lambda:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "${ENTSOG_POINT_DIRECTIONS}" \
  --date 2026-05-03
```

Update Lambda runtime environment for the first small ENTSOG run:

```bash
export AWS_REGION=eu-west-2
export LAMBDA_FUNCTION_NAME=energy-market-elexon-ingest
export S3_BUCKET=energy-market-lake-your-real-suffix
export BACKFILL_DAYS=1
export HTTP_TIMEOUT_SECONDS=30
export ENTSOG_BASE_URL=https://transparency.entsog.eu/api/v1
export ENTSOG_POINT_DIRECTIONS=BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
export ENTSOG_FLOW_INDICATOR="Physical Flow"
export ENTSOG_DEMAND_INDICATOR=Allocation
export ENTSOG_PERIOD_TYPE=day
export ENTSOG_TIMEZONE=WET
export ENTSOG_LIMIT=1000
export ENTSOG_INCLUDE_EXEMPTIONS=0
```

Replace `energy-market-lake-your-real-suffix` with the real bucket name. Do not paste angle-bracket placeholders such as `<your-unique-suffix>` into `zsh`; `<` and `>` are shell redirection characters.

If you need to find the bucket:

```bash
aws s3api list-buckets \
  --query 'Buckets[?starts_with(Name, `energy-market-lake-`)].Name' \
  --output table
```

Live-check the exact values before updating Lambda:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "${ENTSOG_POINT_DIRECTIONS}" \
  --date 2026-05-03 \
  --output-file docs/evidence/entsog-seed-check.md
```

Merge the gas variables into the existing Lambda environment. This avoids accidentally deleting existing variables such as `ELEXON_BASE_URL`, `ENTSOE_BASE_URL`, `ENTSOE_TOKEN`, or `ENTSOE_ZONES`.

```bash
aws lambda get-function-configuration \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Environment.Variables' \
  --output json > /tmp/energy-market-lambda-env-current.json

python3 - <<'PY' > /tmp/energy-market-lambda-env-updated.json
import json
import os
from pathlib import Path

current = json.loads(Path("/tmp/energy-market-lambda-env-current.json").read_text())
updates = {
    "S3_BUCKET": os.environ["S3_BUCKET"],
    "BACKFILL_DAYS": os.environ["BACKFILL_DAYS"],
    "HTTP_TIMEOUT_SECONDS": os.environ["HTTP_TIMEOUT_SECONDS"],
    "ENTSOG_BASE_URL": os.environ["ENTSOG_BASE_URL"],
    "ENTSOG_POINT_DIRECTIONS": os.environ["ENTSOG_POINT_DIRECTIONS"],
    "ENTSOG_FLOW_INDICATOR": os.environ["ENTSOG_FLOW_INDICATOR"],
    "ENTSOG_DEMAND_INDICATOR": os.environ["ENTSOG_DEMAND_INDICATOR"],
    "ENTSOG_PERIOD_TYPE": os.environ["ENTSOG_PERIOD_TYPE"],
    "ENTSOG_TIMEZONE": os.environ["ENTSOG_TIMEZONE"],
    "ENTSOG_LIMIT": os.environ["ENTSOG_LIMIT"],
    "ENTSOG_INCLUDE_EXEMPTIONS": os.environ["ENTSOG_INCLUDE_EXEMPTIONS"],
}
current.update(updates)
print(json.dumps({"Variables": current}))
PY

aws lambda update-function-configuration \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --environment "file:///tmp/energy-market-lambda-env-updated.json"

aws lambda wait function-updated \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}"
```

Confirm Lambda now has the intended ENTSOG runtime values:

```bash
aws lambda get-function-configuration \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Environment.Variables.{S3_BUCKET:S3_BUCKET,BACKFILL_DAYS:BACKFILL_DAYS,ENTSOG_POINT_DIRECTIONS:ENTSOG_POINT_DIRECTIONS,ENTSOG_FLOW_INDICATOR:ENTSOG_FLOW_INDICATOR,ENTSOG_DEMAND_INDICATOR:ENTSOG_DEMAND_INDICATOR,ENTSOG_PERIOD_TYPE:ENTSOG_PERIOD_TYPE,ENTSOG_TIMEZONE:ENTSOG_TIMEZONE,ENTSOG_LIMIT:ENTSOG_LIMIT,ENTSOG_INCLUDE_EXEMPTIONS:ENTSOG_INCLUDE_EXEMPTIONS}' \
  --output table
```

If `S3_BUCKET` changes, update the Lambda role S3 policy to the same bucket before invoking. Otherwise Lambda can read the new environment value but still fail every `PutObject` call with access denied.

```bash
cat > /tmp/energy-market-lambda-s3-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:PutObjectAcl", "s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${S3_BUCKET}",
        "arn:aws:s3:::${S3_BUCKET}/*"
      ]
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name energy-market-lambda-role \
  --policy-name energy-market-lambda-s3-policy \
  --policy-document file:///tmp/energy-market-lambda-s3-policy.json

aws iam get-role-policy \
  --role-name energy-market-lambda-role \
  --policy-name energy-market-lambda-s3-policy \
  --query 'PolicyDocument.Statement[0].Resource' \
  --output table
```

Deploy the current Lambda code package. Run this after changing `lambda/ingest_elexon.py`; environment updates alone do not update the function code.

```bash
(cd lambda && python3 -m zipfile -c /tmp/ingest_elexon.zip ingest_elexon.py)

aws lambda update-function-code \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --zip-file fileb:///tmp/ingest_elexon.zip

aws lambda wait function-updated \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}"
```

Confirm the deployed package contains the ENTSOG operational-data demand fix:

```bash
aws lambda get-function \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Code.Location' \
  --output text > /tmp/ingest_elexon_code_url.txt

rm -rf /tmp/ingest_elexon_deployed
mkdir -p /tmp/ingest_elexon_deployed
curl -sSL "$(cat /tmp/ingest_elexon_code_url.txt)" \
  -o /tmp/ingest_elexon_deployed/function.zip
unzip -q /tmp/ingest_elexon_deployed/function.zip \
  -d /tmp/ingest_elexon_deployed/unzipped

rg -n 'aggregatedData|ENTSOG_INCLUDE_EXEMPTIONS|includeExemptions' \
  /tmp/ingest_elexon_deployed/unzipped/ingest_elexon.py
```

Expected verification:

- `ENTSOG_INCLUDE_EXEMPTIONS` is present.
- `includeExemptions` is present.
- `aggregatedData` is absent.

Invoke Lambda for a deterministic single-day ENTSOG proof. The `date` payload avoids relying on same-day ENTSOG availability.

```bash
aws lambda invoke \
  --function-name "${LAMBDA_FUNCTION_NAME}" \
  --region "${AWS_REGION}" \
  --payload '{"date":"2026-05-03"}' \
  --cli-binary-format raw-in-base64-out \
  docs/evidence/entsog-lambda-invoke-result.json

cat docs/evidence/entsog-lambda-invoke-result.json
```

If `FunctionError` appears in the invoke metadata, inspect the response file and CloudWatch logs before continuing. If the response status is `partial`, inspect `warnings`; a same-day ENTSOG 404 usually means the selected gas day is not fully available yet.

Verify raw ENTSOG keys and inspect one flow and one demand payload:

```bash
export GAS_DATE=2026-05-03

aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "raw/source=entsog/dataset=gas_flow/" \
  --query "Contents[?contains(Key, 'date=${GAS_DATE}')].Key" \
  --output table

aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "raw/source=entsog/dataset=gas_demand/" \
  --query "Contents[?contains(Key, 'date=${GAS_DATE}')].Key" \
  --output table

aws s3 cp \
  "s3://${S3_BUCKET}/raw/source=entsog/dataset=gas_flow/point_direction=BE-TSO-0001ITP-00061entry/date=${GAS_DATE}/payload.json" \
  - | python3 -m json.tool

aws s3 cp \
  "s3://${S3_BUCKET}/raw/source=entsog/dataset=gas_demand/point_direction=BE-TSO-0001ITP-00061entry/date=${GAS_DATE}/payload.json" \
  - | python3 -m json.tool
```

Expected raw proof for the four-point seed:

- 4 `gas_flow` payloads.
- 4 `gas_demand` payloads.
- Flow sample has `indicator=Physical Flow`, `unit=kWh/d`, and a populated `value`.
- Demand sample has `indicator=Allocation`, `unit=kWh/d`, and a populated `value`.

Write into `config/sample.env`:

```bash
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --save-env
```

Local test (optional):

```bash
python scripts/run_ingestion_local.py \
  --bucket energy-market-lake-your-real-suffix \
  --backfill-days 2
```

EventBridge test payload (optional):

```json
{
  "source": "aws.events",
  "detail-type": "Scheduled Event",
  "detail": {},
  "region": "eu-west-2",
  "time": "2026-01-30T02:00:00Z"
}
```

Expected Lambda output for the deterministic ENTSOG proof:

```json
{
  "status": "ok",
  "s3_keys": [
    "raw/source=elexon/dataset=atl/date=2026-05-03/payload.json",
    "raw/source=elexon/dataset=system_prices/date=2026-05-03/payload.json",
    "raw/source=entsog/dataset=gas_flow/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json",
    "raw/source=entsog/dataset=gas_demand/point_direction=BE-TSO-0001ITP-00061entry/date=2026-05-03/payload.json"
  ],
  "warnings": []
}
```

Post-run S3 checklist:

- `raw/source=elexon/dataset=atl/date=YYYY-MM-DD/payload.json` exists
- `raw/source=elexon/dataset=system_prices/date=YYYY-MM-DD/payload.json` exists
- `raw/source=entsog/dataset=gas_flow/point_direction=<id>/date=YYYY-MM-DD/payload.json` exists
- `raw/source=entsog/dataset=gas_demand/point_direction=<id>/date=YYYY-MM-DD/payload.json` exists
- File size > 0 bytes for all expected datasets

## 5) Glue Crawler + ETL (App 2)

- Create a Glue Crawler on the `raw/` prefix
- Run crawler daily (or after ingestion)
- Create Glue ETL job to convert raw -> Parquet

Partition by:

- `source`
- `region`
- `date`

Write to `curated/` prefix.

AWS API calls used for the ENTSOG Glue ETL run:

| API call | Purpose |
| --- | --- |
| `aws glue get-job` | Read the current Glue job configuration and confirm its script location, role, Glue version, worker type, and worker count. |
| `aws iam get-role-policy` | Check whether the Glue role can read/write the selected S3 bucket. |
| `aws iam put-role-policy` | Update the Glue role inline S3 policy when the target bucket changes. |
| `aws s3 cp` | Upload the current local `glue/etl_raw_to_parquet.py` script to the bucket location used by Glue. |
| `aws iam get-role` | Resolve the Glue role ARN needed by `update-job`. |
| `aws glue update-job` | Point the Glue job at the current ETL script and confirm runtime settings. |
| `aws glue start-job-run` | Start the ETL run with `RAW_PATH` and `CURATED_PATH` arguments. |
| `aws glue get-job-run` | Poll the ETL run until it reaches `SUCCEEDED`, `FAILED`, `STOPPED`, or `TIMEOUT`. |
| `aws s3api list-objects-v2` | Verify curated Parquet files were written under `curated/dataset=gas/`. |

Run the Glue ETL for the ENTSOG gas proof bucket:

```bash
export AWS_REGION=eu-west-2
export S3_BUCKET=energy-market-lake-464975959576-20260405
export GLUE_ROLE_NAME=energy-market-glue-role
export GLUE_JOB_NAME=energy-market-etl-raw-to-parquet
```

For a fresh recreation, set `S3_BUCKET` to the bucket that contains your `raw/` ENTSOG proof. The value above is the bucket used for this run.

Preflight the existing Glue job and role:

```bash
aws glue get-job \
  --job-name "${GLUE_JOB_NAME}" \
  --region "${AWS_REGION}" \
  --query '{Name:Job.Name,Role:Job.Role,Command:Job.Command,GlueVersion:Job.GlueVersion,WorkerType:Job.WorkerType,NumberOfWorkers:Job.NumberOfWorkers}' \
  --output json

aws iam get-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-name energy-market-glue-s3-policy \
  --query 'PolicyDocument.Statement' \
  --output json
```

If `S3_BUCKET` changes, update the Glue role S3 policy to the same bucket:

```bash
cat > /tmp/energy-market-glue-s3-policy.json <<JSON
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::${S3_BUCKET}"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::${S3_BUCKET}/*"]
    }
  ]
}
JSON

aws iam put-role-policy \
  --role-name "${GLUE_ROLE_NAME}" \
  --policy-name energy-market-glue-s3-policy \
  --policy-document file:///tmp/energy-market-glue-s3-policy.json
```

Upload the current ETL script and point the Glue job at it:

```bash
aws s3 cp glue/etl_raw_to_parquet.py \
  "s3://${S3_BUCKET}/scripts/etl_raw_to_parquet.py" \
  --region "${AWS_REGION}"

GLUE_ROLE_ARN="$(aws iam get-role \
  --role-name "${GLUE_ROLE_NAME}" \
  --query 'Role.Arn' \
  --output text)"

aws glue update-job \
  --job-name "${GLUE_JOB_NAME}" \
  --job-update "Role=${GLUE_ROLE_ARN},Command={Name=glueetl,ScriptLocation=s3://${S3_BUCKET}/scripts/etl_raw_to_parquet.py,PythonVersion=3},GlueVersion=4.0,NumberOfWorkers=2,WorkerType=G.1X,ExecutionProperty={MaxConcurrentRuns=1}" \
  --region "${AWS_REGION}"
```

Start the ETL job:

```bash
JOB_RUN_ID="$(aws glue start-job-run \
  --job-name "${GLUE_JOB_NAME}" \
  --arguments "{\"--RAW_PATH\":\"s3://${S3_BUCKET}/raw\",\"--CURATED_PATH\":\"s3://${S3_BUCKET}/curated\"}" \
  --region "${AWS_REGION}" \
  --query 'JobRunId' \
  --output text)"

echo "${JOB_RUN_ID}"
```

Wait for completion:

```bash
while true; do
  JOB_STATE="$(aws glue get-job-run \
    --job-name "${GLUE_JOB_NAME}" \
    --run-id "${JOB_RUN_ID}" \
    --region "${AWS_REGION}" \
    --query 'JobRun.JobRunState' \
    --output text)"
  echo "${JOB_STATE}"
  [[ "${JOB_STATE}" == "SUCCEEDED" ]] && break
  [[ "${JOB_STATE}" == "FAILED" || "${JOB_STATE}" == "STOPPED" || "${JOB_STATE}" == "TIMEOUT" ]] && exit 1
  sleep 20
done
```

Verify curated gas output:

```bash
aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "curated/dataset=gas/" \
  --query 'Contents[].{Key:Key,Size:Size}' \
  --output table
```

Inspect the curated gas Parquet locally. This uses DuckDB as a lightweight Parquet reader:

```bash
python3 -m pip install duckdb

PARQUET_KEY="$(aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "curated/dataset=gas/region=eu/date=2026-05-03/" \
  --query 'Contents[?ends_with(Key, `.parquet`)] | [0].Key' \
  --output text)"

aws s3 cp "s3://${S3_BUCKET}/${PARQUET_KEY}" \
  /tmp/entsog-gas-2026-05-03.parquet

mkdir -p /tmp/energy-market-curated/dataset=gas/region=eu/date=2026-05-03
cp /tmp/entsog-gas-2026-05-03.parquet \
  /tmp/energy-market-curated/dataset=gas/region=eu/date=2026-05-03/part-00000.snappy.parquet

python3 - <<'PY'
import duckdb

path = "/tmp/energy-market-curated/dataset=gas/region=eu/date=2026-05-03/*.parquet"
con = duckdb.connect()

print(con.execute(f"""
SELECT
  region,
  date,
  COUNT(*) AS total_rows,
  SUM(CASE WHEN flow_kwh_d IS NOT NULL THEN 1 ELSE 0 END) AS flow_rows,
  SUM(CASE WHEN demand_kwh_d IS NOT NULL THEN 1 ELSE 0 END) AS demand_rows,
  COUNT(DISTINCT point_direction) AS point_directions
FROM read_parquet('{path}', hive_partitioning=true)
GROUP BY region, date
""").fetchall())
PY
```

## 6) Athena

- Set query result location to `s3://.../athena-results/`
- Create tables from Glue Data Catalog
- Run demo queries from `athena/queries.sql`

AWS API calls used for the ENTSOG Glue Catalog and Athena exposure run:

| API call | Purpose |
| --- | --- |
| `aws glue get-crawler` | Check the curated crawler target, state, database, role, and table prefix before changing it. |
| `aws glue update-crawler` | Point the curated crawler at the selected bucket's `curated/` prefix. |
| `aws glue start-crawler` | Run catalog discovery over curated Parquet outputs. |
| `aws glue get-tables` | Confirm the crawler created or updated `curated_dataset_gas`. |
| `aws glue get-table` | Inspect the gas table schema, S3 location, and partition keys. |
| `aws athena start-query-execution` | Run gas validation and demo SQL against the Glue Data Catalog table. |
| `aws athena get-query-execution` | Poll Athena query status until `SUCCEEDED`, `FAILED`, or `CANCELLED`. |
| `aws athena get-query-results` | Fetch Athena result rows for evidence. |

Run or update the curated crawler for gas:

```bash
export AWS_REGION=eu-west-2
export S3_BUCKET=energy-market-lake-464975959576-20260405
export GLUE_DATABASE_NAME=energy_market_lake
export CURATED_CRAWLER_NAME=energy-market-curated-crawler
export GLUE_ROLE_NAME=energy-market-glue-role
```

For a fresh recreation, keep `S3_BUCKET` aligned with the bucket used in Lambda and Glue ETL. Preflight the database and crawler:

```bash
aws glue get-database \
  --name "${GLUE_DATABASE_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Database.{Name:Name,Description:Description}' \
  --output json

aws glue get-crawler \
  --name "${CURATED_CRAWLER_NAME}" \
  --region "${AWS_REGION}" \
  --query '{Name:Crawler.Name,Role:Crawler.Role,DatabaseName:Crawler.DatabaseName,Targets:Crawler.Targets,State:Crawler.State,TablePrefix:Crawler.TablePrefix}' \
  --output json
```

Update the crawler target and run it:

```bash
aws glue update-crawler \
  --name "${CURATED_CRAWLER_NAME}" \
  --role "${GLUE_ROLE_NAME}" \
  --database-name "${GLUE_DATABASE_NAME}" \
  --targets "S3Targets=[{Path=s3://${S3_BUCKET}/curated/}]" \
  --table-prefix curated_ \
  --region "${AWS_REGION}"

aws glue start-crawler \
  --name "${CURATED_CRAWLER_NAME}" \
  --region "${AWS_REGION}"
```

Wait for the crawler:

```bash
while true; do
  CRAWLER_STATE="$(aws glue get-crawler \
    --name "${CURATED_CRAWLER_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Crawler.State' \
    --output text)"
  echo "${CRAWLER_STATE}"
  [[ "${CRAWLER_STATE}" == "READY" ]] && break
  sleep 15
done

aws glue get-crawler \
  --name "${CURATED_CRAWLER_NAME}" \
  --region "${AWS_REGION}" \
  --query '{Name:Crawler.Name,State:Crawler.State,LastCrawl:Crawler.LastCrawl}' \
  --output json
```

Confirm the gas catalog table:

```bash
aws glue get-table \
  --database-name "${GLUE_DATABASE_NAME}" \
  --name curated_dataset_gas \
  --region "${AWS_REGION}" \
  --query 'Table.{Name:Name,Location:StorageDescriptor.Location,Columns:StorageDescriptor.Columns,PartitionKeys:PartitionKeys}' \
  --output json
```

Validate the gas Athena schema and completeness:

```bash
python3 scripts/validate_athena_schema.py \
  --region "${AWS_REGION}" \
  --database "${GLUE_DATABASE_NAME}" \
  --table curated_dataset_gas \
  --output-location "s3://${S3_BUCKET}/athena-results/" \
  --expected-sources entsog \
  --output-file "docs/evidence/athena-gas-schema-$(date +%Y%m%d).md"
```

Run a gas Athena query for the deterministic proof date:

```bash
QUERY="SELECT date, point_direction, point_label, direction_key, SUM(flow_kwh_d) AS total_flow_kwh_d, SUM(demand_kwh_d) AS total_demand_kwh_d FROM curated_dataset_gas WHERE source = 'entsog' AND region = 'eu' AND date = '2026-05-03' GROUP BY date, point_direction, point_label, direction_key ORDER BY point_direction"

QUERY_EXECUTION_ID="$(aws athena start-query-execution \
  --query-string "${QUERY}" \
  --query-execution-context Database="${GLUE_DATABASE_NAME}" \
  --result-configuration OutputLocation="s3://${S3_BUCKET}/athena-results/" \
  --region "${AWS_REGION}" \
  --query 'QueryExecutionId' \
  --output text)"

while true; do
  QUERY_STATE="$(aws athena get-query-execution \
    --query-execution-id "${QUERY_EXECUTION_ID}" \
    --region "${AWS_REGION}" \
    --query 'QueryExecution.Status.State' \
    --output text)"
  echo "${QUERY_STATE}"
  [[ "${QUERY_STATE}" == "SUCCEEDED" ]] && break
  [[ "${QUERY_STATE}" == "FAILED" || "${QUERY_STATE}" == "CANCELLED" ]] && exit 1
  sleep 2
done

aws athena get-query-results \
  --query-execution-id "${QUERY_EXECUTION_ID}" \
  --region "${AWS_REGION}" \
  --output table
```

Phase 5 gas evidence files from the completed implementation:

```text
docs/evidence/run-entsog-gas-20260506.md
docs/evidence/athena-gas-schema-20260506.md
docs/evidence/athena-gas-query-20260506.txt
docs/evidence/athena-gas-query-summary-20260506.md
```

Expected Phase 5 proof points for deterministic date `2026-05-03`:

```text
gas_flow raw payloads: 4
gas_demand raw payloads: 4
curated gas parquet objects: 1
Athena schema validation: PASS
Athena rows returned for selected pointDirections: 4
missing selected pointDirections: none
```

## 7) ENTSOG 7-Day Gas Trend Refresh

Use this when the Gas tab needs a rolling seven-day view instead of a single-day proof.

This example keeps the four-point seed and loads six additional historical dates alongside the existing latest curated gas date.

Set the runtime variables:

```bash
export AWS_REGION=eu-west-2
export S3_BUCKET=energy-market-lake-464975959576-20260405
export LAMBDA_FUNCTION_NAME=energy-market-elexon-ingest
export GLUE_JOB_NAME=energy-market-etl-raw-to-parquet
export GLUE_CRAWLER_NAME=energy-market-curated-crawler
export GLUE_DATABASE_NAME=energy_market_curated
```

Invoke Lambda for the six additional gas dates:

```bash
mkdir -p docs/evidence

for GAS_DATE in 2026-04-29 2026-04-30 2026-05-01 2026-05-02 2026-05-03 2026-05-04; do
  aws lambda invoke \
    --function-name "${LAMBDA_FUNCTION_NAME}" \
    --region "${AWS_REGION}" \
    --payload "{\"date\":\"${GAS_DATE}\"}" \
    --cli-binary-format raw-in-base64-out \
    "docs/evidence/entsog-lambda-invoke-${GAS_DATE}.json"

  cat "docs/evidence/entsog-lambda-invoke-${GAS_DATE}.json"
done
```

Expected invoke result:

```text
StatusCode: 200
status: ok
warnings: []
```

Verify raw seven-day gas payload counts:

```bash
aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "raw/source=entsog/dataset=gas_flow/" \
  --query "length(Contents[?contains(Key, 'date=2026-04-29') || contains(Key, 'date=2026-04-30') || contains(Key, 'date=2026-05-01') || contains(Key, 'date=2026-05-02') || contains(Key, 'date=2026-05-03') || contains(Key, 'date=2026-05-04') || contains(Key, 'date=2026-05-05')])" \
  --output text

aws s3api list-objects-v2 \
  --bucket "${S3_BUCKET}" \
  --prefix "raw/source=entsog/dataset=gas_demand/" \
  --query "length(Contents[?contains(Key, 'date=2026-04-29') || contains(Key, 'date=2026-04-30') || contains(Key, 'date=2026-05-01') || contains(Key, 'date=2026-05-02') || contains(Key, 'date=2026-05-03') || contains(Key, 'date=2026-05-04') || contains(Key, 'date=2026-05-05')])" \
  --output text
```

Expected counts:

```text
gas_flow: 28
gas_demand: 28
```

Run the Glue ETL:

```bash
GLUE_JOB_RUN_ID="$(aws glue start-job-run \
  --job-name "${GLUE_JOB_NAME}" \
  --region "${AWS_REGION}" \
  --arguments "{\"--RAW_PATH\":\"s3://${S3_BUCKET}/raw\",\"--CURATED_PATH\":\"s3://${S3_BUCKET}/curated\"}" \
  --query 'JobRunId' \
  --output text)"

echo "${GLUE_JOB_RUN_ID}"

while true; do
  GLUE_STATE="$(aws glue get-job-run \
    --job-name "${GLUE_JOB_NAME}" \
    --run-id "${GLUE_JOB_RUN_ID}" \
    --region "${AWS_REGION}" \
    --query 'JobRun.JobRunState' \
    --output text)"
  echo "${GLUE_STATE}"
  [[ "${GLUE_STATE}" == "SUCCEEDED" ]] && break
  [[ "${GLUE_STATE}" == "FAILED" || "${GLUE_STATE}" == "STOPPED" || "${GLUE_STATE}" == "TIMEOUT" ]] && exit 1
  sleep 15
done
```

Run the curated crawler:

```bash
aws glue start-crawler \
  --name "${GLUE_CRAWLER_NAME}" \
  --region "${AWS_REGION}"

while true; do
  CRAWLER_STATE="$(aws glue get-crawler \
    --name "${GLUE_CRAWLER_NAME}" \
    --region "${AWS_REGION}" \
    --query 'Crawler.State' \
    --output text)"
  echo "${CRAWLER_STATE}"
  [[ "${CRAWLER_STATE}" == "READY" ]] && break
  sleep 10
done

aws glue get-crawler \
  --name "${GLUE_CRAWLER_NAME}" \
  --region "${AWS_REGION}" \
  --query 'Crawler.LastCrawl.Status' \
  --output text
```

Run an Athena seven-day coverage query:

```bash
QUERY="SELECT date, COUNT(DISTINCT point_direction) AS points, COUNT_IF(flow_kwh_d IS NOT NULL) AS flow_rows, COUNT_IF(demand_kwh_d IS NOT NULL) AS demand_rows FROM curated_dataset_gas WHERE source = 'entsog' AND region = 'eu' AND date BETWEEN '2026-04-29' AND '2026-05-05' GROUP BY date ORDER BY date DESC"

QUERY_EXECUTION_ID="$(aws athena start-query-execution \
  --query-string "${QUERY}" \
  --query-execution-context Database="${GLUE_DATABASE_NAME}" \
  --result-configuration OutputLocation="s3://${S3_BUCKET}/athena-results/" \
  --region "${AWS_REGION}" \
  --query 'QueryExecutionId' \
  --output text)"

while true; do
  QUERY_STATE="$(aws athena get-query-execution \
    --query-execution-id "${QUERY_EXECUTION_ID}" \
    --region "${AWS_REGION}" \
    --query 'QueryExecution.Status.State' \
    --output text)"
  echo "${QUERY_STATE}"
  [[ "${QUERY_STATE}" == "SUCCEEDED" ]] && break
  [[ "${QUERY_STATE}" == "FAILED" || "${QUERY_STATE}" == "CANCELLED" ]] && exit 1
  sleep 2
done

aws athena get-query-results \
  --query-execution-id "${QUERY_EXECUTION_ID}" \
  --region "${AWS_REGION}" \
  --output json > docs/evidence/gas-7day-athena-coverage-$(date +%Y%m%d).json
```

Regenerate dashboard data and validate the trend shape:

```bash
.venv/bin/python scripts/generate_dashboard.py \
  --bucket "${S3_BUCKET}" \
  --output-json dashboard-ui/public/dashboard-data.json

jq -r '.overview.gasContext as $g | "latestDate=\($g.latestDate)\npointDirections=\($g.pointDirections|length)\ntrendPoints=\($g.trendPoints|length)\ntrendRange=\($g.trendPoints[0].date) to \($g.trendPoints[-1].date)\nlatestFlow=\($g.trendPoints[-1].flow)\nlatestAllocation=\($g.trendPoints[-1].allocation)\nlatestDelta=\($g.trendPoints[-1].delta)\nlatestComplete=\($g.trendPoints[-1].complete)"' \
  dashboard-ui/public/dashboard-data.json
```

Expected readback:

```text
latestDate=2026-05-05
pointDirections=4
trendPoints=7
trendRange=2026-04-29 to 2026-05-05
latestFlow=784.0 GWh/d
latestAllocation=790.1 GWh/d
latestDelta=+6.1 GWh/d
latestComplete=4/4
```

Validate the dashboard and contracts:

```bash
python3 -m py_compile scripts/generate_dashboard.py
npm run build
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
git diff --check
```

Evidence from this run:

```text
docs/evidence/gas-7day-trend-20260507.md
docs/evidence/gas-7day-athena-coverage-20260507.json
docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png
```

## 8) Cost Guardrails

- Keep backfill to 30-90 days
- Use Parquet + partitions
- Run Glue jobs only when needed

## 9) Terraform Infrastructure As Code

Terraform configuration for recreating or configuring the AWS lakehouse resources lives at:

```text
infra/terraform/lakehouse/
```

This is the Infrastructure as Code rebuild path for:

- Optional data lake S3 bucket
- Lambda ingestion function, IAM role, and CloudWatch log group
- EventBridge ingestion schedule
- Glue IAM role, database, raw crawler, curated crawler, and ETL job
- Glue ETL script upload to S3
- Athena workgroup and query result location

Terraform uses a remote S3 backend for durable state. Keep the Terraform state bucket separate from the data lake bucket.

Datastores represented in this design:

- S3 data lake bucket: raw, curated, failed, archive, and Athena result objects.
- Glue Data Catalog: metadata tables and partitions over S3 data.
- CloudWatch Logs: Lambda and Glue operational logs.
- S3 Terraform state bucket: durable Terraform state storage.
- Optional SSM Parameter Store or Secrets Manager: future location for secrets such as `ENTSOE_TOKEN`.

Backend example:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/infra/terraform/lakehouse
export AWS_REGION=eu-west-2
export TF_STATE_BUCKET=energy-market-terraform-state-464975959576-eu-west-2
```

Bootstrap the state bucket before `terraform init`:

```bash
aws s3api create-bucket \
  --bucket "${TF_STATE_BUCKET}" \
  --region "${AWS_REGION}" \
  --create-bucket-configuration LocationConstraint="${AWS_REGION}"

aws s3api put-public-access-block \
  --bucket "${TF_STATE_BUCKET}" \
  --public-access-block-configuration \
  "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

aws s3api put-bucket-versioning \
  --bucket "${TF_STATE_BUCKET}" \
  --versioning-configuration Status=Enabled

aws s3api put-bucket-encryption \
  --bucket "${TF_STATE_BUCKET}" \
  --server-side-encryption-configuration \
  '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
```

Then create the backend config:

```bash
cp backend.hcl.example backend.hcl
```

Example `backend.hcl`:

```hcl
bucket       = "energy-market-terraform-state-464975959576-eu-west-2"
key          = "energy-market-data-lake/dev/terraform.tfstate"
region       = "eu-west-2"
encrypt      = true
use_lockfile = true
```

The backend bucket should have:

- Block Public Access
- Versioning
- Server-side encryption
- Least-privilege IAM access

Start from the variable example:

```bash
cp terraform.tfvars.example terraform.tfvars
```

Use an existing data lake bucket:

```hcl
create_data_bucket = false
data_bucket_name   = "energy-market-lake-464975959576-20260405"
```

Create a new data lake bucket:

```hcl
create_data_bucket = true
data_bucket_name   = "energy-market-lake-your-real-suffix"
```

Phase 8 deterministic AI insight orchestration is optional. It adds:

- `lambda/news_ai_orchestration.py` as the Step Functions task Lambda.
- A Step Functions state machine with validation gates and failure catches.
- An SNS topic for failed executions.
- An optional separate dashboard/static S3 bucket.
- An initially disabled EventBridge schedule.

Build the Phase 8 Lambda package before enabling those Terraform resources:

```bash
../../../scripts/build_phase8_lambda_package.sh
```

The package is written to:

```text
infra/terraform/lakehouse/.terraform/build/news_ai_orchestration.zip
```

Example Phase 8 variables:

```hcl
create_dashboard_bucket = false
dashboard_bucket_name   = "energy-market-dashboard-public-464975959576-20260405"

ai_orchestration_enabled             = true
ai_orchestration_schedule_enabled    = false
ai_orchestration_dashboard_data_key  = "dashboard/dashboard-data.json"
ai_orchestration_sns_email           = ""
```

Keep `ai_orchestration_schedule_enabled = false` until one manual Step
Functions execution has passed. The public `dashboard_snapshot_v1.json` is
published last, after validation, so failed runs should leave the previous
public snapshot unchanged.

Phase 8 live AWS proof from 2026-05-11 used a targeted Terraform apply because
the older ingestion, Glue, and Athena resources were not fully imported into
this state yet. The proof deployed the orchestration Lambda, Step Functions
state machine, SNS failure topic, disabled EventBridge schedule, and separate
dashboard/static bucket.

Evidence:

```text
docs/evidence/phase8-aws-live-execution-20260511.md
```

Initialize with remote state:

```bash
terraform init -backend-config=backend.hcl
```

For local syntax validation without touching remote state:

```bash
terraform init -backend=false
terraform validate
```

Plan and apply:

```bash
terraform plan -out tfplan
terraform apply tfplan
```

If the manual AWS resources already exist in the target account, either import them into Terraform state first or use a different `project_prefix` for a fresh parallel environment. See `infra/terraform/lakehouse/README.md` for import examples.

Full import checklist:

```text
docs/terraform-import-checklist.md
```

The import checklist includes:

- Read-only AWS preflight calls to confirm the current Lambda, IAM, EventBridge, Glue, S3 object, and Athena state.
- Exact `terraform import` commands for the existing Lambda, roles, policies, log group, EventBridge rule/target/permission, Glue resources, and ETL script object.
- Optional imports for the data lake bucket if you later switch from `create_data_bucket = false` to Terraform-managed bucket ownership.
- Expected first-plan drift, including schedule disablement, Lambda log retention, raw crawler retargeting, tags, and Athena workgroup creation.

Current import command set for the existing manual resources:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/infra/terraform/lakehouse
export AWS_REGION=eu-west-2
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export S3_BUCKET=energy-market-lake-464975959576-20260405

terraform init -backend-config=backend.hcl

terraform import aws_iam_role.lambda energy-market-lambda-role
terraform import aws_iam_role_policy_attachment.lambda_basic_execution 'energy-market-lambda-role/arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'
terraform import aws_iam_role_policy.lambda_s3 'energy-market-lambda-role:energy-market-lambda-s3-policy'
terraform import aws_cloudwatch_log_group.lambda /aws/lambda/energy-market-elexon-ingest
terraform import aws_lambda_function.ingest energy-market-elexon-ingest
terraform import aws_cloudwatch_event_rule.daily_ingestion energy-market-daily-ingestion
terraform import aws_cloudwatch_event_target.daily_ingestion energy-market-daily-ingestion/1
terraform import aws_lambda_permission.allow_eventbridge energy-market-elexon-ingest/energy-market-daily-ingestion-invoke
terraform import aws_iam_role.glue energy-market-glue-role
terraform import aws_iam_role_policy_attachment.glue_service_role 'energy-market-glue-role/arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
terraform import aws_iam_role_policy.glue_s3 'energy-market-glue-role:energy-market-glue-s3-policy'
terraform import aws_s3_object.glue_script "${S3_BUCKET}/scripts/etl_raw_to_parquet.py"
terraform import aws_glue_catalog_database.lakehouse "${ACCOUNT_ID}:energy_market_lake"
terraform import aws_glue_crawler.raw energy-market-raw-crawler
terraform import aws_glue_crawler.curated energy-market-curated-crawler
terraform import aws_glue_job.raw_to_parquet energy-market-etl-raw-to-parquet

terraform state list | sort
terraform plan -out tfplan
```

Terraform rebuild/import gotchas:

- Terraform cannot create its own S3 backend bucket from the same root; bootstrap the state bucket first.
- Keep the Terraform state bucket separate from the raw/curated data lake bucket.
- Import links existing AWS resources to Terraform state, but it does not generate code or guarantee drift-free plans.
- When `create_data_bucket = false`, the existing data lake bucket is referenced but not managed by Terraform.
- If `S3_BUCKET` changes, align Lambda and Glue IAM policies to the same bucket before invoking jobs.
- Keep `schedule_enabled = false` until manual Lambda, Glue, crawler, and Athena validation has passed.
- Avoid putting real secrets in Terraform variables until a Secrets Manager or SSM pattern is added.
- After import or crawler runs, confirm `curated_dataset_gas` points at the intended `curated/dataset=gas/` S3 location.
