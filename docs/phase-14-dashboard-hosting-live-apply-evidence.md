# Phase 14: Dashboard Hosting Live Apply Evidence

Use this checklist to decide whether the dashboard hosting boundary is safe to
apply live, then capture evidence if the live apply is approved.

## Goal

Move from Phase 13 plan-only publish proof to a controlled live hosting
evidence path:

```text
review Terraform plan -> apply dashboard hosting boundary ->
publish dashboard assets -> verify CloudFront/S3/HTTP -> capture evidence
```

This phase is intentionally conservative. The first state is preflight only:
review variables, state, proof commands, risks, and rollback path. Do not run
`terraform apply` until the plan has been reviewed.

## Current State

- `main` is clean and synced.
- Phase 12 added optional CloudFront and private S3 Terraform resources.
- Phase 13 added a plan-only dashboard publish script.
- `infra/terraform/lakehouse/terraform.tfvars` is local-only and ignored by
  Git, as expected.
- Current local tfvars has:
  - `create_dashboard_bucket = true`
  - `dashboard_bucket_name = "energy-market-dashboard-public-464975959576-20260511"`
  - no explicit `dashboard_cloudfront_enabled = true`
- Terraform state currently includes the dashboard S3 bucket resources.
- Terraform state does not currently include CloudFront resources.
- `terraform validate` passes.

## Target State

- Terraform plan evidence shows the exact CloudFront/S3 bucket-policy changes.
- Any unrelated drift is reviewed before apply.
- If safe, live apply creates the CloudFront delivery boundary.
- Dashboard assets are published with `scripts/publish_dashboard_static_site.sh
  --apply`.
- Evidence captures Terraform outputs, S3 objects, CloudFront distribution
  state, HTTP headers, and rollback notes.

## Scope Boundary

In scope:

- Dashboard CloudFront distribution.
- CloudFront Origin Access Control.
- S3 bucket policy for CloudFront-only reads.
- Dashboard static asset publish.
- CloudFront invalidation.
- Evidence capture.

Out of scope:

- DNS, ACM certificates, or custom domains.
- CloudWatch alarms and budgets.
- EventBridge schedule enablement.
- Bedrock or OpenClaw managed model invocation.
- Changing dashboard UI behavior.
- Broad Terraform reconciliation unrelated to dashboard hosting.

## Preflight Checklist

- [x] Confirm repo is clean on `main` before starting Phase 14.
- [x] Confirm `terraform validate` passes.
- [x] Confirm dashboard bucket is already in Terraform state.
- [x] Confirm CloudFront resources are not yet in Terraform state.
- [x] Confirm local tfvars is ignored by Git.
- [x] Add or temporarily pass `dashboard_cloudfront_enabled = true`.
- [x] Run Terraform plan only.
- [x] Save plan output as evidence.
- [x] Review plan for unrelated creates, updates, replacements, or destroys.
- [x] Decide whether to apply.

## Phase 14A Plan Review

Plan evidence:

```text
docs/evidence/phase14-dashboard-hosting-plan-20260519T202521Z.txt
```

Command used:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -var='dashboard_cloudfront_enabled=true' \
  -out=tfplan-phase14-dashboard-hosting
```

Plan summary:

```text
Plan: 4 to add, 1 to change, 0 to destroy.
```

Expected dashboard-hosting additions:

- `aws_cloudfront_distribution.dashboard_static[0]`
- `aws_cloudfront_origin_access_control.dashboard_static[0]`
- `aws_cloudfront_response_headers_policy.dashboard_static[0]`
- `aws_s3_bucket_policy.dashboard_static_cloudfront[0]`

Blocking unrelated drift:

- `aws_lambda_function.ingest` would be updated in-place.

Phase 14A decision: **do not apply this plan**.

Reason: the plan is not limited to the dashboard hosting boundary. The
CloudFront additions are expected, but the ingestion Lambda update is outside
the approved Phase 14 scope. The next safe state is to isolate or neutralize the
Lambda drift before producing a new apply candidate plan.

## Phase 14B Lambda Drift Isolation

Plan evidence:

```text
docs/evidence/phase14b-dashboard-hosting-refreshfalse-plan-20260520.txt
```

Commands used:

```bash
terraform -chdir=infra/terraform/lakehouse state show \
  aws_lambda_function.ingest

aws lambda get-function-configuration \
  --function-name energy-market-elexon-ingest \
  --region eu-west-2 \
  --query '{FunctionName:FunctionName,LastModified:LastModified,CodeSha256:CodeSha256,Runtime:Runtime,Handler:Handler,MemorySize:MemorySize,Timeout:Timeout,Role:Role,EnvironmentKeys:keys(Environment.Variables)}'

LAMBDA_ARN="arn:aws:lambda:eu-west-2:464975959576:function:energy-market-elexon-ingest"

aws lambda list-tags \
  --resource "${LAMBDA_ARN}" \
  --region eu-west-2

terraform -chdir=infra/terraform/lakehouse plan \
  -refresh=false \
  -var='dashboard_cloudfront_enabled=true' \
  -out=tfplan-phase14b-refreshfalse
```

Findings:

- Terraform state and live AWS agree on the deployed Lambda code hash:
  `LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=`.
- The locally generated Terraform archive hash is different:
  `O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=`.
- Terraform state has `source_code_hash = null`, `tags = {}`, and
  `tags_all = {}` for `aws_lambda_function.ingest`.
- Live AWS also has no Lambda tags.
- A `-refresh=false` plan still proposes the same in-place Lambda update.

Conclusion: the Phase 14A Lambda update is not caused by a live refresh during
planning. It is a Terraform configuration/state reconciliation issue: the root
configuration now declares a Lambda package hash and common tags that are not
represented in the current Terraform state/live resource, and the local Lambda
package differs from the deployed Lambda code.

Phase 14B decision: **do not apply yet**.

Safest isolation path:

1. Keep dashboard hosting live apply blocked until the root plan contains only
   dashboard-hosting changes.
2. Do not add broad `ignore_changes` for Lambda code or environment variables;
   that would hide real ingestion drift.
3. Reconcile the Lambda in a separate, explicit slice before the dashboard live
   apply. That slice should decide whether to intentionally redeploy the
   current repo Lambda package, or to preserve the deployed Lambda package and
   align Terraform state/configuration around that decision.
4. Use a targeted dashboard apply only as a break-glass option after an explicit
   approval, because Terraform `-target` is not a normal release boundary and
   can hide dependency changes outside the target set.

Next safe state boundary:

- Produce a Lambda-only reconciliation plan.
- Confirm whether the repo Lambda package is the intended live version.
- Either accept a controlled Lambda redeploy first, or adjust the Terraform
  ownership model intentionally.
- Re-run the Phase 14 dashboard plan and proceed only if it is limited to:
  CloudFront distribution, OAC, response headers policy, and dashboard S3 bucket
  policy.

Lambda rollback preparation for the next slice:

```bash
LAMBDA_CONFIG_QUERY="{FunctionName:FunctionName,LastModified:LastModified,"
LAMBDA_CONFIG_QUERY+="CodeSha256:CodeSha256,Runtime:Runtime,Handler:Handler,"
LAMBDA_CONFIG_QUERY+="MemorySize:MemorySize,Timeout:Timeout,Role:Role,"
LAMBDA_CONFIG_QUERY+="EnvironmentKeys:keys(Environment.Variables)}"

aws lambda get-function-configuration \
  --function-name energy-market-elexon-ingest \
  --region eu-west-2 \
  --query "${LAMBDA_CONFIG_QUERY}" \
  > docs/evidence/phase14b-ingest-lambda-current-config-sanitized.json
```

Before any intentional Lambda redeploy, fetch the `Code.Location` pre-signed
URL into a shell variable and download the currently deployed package into a
local, ignored rollback artifact. Do not print the URL and do not commit the
downloaded package.

If the Lambda reconciliation causes an ingestion regression, restore the
previous package with `aws lambda update-function-code` or a controlled
Terraform rollback plan, then rerun the ingestion smoke checks before returning
to the dashboard hosting apply boundary.

## Phase 14C Lambda Reconciliation Decision

Evidence:

```text
docs/evidence/phase14c-root-lambda-reconcile-plan-20260520.txt
docs/evidence/phase14c-ingest-lambda-current-config-sanitized-20260520.json
docs/evidence/phase14c-ingest-lambda-current-tags-20260520.json
```

Commands used:

```bash
LAMBDA_CONFIG_QUERY="{FunctionName:FunctionName,LastModified:LastModified,"
LAMBDA_CONFIG_QUERY+="CodeSha256:CodeSha256,Runtime:Runtime,Handler:Handler,"
LAMBDA_CONFIG_QUERY+="MemorySize:MemorySize,Timeout:Timeout,Role:Role,"
LAMBDA_CONFIG_QUERY+="EnvironmentKeys:keys(Environment.Variables)}"

terraform -chdir=infra/terraform/lakehouse plan \
  -out=tfplan-phase14c-root-lambda-reconcile

terraform -chdir=infra/terraform/lakehouse show -no-color \
  tfplan-phase14c-root-lambda-reconcile \
  > docs/evidence/phase14c-root-lambda-reconcile-plan-20260520.txt

aws lambda get-function-configuration \
  --function-name energy-market-elexon-ingest \
  --region eu-west-2 \
  --query "${LAMBDA_CONFIG_QUERY}" \
  --output json \
  > docs/evidence/phase14c-ingest-lambda-current-config-sanitized-20260520.json

LAMBDA_ARN="arn:aws:lambda:eu-west-2:464975959576:function:energy-market-elexon-ingest"

aws lambda list-tags \
  --resource "${LAMBDA_ARN}" \
  --region eu-west-2 \
  --output json \
  > docs/evidence/phase14c-ingest-lambda-current-tags-20260520.json
```

Findings:

- With CloudFront disabled, a normal root Terraform plan shows only the
  ingestion Lambda reconciliation: `Plan: 0 to add, 1 to change, 0 to destroy`.
- The proposed change still adds `source_code_hash`, `filename`, `publish =
  false`, and the standard Terraform tags to `aws_lambda_function.ingest`.
- The currently deployed Lambda ZIP hash remains
  `LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=`.
- The Terraform-built local ZIP hash remains
  `O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=`.
- The extracted deployed `ingest_elexon.py` and local
  `lambda/ingest_elexon.py` have the same SHA-256 file hash:
  `525ef7109341258906f3ed6b6fbc0ce829666cb8cbfd06c53df78e46caed4997`.
- Live Lambda tags are currently `{}`.

Conclusion: the Lambda drift is operationally low risk but still a real live
mutation. The code file content matches, so the ZIP-level difference is likely
packaging metadata or archive construction rather than application source
drift. The Terraform plan would still update the Lambda package metadata,
`last_modified`, `source_code_hash`, and tags.

Phase 14C decision: **do not apply during this decision slice**.

Recommended next state:

1. Run a controlled Lambda-only reconciliation apply as its own explicit state,
   using the normal root plan with dashboard CloudFront still disabled.
2. Before apply, download the currently deployed Lambda ZIP into a local,
   ignored rollback file without printing the pre-signed URL.
3. Apply only the saved root plan that shows `Plan: 0 to add, 1 to change,
   0 to destroy`.
4. Verify Lambda code hash, tags, configuration keys, and an ingestion smoke
   check.
5. Re-run the Phase 14 dashboard hosting plan only after the Lambda
   reconciliation plan is clean.

Alternatives rejected:

- Broad `ignore_changes` on Lambda code or environment variables: rejected
  because it would hide future ingestion drift.
- Preserving the deployed ZIP as the Terraform-owned package: rejected for now
  because the extracted source file matches the repo source, while Terraform
  already defines the repo source as the ownership boundary.
- Targeted dashboard apply while Lambda drift remains: rejected except as an
  explicit break-glass path, because `-target` is not a normal release boundary.

## Phase 14D Lambda Reconciliation Apply

Evidence:

```text
docs/evidence/phase14d-lambda-reconcile-apply-20260521.txt
docs/evidence/phase14d-ingest-lambda-post-apply-config-sanitized-20260521.json
docs/evidence/phase14d-ingest-lambda-post-apply-tags-20260521.json
docs/evidence/phase14d-ingest-lambda-smoke-invoke-20260521.json
docs/evidence/phase14d-ingest-lambda-smoke-response-20260521.json
docs/evidence/phase14d-ingest-lambda-smoke-s3-head-20260521.json
docs/evidence/phase14d-post-apply-nochange-plan-20260521.txt
```

Rollback package captured locally before apply:

```text
infra/terraform/lakehouse/.terraform/rollback/ingest-elexon-before-phase14d-20260521T141212Z.zip
```

The rollback ZIP was intentionally not committed. Its SHA-256 base64 hash
matched the pre-apply deployed Lambda hash:

```text
LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=
```

Apply command:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  -no-color \
  tfplan-phase14d-lambda-reconcile
```

Apply result:

```text
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
```

Post-apply Lambda state:

- `CodeSha256`: `O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=`
- Tags now include `Environment`, `ManagedBy`, `Project`, and `Workload`.
- Sanitized configuration check retained the expected environment variable
  keys without exposing secret values.

Smoke test:

```bash
aws lambda invoke \
  --function-name energy-market-elexon-ingest \
  --region eu-west-2 \
  --cli-binary-format raw-in-base64-out \
  --payload '{"date":"2026-05-03"}' \
  docs/evidence/phase14d-ingest-lambda-smoke-response-20260521.json
```

Smoke result:

- Invoke status code: `200`
- Handler status: `ok`
- Warnings: `[]`
- S3 object evidence confirmed
  `raw/source=elexon/dataset=atl/date=2026-05-03/payload.json`

Post-apply Terraform plan:

```text
No changes. Your infrastructure matches the configuration.
```

Phase 14D decision: **Lambda drift reconciled**.

Next safe state boundary:

- Re-run the Phase 14 dashboard hosting plan with
  `dashboard_cloudfront_enabled=true`.
- Proceed to live dashboard hosting apply only if the root plan is limited to
  CloudFront distribution, OAC, response headers policy, and dashboard S3 bucket
  policy.
- Keep the local rollback ZIP until the dashboard hosting apply is complete and
  the project reaches the next clean pause boundary.

## Proof Commands

Run from the repo root unless noted.

### 1. Baseline Checks

```bash
git switch main
git pull --ff-only origin main
git status --short --branch

npm --prefix dashboard-ui run build
.venv/bin/python scripts/validate_contracts.py \
  --include-evidence \
  --check-failures

terraform -chdir=infra/terraform/lakehouse validate
terraform -chdir=infra/terraform/lakehouse state list | \
  rg 'dashboard|cloudfront|s3_bucket_policy|origin_access'
terraform -chdir=infra/terraform/lakehouse output
```

### 2. Plan The Hosting Boundary

Prefer a temporary CLI variable first so the local tfvars file does not need to
change just to inspect the plan:

```bash
terraform -chdir=infra/terraform/lakehouse plan \
  -var='dashboard_cloudfront_enabled=true' \
  -out=tfplan-phase14-dashboard-hosting

terraform -chdir=infra/terraform/lakehouse show -no-color \
  tfplan-phase14-dashboard-hosting | \
  tee docs/evidence/phase14-dashboard-hosting-plan-$(date -u +%Y%m%dT%H%M%SZ).txt
```

The plan is acceptable only if the proposed changes are limited to the
dashboard hosting boundary:

- `aws_cloudfront_origin_access_control.dashboard_static`
- `aws_cloudfront_response_headers_policy.dashboard_static`
- `aws_cloudfront_distribution.dashboard_static`
- `aws_s3_bucket_policy.dashboard_static_cloudfront`
- supporting data reads such as the managed CloudFront cache policy

Stop if the plan includes:

- Lambda replacement or code update
- Step Functions replacement
- EventBridge schedule enablement
- data lake bucket replacement
- Glue/Athena replacement
- IAM broadening unrelated to CloudFront/S3 dashboard read access
- any destroy action not explicitly expected

### 3. Live Apply Command

Run only after the saved plan is reviewed and accepted:

```bash
terraform -chdir=infra/terraform/lakehouse apply \
  tfplan-phase14-dashboard-hosting
```

After apply:

```bash
terraform -chdir=infra/terraform/lakehouse output \
  dashboard_cloudfront_distribution_id
terraform -chdir=infra/terraform/lakehouse output \
  dashboard_cloudfront_domain_name
```

### 4. Publish Dashboard Assets

Run only after the CloudFront outputs are non-null:

```bash
scripts/publish_dashboard_static_site.sh \
  --apply \
  --evidence-file docs/evidence/dashboard-hosting-live-apply-$(date -u +%Y%m%dT%H%M%SZ).md
```

### 5. Post-Apply Evidence

```bash
DASHBOARD_BUCKET="$(
  terraform -chdir=infra/terraform/lakehouse output -raw dashboard_bucket_name
)"
DISTRIBUTION_ID="$(
  terraform -chdir=infra/terraform/lakehouse output -raw dashboard_cloudfront_distribution_id
)"
DISTRIBUTION_DOMAIN="$(
  terraform -chdir=infra/terraform/lakehouse output -raw dashboard_cloudfront_domain_name
)"

aws s3 ls "s3://${DASHBOARD_BUCKET}/"
aws cloudfront get-distribution --id "${DISTRIBUTION_ID}"
curl -fsSI "https://${DISTRIBUTION_DOMAIN}/index.html"
curl -fsSI "https://${DISTRIBUTION_DOMAIN}/dashboard-data.json"
curl -fsSI "https://${DISTRIBUTION_DOMAIN}/dashboard_snapshot_v1.sample.json"
```

## Rollback Path

Fast operational rollback:

- stop sharing the CloudFront URL
- keep the previous dashboard evidence in Git and S3
- republish the previous known-good `dashboard-ui/dist` if only content is bad

Infrastructure rollback:

1. Set `dashboard_cloudfront_enabled = false` or remove the temporary
   `-var='dashboard_cloudfront_enabled=true'`.
2. Run a new Terraform plan.
3. Confirm the plan destroys only CloudFront, OAC, response header policy, and
   the dashboard bucket policy.
4. Apply only after confirming the private dashboard bucket itself is not being
   destroyed.

Content rollback:

```bash
scripts/publish_dashboard_static_site.sh \
  --apply \
  --skip-build \
  --evidence-file docs/evidence/dashboard-hosting-content-rollback-$(date -u +%Y%m%dT%H%M%SZ).md
```

## Safety Decision

Current decision: **plan reviewed, not safe to apply yet**.

Reasons:

- CloudFront resources are not in state yet, so the first live apply will create
  new public delivery infrastructure.
- The root also manages older lakehouse, Lambda, Step Functions, IAM, and
  schedule resources; any unrelated drift must be reviewed before apply.
- The dashboard bucket already exists in Terraform state, so this should be a
  narrow add-on if the plan is clean.
- The Phase 14A plan included an unrelated in-place update to
  `aws_lambda_function.ingest`.
- Phase 14B confirmed the Lambda update is caused by configuration/state
  reconciliation, not just live refresh noise.

Apply becomes acceptable only when the saved plan shows a narrow dashboard
hosting change set and no unrelated replacements, destroys, schedule
enablement, or IAM broadening.
