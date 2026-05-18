#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TERRAFORM_DIR="${ROOT_DIR}/infra/terraform/lakehouse"
DASHBOARD_DIR="${ROOT_DIR}/dashboard-ui"
DIST_DIR="${DASHBOARD_DIR}/dist"
EVIDENCE_DIR="${ROOT_DIR}/docs/evidence"

APPLY=false
SKIP_BUILD=false
BUCKET=""
DISTRIBUTION_ID=""
DISTRIBUTION_DOMAIN=""
EVIDENCE_FILE=""
AWS_REGION_VALUE="${AWS_REGION:-eu-west-2}"
AWS_PROFILE_VALUE="${AWS_PROFILE:-}"

usage() {
  cat <<'USAGE'
Usage: scripts/publish_dashboard_static_site.sh [options]

Build and publish, or plan publishing, for the React dashboard static site.
The default mode is plan-only: no AWS write commands are executed.

Options:
  --apply                    Execute S3 sync and CloudFront invalidation.
  --skip-build               Reuse dashboard-ui/dist instead of rebuilding.
  --bucket NAME              Dashboard S3 bucket. Defaults to Terraform output.
  --distribution-id ID       CloudFront distribution ID. Defaults to Terraform output.
  --distribution-domain NAME CloudFront domain name. Defaults to Terraform output.
  --evidence-file PATH       Markdown evidence file to write.
  --region REGION            AWS region for CLI commands. Defaults to AWS_REGION or eu-west-2.
  --profile PROFILE          AWS profile for CLI commands.
  -h, --help                 Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      APPLY=true
      shift
      ;;
    --skip-build)
      SKIP_BUILD=true
      shift
      ;;
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    --distribution-id)
      DISTRIBUTION_ID="$2"
      shift 2
      ;;
    --distribution-domain)
      DISTRIBUTION_DOMAIN="$2"
      shift 2
      ;;
    --evidence-file)
      EVIDENCE_FILE="$2"
      shift 2
      ;;
    --region)
      AWS_REGION_VALUE="$2"
      shift 2
      ;;
    --profile)
      AWS_PROFILE_VALUE="$2"
      shift 2
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

require_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required command not found: $1" >&2
    exit 1
  fi
}

terraform_output() {
  local name="$1"
  local value
  value="$(terraform -chdir="${TERRAFORM_DIR}" output -raw "${name}" 2>/dev/null || true)"
  if [[ "${value}" == "null" ]]; then
    value=""
  fi
  printf '%s' "${value}"
}

quote_args() {
  printf ' %q' "$@"
}

run_or_print() {
  local -a cmd=("$@")
  printf '+'
  quote_args "${cmd[@]}"
  printf '\n'
  if [[ "${APPLY}" == "true" ]]; then
    "${cmd[@]}"
  fi
}

require_command npm
require_command terraform
require_command git
if [[ "${APPLY}" == "true" ]]; then
  require_command aws
fi

if [[ -z "${BUCKET}" ]]; then
  BUCKET="$(terraform_output dashboard_bucket_name)"
fi

if [[ -z "${DISTRIBUTION_ID}" ]]; then
  DISTRIBUTION_ID="$(terraform_output dashboard_cloudfront_distribution_id)"
fi

if [[ -z "${DISTRIBUTION_DOMAIN}" ]]; then
  DISTRIBUTION_DOMAIN="$(terraform_output dashboard_cloudfront_domain_name)"
fi

if [[ -z "${BUCKET}" ]]; then
  echo "Dashboard bucket is empty. Pass --bucket or set dashboard_bucket_name." >&2
  exit 1
fi

if [[ "${SKIP_BUILD}" != "true" ]]; then
  npm --prefix "${DASHBOARD_DIR}" run build
  "${ROOT_DIR}/.venv/bin/python" "${ROOT_DIR}/scripts/validate_contracts.py" \
    --include-evidence \
    --check-failures
fi

for required_file in \
  "${DIST_DIR}/index.html" \
  "${DIST_DIR}/dashboard-data.json" \
  "${DIST_DIR}/dashboard_snapshot_v1.sample.json"; do
  if [[ ! -f "${required_file}" ]]; then
    echo "Missing dashboard artifact: ${required_file}" >&2
    exit 1
  fi
done

if [[ -z "${EVIDENCE_FILE}" ]]; then
  EVIDENCE_FILE="${EVIDENCE_DIR}/dashboard-hosting-publish-$(date -u +%Y%m%dT%H%M%SZ).md"
fi

mkdir -p "$(dirname "${EVIDENCE_FILE}")"

MODE="plan-only"
if [[ "${APPLY}" == "true" ]]; then
  MODE="apply"
fi

GIT_BRANCH="$(git -C "${ROOT_DIR}" branch --show-current)"
GIT_COMMIT="$(git -C "${ROOT_DIR}" rev-parse --short HEAD)"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ASSET_COUNT="$(find "${DIST_DIR}" -type f | wc -l | tr -d ' ')"

cat > "${EVIDENCE_FILE}" <<EVIDENCE
# Dashboard Hosting Publish Evidence

<!-- markdownlint-disable MD013 -->

Generated at: ${GENERATED_AT}
Mode: ${MODE}
Git branch: ${GIT_BRANCH}
Git commit: ${GIT_COMMIT}

## Inputs

- Dashboard dist: \`${DIST_DIR}\`
- Dashboard bucket: \`${BUCKET}\`
- CloudFront distribution ID: \`${DISTRIBUTION_ID:-not configured}\`
- CloudFront domain: \`${DISTRIBUTION_DOMAIN:-not configured}\`
- AWS region: \`${AWS_REGION_VALUE}\`
- Asset count: ${ASSET_COUNT}

## Commands

\`\`\`bash
npm --prefix dashboard-ui run build
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
aws s3 sync dashboard-ui/dist/ s3://${BUCKET}/ \\
  --delete \\
  --exclude "assets/*" \\
  --cache-control "no-cache"
aws s3 sync dashboard-ui/dist/assets/ s3://${BUCKET}/assets/ \\
  --delete \\
  --cache-control "public,max-age=31536000,immutable"
EVIDENCE

if [[ -n "${DISTRIBUTION_ID}" ]]; then
  cat >> "${EVIDENCE_FILE}" <<EVIDENCE
aws cloudfront create-invalidation \\
  --distribution-id ${DISTRIBUTION_ID} \\
  --paths "/index.html" "/assets/*" "/dashboard-data.json" "/dashboard_snapshot_v1.sample.json"
EVIDENCE
fi

cat >> "${EVIDENCE_FILE}" <<'EVIDENCE'
```

## Result

EVIDENCE

if [[ "${APPLY}" == "true" ]]; then
  echo "- AWS publish commands were executed." >> "${EVIDENCE_FILE}"
else
  echo "- Plan-only mode. No AWS write commands were executed." >> "${EVIDENCE_FILE}"
fi

AWS_ARGS=(--region "${AWS_REGION_VALUE}")
if [[ -n "${AWS_PROFILE_VALUE}" ]]; then
  AWS_ARGS+=(--profile "${AWS_PROFILE_VALUE}")
fi

S3_SYNC_COMMON=(--delete)

run_or_print aws "${AWS_ARGS[@]}" s3 sync "${DIST_DIR}/" "s3://${BUCKET}/" \
  "${S3_SYNC_COMMON[@]}" \
  --exclude "assets/*" \
  --cache-control "no-cache"

run_or_print aws "${AWS_ARGS[@]}" s3 sync "${DIST_DIR}/assets/" "s3://${BUCKET}/assets/" \
  "${S3_SYNC_COMMON[@]}" \
  --cache-control "public,max-age=31536000,immutable"

if [[ -n "${DISTRIBUTION_ID}" ]]; then
  run_or_print aws "${AWS_ARGS[@]}" cloudfront create-invalidation \
    --distribution-id "${DISTRIBUTION_ID}" \
    --paths "/index.html" "/assets/*" "/dashboard-data.json" "/dashboard_snapshot_v1.sample.json"
else
  echo "CloudFront distribution ID not configured; invalidation command skipped."
fi

echo "Wrote publish evidence: ${EVIDENCE_FILE}"
