# Phase 8: AWS AI Insight Orchestration

Use this plan to move the local news and AI insight MVP into an AWS-managed
workflow without introducing Bedrock or OpenClaw before the orchestration and
validation boundary is proven.

## Goal

Operationalize the existing local news and AI insight flow as a scheduled,
observable AWS workflow:

```text
Athena energy export
  -> RSS/news ingest
  -> contract validation
  -> AI input bundle
  -> deterministic AI insight merge
  -> ai_insight_v1 validation
  -> public dashboard snapshot publish
  -> audit, failed, CloudWatch, and SNS paths
```

The important portfolio hook is not "AI generated text"; it is controlled AI
orchestration with schema gates, quarantine, auditability, and a safe public
publish boundary.

## Branch

```text
feature/aws-ai-insight-orchestration
```

## Scope Boundary

In scope for Phase 8:

- AWS orchestration for the existing local news and deterministic AI merge path.
- S3-backed inputs, outputs, failed payloads, and audit evidence.
- Step Functions workflow with retries and catch paths.
- CloudWatch logs and SNS notifications for failure events.
- Dashboard snapshot publish to a public-safe prefix.
- Documentation and evidence for rebuild/demo.

Deferred until after Phase 8:

- Bedrock `InvokeModel`.
- OpenClaw on ECS/Fargate or another managed runtime.
- Multi-agent orchestration.
- Fine-tuning.
- Publishing raw model text directly to the dashboard.

## Design Lock Decisions

These decisions are accepted for Phase 8 and should guide implementation.

### Run ID Shape

Use a workflow prefix, UTC timestamp, and short UUID:

```text
ai-insight-YYYYMMDDTHHMMSSZ-<8-char-uuid>
```

Example:

```text
ai-insight-20260509T093015Z-a1b2c3d4
```

Rationale:

- Human-readable enough for evidence and S3 inspection.
- Time-sortable for audit review.
- Collision-safe for retries, manual reruns, and parallel tests.
- Clear that the run belongs to the AI insight workflow.

### Step Functions Payload Shape

Use a hybrid payload:

- pass metadata, state, counts, status, and S3 artifact references inline
- keep full contract payloads in S3
- do not pass full RSS articles or AI insight documents between states

Shape:

```json
{
  "workflow": "ai_insight",
  "run_id": "ai-insight-20260509T093015Z-a1b2c3d4",
  "status": "ai_insight_validated",
  "lake_bucket": "energy-market-lake-...",
  "dashboard_bucket": "energy-market-dashboard-public-...",
  "artifacts": {
    "energy_input": "curated/dataset=energy_input/run_id=.../payload.json",
    "news_summary": "curated/dataset=news_summary/run_id=.../payload.json",
    "ai_input_bundle": "curated/dataset=ai_input_bundle/run_id=.../payload.json",
    "ai_insight": "curated/dataset=ai_insight/run_id=.../payload.json",
    "dashboard_snapshot": "dashboard_snapshot_v1.json"
  },
  "summary": {
    "article_count": 18,
    "insight_count": 1,
    "risk_level": "watch"
  }
}
```

Rationale:

- Step Functions execution history remains useful in a technical demo.
- S3 remains the artifact store of record.
- State payloads stay small and avoid exposing full article/model content.
- Retry and failure paths can pass stable artifact references.

### Dashboard Publish Boundary

Use a separate public/static dashboard bucket, while keeping it optional in
Terraform for rebuild flexibility.

Private lake bucket:

```text
raw/
curated/
failed/
audit/
```

Public dashboard bucket:

```text
dashboard_snapshot_v1.json
static React assets, if included in this phase
```

Rationale:

- Cleaner public/private boundary.
- Lower blast radius if a bucket policy is misconfigured.
- Stronger SAP-C02 story around separation of concerns and least privilege.
- Minimal storage cost penalty for a small dashboard JSON/static site.

## Current Local Producers

- Generate dashboard data:
  - Script: `scripts/generate_dashboard.py`
  - Output: `dashboard-ui/public/dashboard-data.json`
- Export energy input:
  - Script: `scripts/export_energy_input_local.py`
  - Output: `docs/evidence/energy_input_v1.sample.json`
- Ingest curated news:
  - Script: `scripts/ingest_news_local.py`
  - Output: `docs/evidence/curated/news_summary_v1.sample.json`
- Create AI input bundle:
  - Script: `scripts/create_ai_input_bundle_local.py`
  - Output: `docs/evidence/ai/ai_input_bundle_v1.sample.json`
- Merge AI insight:
  - Script: `scripts/merge_ai_insight_local.py`
  - Output: `docs/evidence/curated/ai_insight_v1.sample.json`
- Publish dashboard snapshot:
  - Script: `scripts/publish_dashboard_snapshot_local.py`
  - Output: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`
- Validate contracts:
  - Script: `scripts/validate_contracts.py`
  - Output: pass/fail for good and known-bad samples

## Target AWS Data Contracts

- `energy_input_v1.json`
  - Location: `curated/source=ai_orchestration/dataset=energy_input/run_id=<run_id>/payload.json`
  - Producer: Athena export Lambda
  - Consumer: AI bundle Lambda
- `news_summary_v1.json`
  - Location: `curated/source=ai_orchestration/dataset=news_summary/run_id=<run_id>/payload.json`
  - Producer: news ingest Lambda
  - Consumer: AI bundle Lambda
- `ai_input_bundle_v1.json`
  - Location: `curated/source=ai_orchestration/dataset=ai_input_bundle/run_id=<run_id>/payload.json`
  - Producer: AI bundle Lambda
  - Consumer: AI merge Lambda
- `ai_insight_v1.json`
  - Location: `curated/source=ai_orchestration/dataset=ai_insight/run_id=<run_id>/payload.json`
  - Producer: deterministic AI merge Lambda
  - Consumer: publisher Lambda
- `dashboard_snapshot_v1.json`
  - Location: `s3://<dashboard-bucket>/dashboard_snapshot_v1.json`
  - Producer: publisher Lambda
  - Consumer: React dashboard

Failure and audit paths:

```text
s3://<lake-bucket>/failed/workflow=ai_insight/component=<component>/run_id=<run_id>/payload.json
s3://<lake-bucket>/audit/workflow=ai_insight/run_id=<run_id>/summary.json
```

## S3 Artifact Contract

State now:

```text
Local evidence files exist under docs/evidence/ and dashboard-ui/public/.
```

Target state:

```text
Every Phase 8 workflow artifact has an exact S3 location and can be tied back
to one run_id.
```

Private lake bucket:

```text
s3://<data_bucket_name>/
```

Private artifact prefixes:

- Energy input:
  - Prefix: `curated/source=ai_orchestration/dataset=energy_input/`
  - Payload key: `<prefix>run_id=<run_id>/payload.json`
- News summary:
  - Prefix: `curated/source=ai_orchestration/dataset=news_summary/`
  - Payload key: `<prefix>run_id=<run_id>/payload.json`
- AI input bundle:
  - Prefix: `curated/source=ai_orchestration/dataset=ai_input_bundle/`
  - Payload key: `<prefix>run_id=<run_id>/payload.json`
- AI insight:
  - Prefix: `curated/source=ai_orchestration/dataset=ai_insight/`
  - Payload key: `<prefix>run_id=<run_id>/payload.json`
- Failed payloads:
  - Prefix: `failed/workflow=ai_insight/component=<component>/`
  - Payload key: `<prefix>run_id=<run_id>/payload.json`
- Audit summaries:
  - Prefix: `audit/workflow=ai_insight/`
  - Payload key: `<prefix>run_id=<run_id>/summary.json`

Public dashboard bucket:

```text
s3://<dashboard_bucket_name>/
```

Public artifact keys:

- Dashboard snapshot:
  - Key: `dashboard_snapshot_v1.json`
- Optional immutable dashboard snapshot:
  - Key: `snapshots/run_id=<run_id>/dashboard_snapshot_v1.json`
- Optional React static assets:
  - Prefix: `assets/`

Trade-off decision:

- `source=ai_orchestration` keeps Phase 8 curated JSON separate from existing
  lakehouse datasets.
- `dataset=<contract-name>` makes IAM and Athena/catalog decisions easier later.
- `run_id=<run_id>` keeps every artifact traceable to the Step Functions run.
- the public bucket contains only approved dashboard output, not raw, curated,
  failed, or audit artifacts.

## Terraform Variable Interface

State now:

```text
Terraform already supports an existing or newly created data lake bucket.
```

Target state:

```text
Phase 8 has named Terraform inputs for orchestration, public publishing,
retention, notifications, and schedule control.
```

Variables to add:

- `create_dashboard_bucket`
  - Type: `bool`
  - Default: `true`
  - Purpose: create a separate public/static dashboard bucket.
- `dashboard_bucket_name`
  - Type: `string`
  - Default: `""`
  - Purpose: existing or desired dashboard bucket name.
- `ai_orchestration_enabled`
  - Type: `bool`
  - Default: `false`
  - Purpose: keep Phase 8 schedule/manual resources off until validated.
- `ai_orchestration_schedule_expression`
  - Type: `string`
  - Default: `cron(30 6 * * ? *)`
  - Purpose: daily EventBridge schedule after manual proof.
- `ai_orchestration_log_retention_days`
  - Type: `number`
  - Default: `14`
  - Purpose: CloudWatch retention for Phase 8 logs.
- `ai_orchestration_lambda_timeout_seconds`
  - Type: `number`
  - Default: `300`
  - Purpose: Lambda timeout for news/AI handlers.
- `ai_orchestration_lambda_memory_size`
  - Type: `number`
  - Default: `512`
  - Purpose: Lambda memory for RSS parsing and JSON processing.
- `ai_orchestration_news_limit_per_feed`
  - Type: `number`
  - Default: `4`
  - Purpose: keep RSS ingest bounded and low cost.
- `ai_orchestration_news_max_articles`
  - Type: `number`
  - Default: `18`
  - Purpose: keep dashboard/news payloads compact.
- `ai_orchestration_feeds`
  - Type: `list(string)`
  - Default: current local RSS feed set.
  - Purpose: make news sources configurable without code changes.
- `ai_orchestration_sns_email`
  - Type: `string`
  - Default: `""`
  - Purpose: optional email subscription for failure notifications.
- `ai_orchestration_state_machine_name`
  - Type: `string`
  - Default: `energy-market-ai-insight-orchestration`
  - Purpose: predictable Step Functions state-machine name.

Environment variables for Lambda handlers:

- `AWS_REGION`
- `DATA_BUCKET`
- `DASHBOARD_BUCKET`
- `ATHENA_DATABASE`
- `ATHENA_WORKGROUP`
- `ATHENA_OUTPUT_LOCATION`
- `NEWS_FEEDS`
- `NEWS_LIMIT_PER_FEED`
- `NEWS_MAX_ARTICLES`
- `AI_ORCHESTRATION_MODE`

Initial value for `AI_ORCHESTRATION_MODE`:

```text
deterministic
```

## Target AWS Workflow

```text
EventBridge schedule or manual execution
  -> Step Functions state machine
    -> ExportEnergyInput
    -> IngestNewsSummary
    -> ValidateInputs
    -> CreateAiInputBundle
    -> MergeAiInsightDeterministic
    -> ValidateAiInsight
    -> PublishDashboardSnapshot
    -> WriteAuditSuccess

Any failed validation or runtime error:
  -> WriteFailedPayload
  -> PublishSnsFailure
  -> KeepPreviousGoodDashboardSnapshot
```

## Implementation Checklist With Time Estimates

### 1. Phase 8 Design Lock

Estimate: 0.5 day

- [ ] Confirm the branch starts from clean `main`.
- [ ] Review current local script inputs and outputs.
- [x] Lock run ID format.
- [x] Lock Step Functions payload shape.
- [x] Decide whether dashboard output reuses the lake bucket or a separate
      public/static bucket.
- [x] Lock exact S3 prefixes and Terraform variable names.
- [x] Record environment variables and Terraform variables.

Acceptance:

- This plan is committed and points to concrete local scripts and AWS paths.

### 2. Shared Runtime Utilities

Estimate: 1 day

- [ ] Add shared S3 JSON read/write helpers.
- [ ] Add run ID generation helper.
- [ ] Add contract validation helper usable by Lambda handlers.
- [ ] Refactor local scripts only where needed so local and AWS paths share
      business logic.
- [ ] Preserve local demo commands.

Acceptance:

- Existing local scripts still run.
- `python -m compileall scripts lambda glue` passes.
- Contract validation still passes.

### 3. Lambda Handler Slice

Estimate: 1.5-2 days

- [ ] Add `lambda/news_ai_orchestration.py` or a small handler package.
- [ ] Implement `ExportEnergyInput` handler.
- [ ] Implement `IngestNewsSummary` handler.
- [ ] Implement `CreateAiInputBundle` handler.
- [ ] Implement `MergeAiInsightDeterministic` handler.
- [ ] Implement `PublishDashboardSnapshot` handler.
- [ ] Add handler-level structured output for Step Functions.

Acceptance:

- Each handler can be invoked locally with a sample event.
- Each handler writes the expected S3 key when configured for AWS.

### 4. Validation And Quarantine Slice

Estimate: 1 day

- [ ] Validate `energy_input_v1`.
- [ ] Validate `news_summary_v1`.
- [ ] Validate `ai_input_bundle_v1`.
- [ ] Validate `ai_insight_v1`.
- [ ] Validate `dashboard_snapshot_v1`.
- [ ] Write invalid payloads to `failed/`.
- [ ] Preserve the previous good public dashboard snapshot on failure.
- [ ] Add failure reason, component, schema name, and run ID to failed records.

Acceptance:

- A known-bad AI output is rejected.
- No invalid output reaches the dashboard publish location.

### 5. Terraform Foundation

Estimate: 1-1.5 days

- [ ] Add or extend S3 prefix conventions for curated, failed, audit, and public
      dashboard outputs.
- [ ] Add Lambda IAM permissions for required S3 prefixes.
- [ ] Add Athena query permissions for energy export if needed.
- [ ] Add CloudWatch log groups with retention.
- [ ] Add SNS topic for failure notifications.
- [ ] Add Step Functions execution role.
- [ ] Keep a new S3 bucket optional; default to existing lake bucket.

Acceptance:

- `terraform fmt` passes.
- `terraform validate` passes after initialization.
- Plan output is documented before apply.

### 6. Step Functions Orchestration

Estimate: 1-1.5 days

- [ ] Add state machine definition.
- [ ] Wire Lambda task states.
- [ ] Add retry policies for RSS/network and Athena steps.
- [ ] Add catch paths to write failed payloads and publish SNS notifications.
- [ ] Add EventBridge schedule, initially disabled or manual-only.
- [ ] Add manual execution command to docs.

Acceptance:

- Manual Step Functions execution completes successfully.
- A forced validation failure routes to failed/SNS path.

### 7. Public Dashboard Publish

Estimate: 0.5-1 day

- [ ] Publish only `dashboard_snapshot_v1.json` and approved static assets.
- [ ] If CloudFront is included, add cache behavior that does not trap stale JSON.
- [ ] Confirm the React dashboard reads only public-safe snapshot data.
- [ ] Add a smoke test for dashboard JSON availability.

Acceptance:

- Public dashboard snapshot returns HTTP 200.
- Private raw, curated, failed, and audit paths are not exposed.

### 8. Evidence And Docs Closeout

Estimate: 0.5-1 day

- [ ] Capture successful state-machine execution evidence.
- [ ] Capture failed validation evidence.
- [ ] Capture S3 output key evidence.
- [ ] Capture CloudWatch/SNS evidence.
- [ ] Update `README.md`.
- [ ] Update `docs/setup.md`.
- [ ] Update `docs/demo-walkthrough.md`.
- [ ] Add Phase 8 closeout evidence under `docs/evidence/`.

Acceptance:

- Demo can explain the AI orchestration boundary in under two minutes.
- Rebuild/setup docs contain the exact AWS CLI and Terraform commands used.

## Estimated Effort

Minimum useful Phase 8:

```text
5-7 working days
```

Portfolio-grade Phase 8 with evidence and docs:

```text
8-12 working days
```

Defer until Phase 9 or later:

```text
Bedrock InvokeModel: +2-4 working days
OpenClaw managed runtime: +3-5 working days
```

## Token-Saving Execution Slices

Use these as PR-sized implementation chunks:

1. Design lock and S3 contract doc.
2. Shared runtime utilities and local compatibility.
3. Lambda handlers.
4. Validation and quarantine.
5. Terraform IAM/SNS/logs/Step Functions.
6. Manual AWS execution evidence.
7. Public dashboard publish and docs closeout.

## Phase 8 Done Gate

Phase 8 is complete when:

- The AWS workflow can run manually through Step Functions.
- Energy input, news summary, AI bundle, AI insight, and dashboard snapshot are
  all produced as S3-backed JSON contracts.
- Invalid AI output is quarantined and does not publish.
- The previous good dashboard snapshot remains available after failure.
- CloudWatch logs and SNS notification evidence exist.
- The React dashboard consumes only the approved public snapshot.
- Setup and demo docs can recreate the workflow.
