# Energy Market Data Lake + News Insight Dashboard

<!-- markdownlint-disable MD013 -->

A budget-conscious AWS portfolio project for ingesting energy market data,
transforming it into a queryable lakehouse, and presenting decision-ready
dashboard outputs. The implemented baseline is a serverless energy data lake
using Lambda, S3, Glue, Athena, EventBridge, and an operator-focused React
dashboard. The news + AI extension adds RSS summaries, strict JSON contracts,
a deterministic AI-style merge, failure samples, a public-safe dashboard
snapshot, and a live-proven AWS Step Functions orchestration path.

Region: **eu-west-2 (London)**

## Project Scope

### Implemented Today

- Scheduled ingestion path using EventBridge and Lambda.
- Raw S3 landing zone for energy market payloads.
- Glue crawler and Glue ETL pattern for raw-to-curated transformation.
- Athena query layer over curated Parquet data.
- Evidence generation under `docs/evidence/`.
- HTML dashboard generation from Athena-backed data.
- React + TypeScript dashboard under `dashboard-ui/`, with a Phase 10
  `Overview` page for alerts, executive KPIs, P&L drivers, risk coverage,
  market context, AI insight, and data-quality state.
- ENTSOG gas proof from raw ingestion through curated Parquet, Glue Catalog, Athena query, and validation evidence.
- ENTSOG gas context cards and selected pointDirection table in the React dashboard.
- Local RSS/news ingestion evidence.
- Expanded public-safe market-news article grid for gas and electricity context.
- JSON schema contracts for energy, news, AI insight, and dashboard snapshot outputs.
- Local AI input bundle and deterministic AI insight merge.
- Validator checks for good evidence and intentionally bad failure samples.
- Public-safe dashboard snapshot loaded by the React app.
- Visible data freshness warning for old local demo evidence.
- Phase 8 AWS AI insight orchestration with Lambda, Step Functions, S3
  artifacts, validation gates, failed-run quarantine, and manual execution
  evidence.
- Phase 10 responsive dashboard evidence for desktop, tablet, and mobile.
- Phase 11 deterministic dashboard filters with URL-backed state, filtered
  portfolio views, export metadata, and desktop/mobile evidence.
- Phase 12 Terraform foundation for private S3 plus CloudFront dashboard
  delivery, disabled by default until a live hosting decision.
- Phase 13 dashboard hosting publish/runbook proof with plan-only evidence and
  explicit `--apply` for live S3/CloudFront writes.
- Phase 14D ingestion Lambda reconciliation applied with rollback evidence,
  smoke invoke proof, Terraform tags, and a clean post-apply Terraform plan.
- Phase 14E dashboard hosting apply-candidate plan reviewed cleanly:
  CloudFront/OAC/response headers/S3 bucket policy only.
- Phase 14F CloudFront hosting live apply completed with dashboard publish,
  invalidation, HTTP header evidence, and clean post-apply Terraform plan.
- Phase 15 hosted-dashboard demo hardening adds the live CloudFront URL and
  verification path to the demo walkthrough.
- Phase 16 restored the live AI `dashboard_snapshot_v1.json` path from the
  successful Phase 8 curated artifacts without changing infrastructure.
- Phase 17 managed AI refresh preflight selects a Bedrock-first path while
  keeping deterministic fallback, schedules disabled, and Terraform apply out
  of scope.
- Phase 17A adds a code-only Bedrock adapter and `MergeAiInsightManaged`
  handler path with fake-client validation and no live model invocation.
- Phase 17B confirms the first live Bedrock invocation should not run yet:
  Claude 3 Haiku needs model-agreement access, while Mistral Ministral 8B needs
  provider-specific adapter proof first.
- Phase 17C adds Mistral request/response compatibility behind fake-client
  proof, keeping Anthropic support and deterministic fallback intact.
- Phase 17D performs one live Mistral invocation under the `$0.10` cap; output
  is rejected by `ai_insight_v1`, proving the validation gate protected the
  dashboard.
- Phase 17E hardens the Mistral prompt and response-shape handling locally,
  accepting only the observed one-key `ai_insight` wrapper before schema
  validation and proving the behavior with fake-client tests.
- Phase 17F performs one controlled second live Mistral invocation; the output
  is stopped before validation because it is incomplete fenced JSON, proving no
  live retry or dashboard publish should happen without another local hardening
  slice.
- Phase 17G hardens the Mistral JSON-completion path locally, raises the
  managed output-token default to `1600`, and adds fake-client coverage for
  incomplete fenced JSON before any third live call.
- Phase 17H performs one controlled third live Mistral invocation; the raised
  token cap prevents truncation, but validation still rejects a root
  `ai_insight_v1` wrapper before any dashboard publish.
- Phase 17I hardens root-wrapper handling locally, accepting only the exact
  `ai_insight_v1` wrapper while keeping broad wrapper shapes rejected.
- Phase 17J preflight recommends a fourth controlled live Mistral invocation
  as a go candidate, but does not invoke Bedrock without explicit approval.
- Phase 17J execution performs one controlled fourth live Mistral invocation;
  root-wrapper normalization works live, but schema validation still rejects
  missing nested insight fields before any dashboard publish.
- Phase 17K hardens the nested Mistral insight-field contract locally after the
  Phase 17J validation failure, requiring exact insight fields, separate
  energy/news reference arrays, and `validation_notes` as an array before any
  fifth live call.
- Phase 17L preflight recommends a fifth controlled live Mistral invocation as
  a go-candidate, but does not invoke Bedrock without explicit approval.
- Phase 17L execution performs one controlled fifth live Mistral invocation;
  output advances beyond the Phase 17J missing-field failure but still fails
  validation on nested object shape before any dashboard publish.
- Phase 17M hardens nested Mistral object shapes locally, requiring
  `time_window` as a `{start,end}` object and forbidding extra reference fields
  such as `value` before any sixth live call.
- Phase 17N preflight recommends a sixth controlled live Mistral invocation as
  a go-candidate, but does not invoke Bedrock without explicit approval.
- Phase 17N execution performs one controlled sixth live Mistral invocation;
  output passes `ai_insight_v1` validation in memory, but no payload is
  committed or published because the boundary was sanitized metadata only.
- Phase 17O preflight holds dashboard publish and managed workflow deployment
  as no-go decisions until a public-safe validated payload is captured as a
  separate evidence boundary.
- Phase 17P captures a public-safe, schema-valid managed AI payload as evidence
  while keeping dashboard publish and managed workflow deployment blocked.
- Phase 17Q converts the Phase 17P payload into a local
  `dashboard_snapshot_v1` candidate and keeps publish blocked until managed
  source links are hardened for the public dashboard.
- Phase 17R hardens dashboard source-link generation locally so managed energy
  references use public dashboard targets while external news URLs are
  preserved.
- Phase 17S makes the managed AI dashboard publish a go-candidate, but keeps
  S3 writes and CloudFront invalidation blocked until explicit execution
  approval.
- Phase 17S execution publishes the approved managed AI dashboard snapshot to
  CloudFront latest plus immutable snapshot paths, with rollback evidence.
- Phase 17T verifies the hosted dashboard demo after managed AI publication:
  CloudFront routes, latest snapshot, immutable snapshot, schema validation,
  and source-link hardening all pass read-only checks.
- Phase 17U keeps managed workflow deployment blocked after preflight review.
- Phase 17V models the managed workflow Terraform/IAM delta plan-only.
- Phase 17W execution applies managed workflow routing while schedules remain
  disabled.
- Phase 17X records managed workflow smoke execution as a publish-capable
  go-candidate only.
- Phase 17Y runs one manual managed workflow smoke; it fails safely before
  Bedrock because the deployed Lambda package is stale.
- Phase 17Z proves the Lambda package refresh preflight locally: the rebuilt
  package contains `MergeAiInsightManaged`, while apply remains blocked until
  explicit approval.
- Phase 17Z execution refreshes the deployed Lambda package with the normal
  root Terraform plan; the live code hash now matches the package containing
  `MergeAiInsightManaged`, while no workflow retry or dashboard publish occurs.
- Phase 17AA records the managed workflow second smoke as a go-candidate, not
  automatic execution, after confirming Lambda refresh, rollback metadata,
  schedule-disabled state, and Terraform no-change.
- Phase 17AA execution runs one controlled managed workflow smoke successfully:
  the managed Bedrock/Mistral merge validates, publishes latest plus immutable
  dashboard snapshots, and leaves schedules disabled.
- Phase 17AB verifies the hosted dashboard after the successful managed
  workflow smoke; latest and immutable snapshot paths are healthy, but managed
  workflow source labels need sanitization before the demo surface is fully
  hardened.
- Phase 17AC locally hardens managed workflow source-label generation so
  private lake references collapse to public-safe dashboard evidence labels;
  deployment/publish remains a separate decision boundary.

### Deferred AWS Extension

- Use the implemented Bedrock adapter and managed AI handler as the proof
  boundary before live model invocation.
- Run the first live Bedrock invocation only behind an explicit token budget,
  IAM delta review, and rollback path.
- Prefer a Mistral compatibility proof before live invocation if cost control
  is the priority.
- Run the first live Mistral invocation only as a single approved proof with a
  hard one-run budget cap.
- Harden Mistral prompt and response-shape handling locally before any second
  live invocation.
- Run OpenClaw in a clear runtime only after the managed cloud AI boundary is
  proven.
- Enable the Phase 8 EventBridge schedule after another operating decision.
- Add CloudWatch alarms after the manual workflow has settled.
- Add DNS, ACM certificate, and custom domain for the public dashboard.

## Current Data Scope

- **UK electricity (Elexon)**: demand by bidding zone (GSP proxy) and system prices (SBP/SSP).
- **EU electricity (ENTSO-E)**: actual load and day-ahead prices for GB, FR, DE-LU, and NL.
- **EU gas (ENTSOG)**: raw and curated physical flows plus allocation-based demand proxy using selected pointDirection IDs.
- **News summaries**: local RSS evidence linked to energy market movements.

## Current Architecture

```text
External Energy APIs
  |-- Elexon
  |-- ENTSO-E
  `-- ENTSOG
        |
        v
EventBridge schedules
created but disabled
        |
        v
Lambda Ingestion
        |
        v
S3 Raw Zone
        |
        v
Glue Crawler + Glue ETL
        |
        v
S3 Curated Zone
        |
        v
Athena
        |
        v
Approved dashboard JSON / React Dashboard
```

## News + AI Orchestration Architecture

```text
Energy dashboard input + RSS Feeds
        |
        v
Private AWS Processing Boundary
  Manual Step Functions execution
        |
        v
  Lambda deterministic orchestration
        |
        v
  validate + write run-scoped S3 artifacts
        |
        v
  validate ai_insight_v1 and dashboard_snapshot_v1
        |
        +------------------+
        |                  |
      valid             invalid
        |                  |
        v                  v
 dashboard bucket     S3 failed/ + SNS + CloudWatch
```

The public dashboard must never read directly from raw, curated, audit, or failed lake data.

EventBridge scheduling is deployed but intentionally disabled. Manual Step
Functions execution is the current safe operating mode.

## Local MVP Flow

```text
Local energy evidence + RSS feeds
        |
        v
validated energy_input_v1 + curated news_summary_v1
        |
        v
AI input bundle
        |
        v
deterministic local AI merge
        |
        v
validated ai_insight_v1
        |
        v
public dashboard_snapshot_v1.sample.json
        |
        v
React dashboard
```

## Demo Evidence

Use these artifacts to review or present the local MVP:

- Walkthrough: `docs/demo-walkthrough.md`
- Phase 10 Overview screenshots:
  - `docs/evidence/screenshots/dashboard-phase10-overview-desktop-20260514.png`
  - `docs/evidence/screenshots/dashboard-phase10-overview-tablet-20260514.png`
  - `docs/evidence/screenshots/dashboard-phase10-overview-mobile-20260514.png`
- Screenshot: `docs/evidence/screenshots/dashboard-week4-local-mvp.png`
- Tabbed dashboard screenshots:
  - `docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png`
  - `docs/evidence/screenshots/dashboard-power-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-20260507.png`
  - `docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png`
- Public dashboard snapshot: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`
- Expanded news refresh evidence: `docs/evidence/news-refresh-expanded-20260507.md`
- Curated AI insight evidence: `docs/evidence/curated/ai_insight_v1.sample.json`
- ENTSOG gas Phase 5 evidence: `docs/evidence/run-entsog-gas-20260506.md`
- ENTSOG Athena validation: `docs/evidence/athena-gas-schema-20260506.md`
- ENTSOG Athena query summary: `docs/evidence/athena-gas-query-summary-20260506.md`
- ENTSOG dashboard gas evidence: `docs/evidence/phase7-dashboard-gas-20260507.md`
- ENTSOG 7-day gas trend evidence: `docs/evidence/gas-7day-trend-20260507.md`
- Phase 8 AWS live execution evidence: `docs/evidence/phase8-aws-live-execution-20260511.md`
- Phase 8 operational runbook: `docs/phase-8-operational-runbook.md`

Run the local evidence pipeline:

```bash
source .venv/bin/activate
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

## Repository Layout

```text
athena/                Athena demo queries
config/                Sample environment settings
dashboard-ui/          React + TypeScript dashboard application
diagrams/              Mermaid, SVG, PNG, and generated architecture diagrams
docs/                  Active documentation and implementation plans
docs/archive/          Historical completed plans and old demo artifacts
docs/evidence/         Generated run, schema, and dashboard evidence
docs/evidence/screenshots/
                       Dashboard screenshots for portfolio/demo use
glue/                  Glue ETL code
infra/terraform/       Terraform Infrastructure as Code for AWS lakehouse resources
lambda/                Lambda ingestion code
scripts/               Local/demo helper scripts
```

## S3 Layout

Current and target storage layout:

```text
s3://<bucket>/
  raw/
    source=elexon/
      dataset=atl/
        date=YYYY-MM-DD/
      dataset=system_prices/
        date=YYYY-MM-DD/
    source=entsoe/
      dataset=actual_load/
        zone=gb|fr|de|nl/
        date=YYYY-MM-DD/
      dataset=day_ahead_prices/
        zone=gb|fr|de|nl/
        date=YYYY-MM-DD/
    source=entsog/
      dataset=gas_flow/
        point_direction=<id>/
        date=YYYY-MM-DD/
      dataset=gas_demand/
        point_direction=<id>/
        date=YYYY-MM-DD/
    source=news/
      dataset=rss_summary/
        date=YYYY-MM-DD/
  curated/
    dataset=electricity/
      source=elexon|entsoe/
      region=gb|fr|de|nl/
      date=YYYY-MM-DD/
    dataset=gas/
      region=eu/
      date=YYYY-MM-DD/
    dataset=news/
      date=YYYY-MM-DD/
  audit/
  failed/
  archive/
```

## Cost Controls

- Keep ingestion scheduled rather than always on.
- Use S3 lifecycle rules for raw data.
- Store curated data as partitioned Parquet to reduce Athena scan costs.
- Run Glue jobs daily, weekly, or manually for demo needs.
- Keep Lambda payloads and runtimes small.
- Avoid NAT Gateway, RDS, and always-on EC2 for the MVP.
- Add AWS Budget alerts before any live demo period.

## Local And Demo Commands

Set up the local Python helper environment:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

Validate the JSON schema contracts:

```bash
python scripts/validate_contracts.py
```

Run the local news + energy + AI insight pipeline:

```bash
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

Run the full demo closeout flow:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
BACKFILL_DAYS=30 ./scripts/closeout_demo.sh
```

Generate a polished HTML dashboard from Athena curated data:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 scripts/generate_dashboard.py
```

Generate JSON for the React app:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake
python3 scripts/generate_dashboard.py \
  --bucket energy-market-lake-464975959576-20260405 \
  --output-json dashboard-ui/public/dashboard-data.json
```

Run the React dashboard locally:

```bash
cd /Users/shola/Workspace/cloud-projects/energy-market-data-lake/dashboard-ui
npm install
npm run dev -- --host 127.0.0.1
```

Verify the app and public snapshot are served:

```bash
curl -I http://127.0.0.1:5173/
curl -I http://127.0.0.1:5173/dashboard_snapshot_v1.sample.json
```

Run a manual Phase 8 AWS orchestration proof:

```bash
export AWS_REGION=eu-west-2
export AI_ORCHESTRATION_STATE_MACHINE_ARN="arn:aws:states:eu-west-2:464975959576:stateMachine:energy-market-ai-insight-orchestration"
export EXECUTION_NAME="phase8-manual-$(date -u +%Y%m%dT%H%M%SZ)"

aws stepfunctions start-execution \
  --state-machine-arn "${AI_ORCHESTRATION_STATE_MACHINE_ARN}" \
  --name "${EXECUTION_NAME}" \
  --input '{}' \
  --region "${AWS_REGION}" \
  --query executionArn \
  --output text
```

The full operational runbook, including preflight, artifact verification, and
failure drill commands, lives in `docs/phase-8-operational-runbook.md`.

Find ENTSOG pointDirection IDs:

```bash
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --ids-only
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --ids-only --max-results 4
python3 scripts/entsog_point_directions.py --countries GB,UK,FR,DE,NL --save-env
```

Seed set validated on 2026-05-03 for both `Physical Flow` and `Allocation`:

```text
BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit
```

ENTSOG uses `UK` in operator-point metadata; the helper treats requested `GB` as `GB,UK` for gas selection.

Live-check a seed set:

```bash
python3 scripts/check_entsog_seed.py \
  --point-directions "BE-TSO-0001ITP-00061entry,BE-TSO-0001ITP-00115exit,CZ-TSO-0001ITP-00537entry,BE-TSO-0001ITP-00555exit" \
  --date 2026-05-03
```

Validate the curated gas Athena table:

```bash
python3 scripts/validate_athena_schema.py \
  --region eu-west-2 \
  --database energy_market_lake \
  --table curated_dataset_gas \
  --output-location s3://energy-market-lake-464975959576-20260405/athena-results/ \
  --expected-sources entsog \
  --output-file docs/evidence/athena-gas-schema-$(date +%Y%m%d).md
```

## Active Documentation

- `PLANS.md`: current delivery sequence and implementation guardrails.
- `docs/setup.md`: setup guide for the serverless energy lakehouse path.
- `docs/phase-1-stabilize-ingestion-lakehouse.md`: active stabilization checklist.
- `docs/entsoe-operationalization-checklist.md`: ENTSO-E reliability checklist.
- `docs/gas-implementation-checklist.md`: ENTSOG gas implementation checklist.
- `docs/entsog-gas-build-plan.md`: time-budgeted ENTSOG gas build tracker.
- `infra/terraform/lakehouse/README.md`: Terraform rebuild path with S3 remote backend and optional data bucket creation.
- `docs/dashboard-ia-spec.md`: React dashboard redesign direction.
- `docs/target-operating-model.md`: high-level target operating model for the
  completed platform vision.
- `docs/interview-demo-talking-points.md`: concise current-state pitch for
  Solution Architect interviews and demos.
- `docs/phase-10-dashboard-implementation.md`: completed Phase 10 dashboard
  implementation checklist and project plan.
- `docs/phase-11-dashboard-filters.md`: completed deterministic dashboard
  filter wiring phase.
- `docs/phase-12-dashboard-hosting-foundation.md`: optional CloudFront/static
  dashboard delivery foundation.
- `docs/phase-13-dashboard-hosting-publish-runbook.md`: publish/runbook proof
  for dashboard build artifacts, S3 sync commands, CloudFront invalidation,
  and evidence capture.
- `docs/phase-14-dashboard-hosting-live-apply-evidence.md`: conservative
  preflight, Lambda reconciliation evidence, proof commands, safety decision,
  and rollback path for live dashboard hosting evidence.
- `docs/phase-17-managed-ai-refresh-preflight.md`: managed AI refresh
  preflight and Bedrock-first decision with deterministic fallback.
- `docs/phase-17b-controlled-bedrock-invocation-preflight.md`: read-only
  model-access, cost, IAM, and go/no-go decision for first live Bedrock
  invocation.
- `docs/phase-8-aws-ai-insight-orchestration.md`: completed plan and checklist
  for AWS AI insight orchestration.
- `docs/phase-8-operational-runbook.md`: manual run, proof, failure drill, and
  demo commands for Phase 8.
- `docs/phase-9-terraform-import-hardening.md`: completed Terraform import and
  operating hardening tracker.
- `docs/demo-walkthrough.md`: concise demo script for the CloudFront-hosted
  dashboard, local fallback, and live orchestration story.
- `docs/news-dashboard-merged-execution-model.md`: 4-week news + AI + dashboard expansion plan.

## Diagrams

- `diagrams/target-operating-model.mmd`: high-level target operating model
  after the planned phases are complete.
- `diagrams/target-operating-model.svg`: rendered target operating model.
- `diagrams/target-aws-service-architecture.mmd`: target AWS service
  architecture with ingestion, lakehouse, AI orchestration, validation, and
  public dashboard delivery.
- `diagrams/target-aws-service-architecture.svg`: rendered target AWS service
  architecture.
- `diagrams/target_aws_service_architecture_icons.py`: Python `diagrams`
  source for the AWS-symbol version of the target service architecture.
- `diagrams/target_aws_service_architecture_icons.png`: rendered AWS-symbol
  target service architecture for interview and walkthrough use.
- `diagrams/target-aws-operations-control-plane.mmd`: target AWS operations
  control-plane view with Terraform, IAM, schedules, observability, alerts,
  budgets, and evidence.
- `diagrams/target-aws-operations-control-plane.svg`: rendered target AWS
  operations control-plane view.
- `diagrams/architecture.mmd`: compact current architecture.
- `diagrams/architecture.svg`: rendered compact current architecture.
- `diagrams/architecture_overview.png`: rendered lakehouse overview diagram;
  updated to include disabled schedules, Phase 8 orchestration, private
  audit/failed paths, and public dashboard JSON.
- `diagrams/flow_diagram.png`: rendered current data-flow diagram with Elexon,
  ENTSO-E, and ENTSOG raw-to-curated paths.
- `diagrams/news-dashboard-high-level.mmd`: high-level current-state diagram
  for news + AI orchestration + dashboard.
- `diagrams/news-dashboard-high-level.svg`: rendered high-level current-state
  diagram.
- `diagrams/news-dashboard-detailed.mmd`: detailed current-state diagram with
  trust boundaries, disabled schedules, validation gates, and failure paths.
- `diagrams/news-dashboard-detailed.svg`: rendered detailed current-state
  diagram.

Regenerate diagram assets after editing sources:

```bash
npx --yes @mermaid-js/mermaid-cli -i diagrams/target-operating-model.mmd -o diagrams/target-operating-model.svg
npx --yes @mermaid-js/mermaid-cli -i diagrams/target-aws-service-architecture.mmd -o diagrams/target-aws-service-architecture.svg
npx --yes @mermaid-js/mermaid-cli -i diagrams/target-aws-operations-control-plane.mmd -o diagrams/target-aws-operations-control-plane.svg
npx --yes @mermaid-js/mermaid-cli -i diagrams/architecture.mmd -o diagrams/architecture.svg
npx --yes @mermaid-js/mermaid-cli -i diagrams/news-dashboard-high-level.mmd -o diagrams/news-dashboard-high-level.svg
npx --yes @mermaid-js/mermaid-cli -i diagrams/news-dashboard-detailed.mmd -o diagrams/news-dashboard-detailed.svg

brew install graphviz
.venv/bin/python -m pip install diagrams
.venv/bin/python diagrams/flow_diagram.py
.venv/bin/python diagrams/architecture_overview.py
.venv/bin/python diagrams/target_aws_service_architecture_icons.py
```

## Archived Documentation

Older completed plans and demo artifacts have been moved to `docs/archive/`:

- `closeout-summary.md`
- `dashboard-wireframe-overview.html`
- `demo-checklist.md`
- `four-week-project-plan.md`
- `project-plan.md`

These are historical references, not the current delivery path.

## Current Delivery Priorities

1. Use `PLANS.md` as the current delivery control document.
2. Keep the React dashboard focused on approved snapshot JSON, URL-backed
   filters, and explicit publish evidence.
3. Keep local and AWS orchestration proof paths reproducible with schema
   validation and failure checks.
4. Keep Phase 8 manual orchestration proof reproducible and schedule-disabled.
5. Keep the CloudFront-hosted dashboard demo path reproducible, including the
   restored live AI snapshot path and sample fallback.
6. Keep managed AI refresh behind the Bedrock adapter contract and schema
   validation gates.
7. Treat Phase 17D as evidence that validation gates protect the dashboard even
   when live model output is malformed.
8. Treat Phase 17E as the local hardening proof required before any second
   live Mistral call.
9. Treat Phase 17F as evidence that prompt/token-budget hardening is needed
   before any further live Mistral call.
10. Treat Phase 17G as the local proof boundary before any third live Mistral
    invocation.
11. Treat Phase 17H as evidence that root-wrapper handling needs local
    hardening before any further live Mistral call.
12. Treat Phase 17I as the local proof boundary before any fourth live Mistral
    invocation.
13. Keep Phase 17J execution separate from the preflight decision: one call
    only after explicit approval, no retry, no dashboard publish.
14. Treat Phase 17J execution as evidence that nested schema-field hardening is
    needed before any further live Mistral call.
15. Treat Phase 17K as the local schema-field proof boundary before any fifth
    live Mistral invocation.
16. Keep Phase 17L execution separate from the preflight decision: one call
    only after explicit approval, no retry, no dashboard publish.
17. Treat Phase 17L execution as evidence that local object-shape hardening is
    needed before any sixth live Mistral call.
18. Treat Phase 17M as the local object-shape proof boundary before any sixth
    live Mistral invocation.
19. Keep Phase 17N execution separate from the preflight decision: one call
    only after explicit approval, no retry, no dashboard publish.
20. Treat Phase 17N execution as evidence that managed Mistral can produce
    schema-valid `ai_insight_v1`, while dashboard publication remains blocked.
21. Treat Phase 17O as the boundary that separates validated payload capture,
    dashboard publish, and managed workflow deployment.
22. Treat Phase 17P as evidence that a public-safe managed AI payload can be
    captured before any dashboard publication decision.
23. Treat Phase 17Q as evidence that dashboard publish also needs
    public-link readiness, not just schema validity.
24. Treat Phase 17R as the local source-link proof boundary before any managed
    AI dashboard publish decision.
25. Treat Phase 17S as a decision boundary only; publishing requires explicit
    execution approval.
26. Treat Phase 17S execution as the live managed AI dashboard publish proof;
    managed workflow deployment remains separate.
27. Treat Phase 17T as the read-only hosted-demo verification boundary after
    managed AI dashboard publication.
28. Treat Phase 17U as the preflight boundary before any managed workflow
    deployment; the deployed workflow still routes through deterministic merge.
29. Treat Phase 17V as the plan-only Terraform/IAM delta boundary; managed
    workflow routing remains disabled unless explicitly approved later.
30. Treat Phase 17W as the deployment decision boundary; managed workflow
    deployment is a go-candidate, not an apply-by-default state.
31. Treat Phase 17W execution as the controlled Terraform apply that switches
    workflow routing to managed mode while keeping schedules disabled.
32. Treat Phase 17X as the managed workflow smoke decision boundary; the smoke
    is publish-capable and still requires explicit execution approval.
33. Treat Phase 17Y as evidence that managed workflow routing reaches the
    managed merge state but the live Lambda package needs refresh before retry.
34. Treat Phase 17Z as the Lambda package refresh preflight; the rebuilt
    package contains `MergeAiInsightManaged`, but apply requires explicit
    approval.
35. Treat Phase 17Z execution as the Lambda package refresh proof; it does not
    authorize an automatic second managed workflow smoke.
36. Treat Phase 17AA as the managed workflow second-smoke decision boundary;
    execution remains explicit-approval only because success can publish the
    dashboard snapshot.
37. Treat Phase 17AA execution as the first successful managed workflow smoke;
    schedule enablement remains a separate operating decision.
38. Treat Phase 17AB as read-only hosted-demo verification after the managed
    workflow snapshot publish; it found source-label sanitization drift that
    must be fixed before any repeated managed workflow run or schedule
    enablement.
39. Treat Phase 17AC as local source-label sanitization proof; deploying that
    fix into Lambda, running another workflow smoke, or publishing a new
    snapshot remains a later explicit decision.
40. Defer DNS, ACM, alarms, schedules, repeated live model invocation, and
    OpenClaw runtime until a phase explicitly targets those operating
    boundaries.

## Notes

- Elexon base URL: `https://data.elexon.co.uk/bmrs/api/v1` (no API key).
- ENTSO-E requires registration and an API token stored in SSM or Secrets Manager.
- ENTSOG is public; the current gas proof uses a four-point seed and the `Physical Flow` plus `Allocation` indicators.
- OpenClaw/local model execution is outside AWS unless moved into Bedrock or managed compute.
- Phase 8 still proves orchestration, validation, and publish controls through
  deterministic workflow logic; the live dashboard snapshot has separately
  published a managed Bedrock/Mistral evidence payload.
- Phase 17U keeps managed workflow deployment blocked until the Terraform/IAM,
  Lambda environment, Step Functions routing, rollback, and failure-path delta
  is reviewed plan-only.
- Phase 17V models that delta behind an opt-in Terraform switch and captures
  both an isolated managed plan and deterministic rollback plan; no deployment
  has occurred.
- Phase 17W records the managed workflow deployment as a go-candidate only;
  execution still requires explicit approval, CloudFront preservation, and
  schedules disabled.
- Phase 17W execution applied the managed workflow Terraform delta; the
  state-machine now routes to managed AI, but no live workflow execution or
  schedule enablement has occurred.
- Phase 17X records managed workflow smoke execution as a go-candidate only;
  it is publish-capable and needs rollback evidence before any run.
- Phase 17Y ran one manual managed workflow smoke; it failed safely before
  Bedrock because the deployed Lambda package did not recognize
  `MergeAiInsightManaged`, and the dashboard snapshot remained unchanged.
- Phase 17Z rebuilt the local Terraform Lambda package and proved it contains
  `MergeAiInsightManaged`; the package refresh remains an explicit apply
  boundary before any second managed workflow smoke.
- Phase 17Z execution refreshed the deployed Lambda package and left schedules
  disabled, dashboard unchanged, and Terraform at no-change.
- Phase 17AA confirms a second managed workflow smoke is a go-candidate, but
  does not run it; rollback-first evidence and explicit approval remain
  required.
- Phase 17AA execution ran one successful managed workflow smoke, published the
  managed workflow dashboard snapshot, and kept the EventBridge schedule
  disabled.
- Phase 17AB read-only checks verify the hosted dashboard, latest workflow
  snapshot, immutable workflow snapshot, schema validity, recent execution
  state, and schedule-disabled posture; they also find that managed workflow
  source labels still need public-surface sanitization.
- Phase 17AC locally proves managed workflow source-label sanitization:
  private lake references no longer appear in dashboard source labels, while
  date context and the safe `dashboard-data.json` URL are retained.
