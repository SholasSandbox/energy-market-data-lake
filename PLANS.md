# PLANS.md

<!-- markdownlint-disable MD013 -->

Source of truth for this plan:

- `README.md` for current platform scope and target model
- `docs/four-week-project-plan.md` for the delivery-focused 4-week MVP plan
- `dashboard-ui/src/App.tsx` for current React implementation reality
- `docs/dashboard-ia-spec.md` for the next dashboard design direction
- `docs/phase-8-aws-ai-insight-orchestration.md` for the completed AWS AI
  orchestration phase
- `docs/phase-9-terraform-import-hardening.md` for the completed Terraform
  import and hardening phase
- `docs/phase-10-dashboard-implementation.md` for the active dashboard
  implementation phase

## Current Baseline

The repo now shows a strong end-to-end mixed-energy demo:

- scheduled ingestion with EventBridge -> Lambda
- raw landing in S3
- Glue crawler + ETL pattern for curated data
- Athena query layer
- generated dashboard outputs
- React + TypeScript dashboard with a Phase 10 operator-focused `Overview`
  page
- ENTSOG gas raw ingestion, curated Parquet, Glue Catalog, Athena validation,
  and dashboard context
- local RSS/news evidence, deterministic AI insight merge, schema validation,
  and public-safe dashboard snapshot

The current implementation boundary is:

- electricity and gas are proven through the lakehouse/dashboard path
- news and AI insight are implemented as a local MVP and AWS-managed manual
  orchestration path
- Terraform now tracks the core lakehouse and Phase 8 resources, with residual
  ingestion Lambda drift documented
- Phase 10 is the active implementation phase for turning the React dashboard
  into a stronger operator-facing product surface, now in documentation and
  demo closeout

## Delivery Order

### Phase 1: Stabilize Ingestion And Lakehouse

Goal: make the ingestion and transformation flow dependable before expanding the analytical surface area.

Working checklist: `docs/phase-1-stabilize-ingestion-lakehouse.md`

Focus:

- keep scheduled ingestion reliable and repeatable
- confirm source freshness and schema consistency
- improve run evidence for ingestion, crawler, ETL, and Athena outputs
- tighten data quality checks around missing settlements, freshness, and source coverage
- keep diagrams, README, and dashboard outputs aligned with actual implemented behavior

Definition of stable:

- scheduled ingestion runs consistently without manual repair
- crawler and ETL outputs are reproducible
- Athena-backed outputs are trustworthy enough to feed dashboard pages
- data quality checks make incomplete or stale data obvious

### Phase 2: Implement Gas End-To-End

Goal: move ENTSOG gas from target-model intent into the same practical lakehouse flow used for electricity.

Focus:

- finalize gas raw dataset contracts from ENTSOG inputs
- define curated gas schema under the README target layout
- implement Glue transformation for gas datasets
- expose curated gas data through Athena
- update dashboard JSON generation to include gas-aware derived outputs where appropriate
- update docs and diagrams so gas is described consistently across storage, ETL, Athena, and presentation layers

Definition of done:

- gas data lands in raw S3
- gas data is transformed into curated form
- gas data is queryable in Athena
- README, diagrams, and generated outputs all reflect that implemented state

### Phase 3: Expand The React Dashboard

Goal: turn the current overview-first React shell into a multi-page decision-support dashboard.

This phase starts only after:

- the ingestion layer is stable
- the gas pipeline is implemented end-to-end

#### 3A. Portfolio Risk

Future development based on `docs/dashboard-ia-spec.md` and the current React navigation:

- implement a real `Portfolio Risk` page instead of reusing the overview shell
- add margin, open exposure, hedge cover, and breached-book KPI strip
- add P&L by book views
- add hedge coverage vs target views
- add hedge cost vs market price views
- add a fuller risk table with row highlighting and investigation pathways

Why later:

- this page depends on stable commercial metrics and trustworthy cross-dataset outputs
- it becomes more valuable once both electricity and gas are represented in the platform model

#### 3B. Market Context

Future development based on `docs/dashboard-ia-spec.md` and the current React navigation:

- implement a real `Market Context` page
- separate external market conditions from the portfolio story
- add deeper price trend views across selected ranges
- add demand trend views across selected ranges
- add intraday profile views
- add regime callouts such as elevated-price or abnormal-demand conditions

Why later:

- the current overview already includes a market context footer
- the standalone page should be introduced only after the underlying data pipeline is stable enough to support richer exploration

#### 3C. Data Quality

The React app already treats `Data Quality` as a distinct view. After ingestion stability and gas rollout:

- extend freshness checks across all supported sources
- show gas-specific coverage and completeness checks
- surface latest successful ingestion, crawler, ETL, and dashboard-generation timestamps
- make quality status easier to compare across electricity and gas

### Phase 8: AWS AI Insight Orchestration

Goal: move the local news and deterministic AI insight MVP into AWS
orchestration without introducing Bedrock or OpenClaw before the validation
boundary is proven.

Status: complete.

Working checklist and runbook:

- `docs/phase-8-aws-ai-insight-orchestration.md`
- `docs/phase-8-operational-runbook.md`

Focus:

- convert local news, energy input, AI bundle, AI merge, validation, and
  dashboard publish steps into S3-backed AWS workflow steps
- orchestrate the workflow with Step Functions
- quarantine invalid AI output before publish
- keep the previous good dashboard snapshot available after failures
- add CloudWatch logs and SNS failure notifications
- capture evidence and setup commands for rebuild/demo

Definition of done:

- Step Functions can run the workflow manually
- S3 contains contract-shaped energy input, news summary, AI bundle, AI insight,
  dashboard snapshot, audit, and failed-path outputs
- invalid AI output does not reach the dashboard
- public dashboard reads only approved dashboard snapshot data
- docs and evidence explain the orchestration boundary clearly

### Phase 9: Terraform Import And Operating Hardening

Goal: adopt the manually created AWS lakehouse resources into Terraform state,
classify drift, and leave the platform in a reproducible operating posture
without enabling schedules prematurely.

Status: complete.

Working tracker: `docs/phase-9-terraform-import-hardening.md`

Definition of done:

- Phase 8 orchestration resources remain reproducible from Terraform
- older lakehouse resources are imported into Terraform state
- accepted governance and executable drift is applied
- ingestion Lambda drift is documented with redeploy criteria
- schedules remain disabled unless a later decision explicitly enables them

### Phase 10: Dashboard Implementation

Goal: implement the first operator-focused React dashboard slice now that the
lakehouse, AI orchestration, and Terraform posture are proven.

Status: active, in documentation/demo closeout.

Working checklist: `docs/phase-10-dashboard-implementation.md`

Focus:

- maintain the implemented `Overview` page from `docs/dashboard-ia-spec.md`
- preserve the global filter bar, executive KPIs, P&L drivers, risk coverage,
  exception table, market context strip, and data-quality state
- keep the dashboard driven by approved public snapshot JSON only
- make stale, limited, or missing evidence visible rather than hidden
- keep README, demo walkthrough, and LinkedIn copy aligned with the implemented
  UI and proof evidence

Definition of done:

- React build passes
- contract validation passes
- desktop, tablet, and mobile screenshots are captured
- docs describe Phase 8 and Phase 9 as completed foundations
- the demo story connects lakehouse data, AI orchestration controls, and the
  visible dashboard decision surface

## Suggested Immediate Next Steps

1. Review the Phase 10 documentation/demo closeout diff.
2. Run the final build, contract validation, and Markdown sanity checks.
3. Commit the Phase 10 implementation and evidence on
   `feature/phase10-dashboard-implementation`.
4. Keep diagram redraws as the next bounded follow-up unless they become
   required for the Phase 10 PR.

## Planning Rule

If implementation reality and design ambition diverge, update this file in the following order:

1. `README.md` reflects current intended platform truth.
2. `PLANS.md` reflects delivery sequence from that truth.
3. diagrams and React pages reflect what is actually implemented now versus what is still future work.
