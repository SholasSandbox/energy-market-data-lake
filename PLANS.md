# PLANS.md

<!-- markdownlint-disable MD013 -->

Source of truth for this plan:

- `README.md` for current platform scope and target model
- `docs/four-week-project-plan.md` for the delivery-focused 4-week MVP plan
- `dashboard-ui/src/App.tsx` for current React implementation reality
- `docs/dashboard-ia-spec.md` for the next dashboard design direction
- `docs/target-operating-model.md` for the envisioned end-state operating
  model and AWS service-level companion diagrams
- `docs/phase-8-aws-ai-insight-orchestration.md` for the completed AWS AI
  orchestration phase
- `docs/phase-9-terraform-import-hardening.md` for the completed Terraform
  import and hardening phase
- `docs/phase-10-dashboard-implementation.md` for the completed dashboard
  implementation phase
- `docs/phase-11-dashboard-filters.md` for the completed deterministic
  dashboard interaction phase

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
- Phase 10 is complete: the React dashboard now has a stronger
  operator-facing `Overview` surface, refreshed demo/docs, responsive
  screenshot evidence, and current architecture diagrams
- the target operating model is now captured as a high-level architecture and
  operating posture view for interview and planning use
- Phase 11 is complete: deterministic dashboard filters are URL-backed, local
  to public dashboard JSON, export-aware, and covered by desktop/mobile
  screenshot evidence

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

Status: complete.

Working checklist: `docs/phase-10-dashboard-implementation.md`

Completed scope:

- implemented the `Overview` page from `docs/dashboard-ia-spec.md`
- added the global filter bar, executive KPIs, P&L drivers, risk coverage,
  exception table, market context strip, and data-quality state
- kept the dashboard driven by approved public snapshot JSON only
- made stale, limited, or missing evidence visible rather than hidden
- aligned README, demo walkthrough, LinkedIn copy, and diagrams with the
  UI and proof evidence

Definition of done:

- React build passes
- contract validation passes
- desktop, tablet, and mobile screenshots are captured
- docs describe Phase 8 and Phase 9 as completed foundations
- the demo story connects lakehouse data, AI orchestration controls, and the
  visible dashboard decision surface

### Phase 11: Deterministic Dashboard Filter Wiring

Goal: turn the Phase 10 filter controls from display/readout controls into
deterministic local interactions that update dashboard views without changing
the approved public data boundary.

Status: complete and merged. Phase 11 delivered URL-backed filters, filtered
portfolio surfaces, market-series date slicing, export metadata, desktop and
mobile screenshot evidence, and documentation updates.

Working checklist: `docs/phase-11-dashboard-filters.md`

Focus:

- wire date range, book, segment, and risk filters into React state
- keep filters encoded in the URL query string for shareable views
- apply the selected filters consistently to KPIs, P&L drivers, risk panels,
  exception rows, market context, and snapshot export metadata
- keep all filtering local to `dashboard-data.json` and
  `dashboard_snapshot_v1.sample.json`
- preserve mobile readability and screenshot evidence

Definition of done:

- React build passes
- contract validation passes
- filter state is deterministic and shareable through the URL
- exported snapshot reflects selected filters
- desktop and mobile visual evidence is refreshed
- docs and demo walkthrough explain the filter behavior clearly

## Suggested Immediate Next Steps

1. Start the next implementation boundary from clean `main`.
2. Keep the next slice narrow enough to prove with build, contract validation,
   docs, and visual evidence.
3. Defer hosting, alarms, or managed AI runtime changes unless the new phase
   explicitly targets that operating boundary.

## Next Branch Preflight Checklist

Use this before opening the next implementation branch:

- Confirm the current branch is clean and synchronized:

  ```bash
  git switch main
  git pull --ff-only origin main
  git status --short --branch
  ```

- Confirm the previous phase is closed:
  - PR merged
  - local feature branch deleted
  - remote feature branch deleted
  - docs reflect completed state

- Define the next state boundary in one sentence:

  ```text
  From: <current clean state>
  To: <smallest useful proven state>
  Proof: <build/check/demo evidence>
  Failure path: <how to pause without leaving repo half-changed>
  ```

- Choose the branch name only after the boundary is clear:

  ```bash
  git switch -c feature/name-of-work
  git status --short --branch
  ```

- Run the baseline proof before editing:

  ```bash
  npm --prefix dashboard-ui run build
  .venv/bin/python scripts/validate_contracts.py \
    --include-evidence \
    --check-failures
  ```

- Stop and update docs first if the planned slice depends on architecture or
  operating assumptions that are not yet reflected in `README.md`, `PLANS.md`,
  or the relevant phase checklist.

## Planning Rule

If implementation reality and design ambition diverge, update this file in the following order:

1. `README.md` reflects current intended platform truth.
2. `PLANS.md` reflects delivery sequence from that truth.
3. diagrams and React pages reflect what is actually implemented now versus what is still future work.
