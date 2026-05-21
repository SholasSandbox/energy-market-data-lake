# PLANS.md

<!-- markdownlint-disable MD013 -->

Source of truth for this plan:

- `README.md` for current platform scope and target model
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
- `docs/phase-12-dashboard-hosting-foundation.md` for the optional
  CloudFront/static dashboard delivery foundation
- `docs/phase-13-dashboard-hosting-publish-runbook.md` for the dashboard
  hosting publish and evidence runbook proof
- `docs/phase-14-dashboard-hosting-live-apply-evidence.md` for the live
  dashboard hosting preflight, proof commands, and rollback path

Historical planning references:

- `docs/archive/four-week-project-plan.md` preserves the original 4-week MVP
  plan. It is archived and no longer a source of truth for current phase
  status.

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
- Terraform now tracks the core lakehouse and Phase 8 resources, with ingestion
  Lambda drift reconciled
- Phase 10 is complete: the React dashboard now has a stronger
  operator-facing `Overview` surface, refreshed demo/docs, responsive
  screenshot evidence, and current architecture diagrams
- the target operating model is now captured as a high-level architecture and
  operating posture view for interview and planning use
- Phase 11 is complete: deterministic dashboard filters are URL-backed, local
  to public dashboard JSON, export-aware, and covered by desktop/mobile
  screenshot evidence
- Phase 12 is complete: Terraform now has an opt-in private S3 plus
  CloudFront foundation for public-safe dashboard delivery
- Phase 13 is complete: dashboard static publish commands and evidence
  capture are scripted in plan-only mode before live hosting writes
- Phase 14 live hosting is complete: ingestion Lambda drift was reconciled,
  CloudFront/S3 hosting was applied, dashboard assets were published, and
  CloudFront HTTP checks passed

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

### Phase 12: Dashboard Hosting Foundation

Goal: add optional Terraform support for CloudFront-fronted dashboard delivery
while keeping the dashboard bucket private and leaving live hosting disabled by
default.

Status: complete. First slice adds CloudFront Origin Access Control,
distribution, S3 bucket policy, security headers, outputs, variables, and
enablement docs.

Working checklist: `docs/phase-12-dashboard-hosting-foundation.md`

Focus:

- keep the approved public dashboard JSON boundary intact
- use private S3 plus CloudFront Origin Access Control
- leave CloudFront disabled until a deliberate live apply decision
- avoid DNS, ACM, alarms, schedules, and managed AI changes in this slice

Definition of done:

- Terraform formatting and validation pass
- dashboard build still passes
- contract validation still passes
- docs explain enablement, proof commands, and out-of-scope items

### Phase 13: Dashboard Hosting Publish Runbook Proof

Goal: turn the Phase 12 hosting foundation into a repeatable operator publish
path for React build artifacts, S3 sync commands, CloudFront invalidation, and
evidence capture.

Status: complete. First slice adds a plan-only publish script and runbook so
the command path can be verified without writing to AWS.

Working checklist: `docs/phase-13-dashboard-hosting-publish-runbook.md`

Focus:

- build and validate dashboard assets before publish
- render the exact S3 sync and CloudFront invalidation commands
- write Markdown evidence for the publish attempt
- require explicit `--apply` before any AWS write commands execute
- keep DNS, ACM, alarms, schedules, and managed AI out of scope

Definition of done:

- shell syntax check passes
- plan-only publish evidence is generated
- React build, contract validation, Terraform validation, Markdown lint, and
  whitespace checks pass

### Phase 14: Dashboard Hosting Live Apply Evidence

Goal: capture controlled evidence for the live dashboard hosting apply path
without broadening into DNS, ACM, alarms, schedules, or managed AI.

Status: plan reviewed. The Phase 14A/14B plans are not safe to apply because
they include an unrelated in-place update to `aws_lambda_function.ingest`.

Working checklist:
`docs/phase-14-dashboard-hosting-live-apply-evidence.md`

Focus:

- review local tfvars and current Terraform state
- enable CloudFront only for the plan/apply boundary
- save and review Terraform plan evidence before apply
- apply only if no unrelated replacements, destroys, schedule changes, or IAM
  broadening appear
- publish dashboard assets only after CloudFront outputs are available

Definition of done:

- Terraform plan evidence is captured and reviewed
- live apply decision is explicit
- if applied, S3, CloudFront, HTTP, and rollback evidence are captured

Phase 14A decision:

- plan evidence:
  `docs/evidence/phase14-dashboard-hosting-plan-20260519T202521Z.txt`
- result: `Plan: 4 to add, 1 to change, 0 to destroy`
- decision: do not apply until Lambda drift is isolated or neutralized

Phase 14B drift isolation:

- no-apply evidence:
  `docs/evidence/phase14b-dashboard-hosting-refreshfalse-plan-20260520.txt`
- Terraform state and live AWS agree on deployed Lambda code hash
  `LpuQEhsU45t3ne5cbEvumah4ljmMPwo8FaxzhW30Z/Y=`
- local Terraform package hash is
  `O+87gZ8+OMKKUwvzsXhA2sCVrAbDOwymkLU7MYS/Goc=`
- `-refresh=false` still proposes the Lambda update, so this is
  configuration/state reconciliation rather than refresh-only noise
- decision: do not apply; reconcile the ingestion Lambda in a separate slice
  before any normal root dashboard hosting apply

Phase 14C Lambda-only reconciliation decision:

- no-apply evidence:
  `docs/evidence/phase14c-root-lambda-reconcile-plan-20260520.txt`
- sanitized live Lambda config evidence:
  `docs/evidence/phase14c-ingest-lambda-current-config-sanitized-20260520.json`
- live Lambda tag evidence:
  `docs/evidence/phase14c-ingest-lambda-current-tags-20260520.json`
- normal root plan with CloudFront disabled shows only
  `aws_lambda_function.ingest`: `Plan: 0 to add, 1 to change, 0 to destroy`
- extracted deployed source and local source have the same SHA-256 hash, so the
  Lambda drift appears to be package metadata/state/tag reconciliation rather
  than source-code drift
- decision: do not apply during the decision slice; next safe state is a
  controlled Lambda-only reconciliation apply with rollback package captured
  locally before any mutation

Phase 14D Lambda-only reconciliation apply:

- apply evidence:
  `docs/evidence/phase14d-lambda-reconcile-apply-20260521.txt`
- post-apply Lambda config evidence:
  `docs/evidence/phase14d-ingest-lambda-post-apply-config-sanitized-20260521.json`
- post-apply Lambda tag evidence:
  `docs/evidence/phase14d-ingest-lambda-post-apply-tags-20260521.json`
- smoke evidence:
  `docs/evidence/phase14d-ingest-lambda-smoke-response-20260521.json`
- post-apply root plan:
  `docs/evidence/phase14d-post-apply-nochange-plan-20260521.txt`
- result: `Apply complete! Resources: 0 added, 1 changed, 0 destroyed.`
- Lambda smoke invoke returned `StatusCode` 200, handler status `ok`, and no
  warnings
- post-apply Terraform plan reports no changes
- decision: Lambda drift is reconciled; dashboard hosting can move back to a
  fresh apply-candidate plan review

Phase 14E dashboard-hosting apply-candidate review:

- plan evidence:
  `docs/evidence/phase14e-dashboard-hosting-apply-candidate-plan-20260521.txt`
- pre-apply output evidence:
  `docs/evidence/phase14e-dashboard-hosting-preapply-outputs-20260521.json`
- result: `Plan: 4 to add, 0 to change, 0 to destroy`
- expected additions are CloudFront distribution, OAC, response headers policy,
  and dashboard S3 bucket policy
- no Lambda, Step Functions, EventBridge schedule, Glue/Athena, replacement, or
  destroy drift appears in the apply-candidate plan
- decision: apply-candidate plan is clean; do not apply until explicit approval

Phase 14F dashboard-hosting live apply:

- apply evidence:
  `docs/evidence/phase14f-dashboard-hosting-apply-20260521.txt`
- post-apply outputs:
  `docs/evidence/phase14f-dashboard-hosting-post-apply-outputs-20260521.json`
- CloudFront distribution evidence:
  `docs/evidence/phase14f-cloudfront-distribution-20260521.json`
- publish evidence:
  `docs/evidence/phase14f-dashboard-hosting-publish-20260521.md`
- HTTP header evidence:
  `docs/evidence/phase14f-cloudfront-http-headers-20260521.txt`
- result: `Apply complete! Resources: 4 added, 0 changed, 0 destroyed.`
- CloudFront distribution: `E2H9BGRGYAHKPN`
- CloudFront domain: `d28yo76if4k3l1.cloudfront.net`
- HTTP checks for `index.html`, `dashboard-data.json`, and
  `dashboard_snapshot_v1.sample.json` returned `200 OK`
- post-apply Terraform plan reports no changes
- publish script was hardened to preserve `dashboard_snapshot_v1.json` and
  `snapshots/*` on future static-site publishes

### Phase 15: CloudFront-Hosted Dashboard Demo Hardening

Goal: make the hosted dashboard demo easy to verify and explain without
changing infrastructure or repopulating the live AI snapshot.

Status: complete as a docs/runbook hardening slice.

Updated demo guide:
`docs/demo-walkthrough.md`

Evidence:

- CloudFront demo HTTP check:
  `docs/evidence/phase15-cloudfront-demo-http-check-20260521.txt`

Focus:

- put the CloudFront dashboard URL into the demo path
- provide quick hosted verification commands
- keep the local dashboard path as a fallback
- explain the private lakehouse versus public static dashboard boundary
- document that live AI `dashboard_snapshot_v1.json` restore is deferred

Definition of done:

- hosted dashboard verification is documented
- demo script explains why CloudFront/S3 hosting is now live
- known follow-up for live AI snapshot restore is explicit
- no Terraform apply, DNS, ACM, alarms, schedules, or managed AI changes

## Suggested Immediate Next Steps

1. Decide whether to repopulate the live AI `dashboard_snapshot_v1.json` using
   a Phase 8 publish rerun or controlled snapshot restore.
2. Keep DNS, ACM, alarms, schedules, and managed AI invocation deferred until a
   phase explicitly targets those operating boundaries.
3. Keep the hosted dashboard demo path reproducible from
   `docs/demo-walkthrough.md`.

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
