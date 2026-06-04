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

### Phase 16: Live AI Dashboard Snapshot Restore

Goal: restore the public-safe live AI dashboard snapshot path that was removed
by the first static-site publish, without changing infrastructure or replacing
the working CloudFront-hosted React demo.

Status: complete as a controlled snapshot restore.

Decision:

- use the successful Phase 8 curated artifacts for run
  `ai-insight-20260511T114815Z-927685a3`
- rebuild `dashboard_snapshot_v1.json` through the existing repo snapshot
  builder and schema validator
- restore only the latest snapshot key and the matching immutable run-id key
- do not rerun managed AI, do not run Terraform apply, and do not change DNS,
  ACM, alarms, schedules, or managed AI invocation

Evidence:

- restored snapshot payload:
  `docs/evidence/phase16-dashboard-snapshot-v1-restored-20260522.json`
- S3 object proof:
  `docs/evidence/phase16-dashboard-snapshot-latest-head-20260522.json`
  and
  `docs/evidence/phase16-dashboard-snapshot-immutable-head-20260522.json`
- CloudFront invalidation proof:
  `docs/evidence/phase16-cloudfront-snapshot-invalidation-status-20260522.json`
- hosted HTTP/JSON proof:
  `docs/evidence/phase16-cloudfront-snapshot-http-json-check-20260522.txt`

Result:

- `https://d28yo76if4k3l1.cloudfront.net/dashboard_snapshot_v1.json`
  returns `200 OK` and parses as `dashboard_snapshot_v1`
- immutable snapshot path
  `/snapshots/run_id=ai-insight-20260511T114815Z-927685a3/dashboard_snapshot_v1.json`
  returns `200 OK`
- existing hosted dashboard routes continue to return `200 OK`
- static-site publish hardening still preserves `dashboard_snapshot_v1.json`
  and `snapshots/*`

### Phase 17: Managed AI Refresh Path Preflight

Goal: decide how the project should move from deterministic AI insight
generation to a managed AI refresh path without weakening the Phase 8
validation and public publish boundary.

Status: planning/preflight complete.

Working document:
`docs/phase-17-managed-ai-refresh-preflight.md`

Decision:

- prefer a Bedrock-first managed AI refresh path
- keep deterministic merge as fallback and comparison path
- keep OpenClaw/ECS runtime deferred until the managed model boundary is proven
- keep EventBridge schedules disabled
- do not change DNS, ACM, alarms, budgets, or dashboard hosting in this phase

Evidence:

- read-only preflight:
  `docs/evidence/phase17-managed-ai-refresh-preflight-readonly-20260522.md`

Next implementation slice:

- Phase 17A: add a Bedrock adapter and `MergeAiInsightManaged` handler path
  behind local tests and fake-client validation
- no Terraform apply, live model invocation, schedule enablement, DNS, ACM, or
  alarm changes

### Phase 17A: Bedrock Adapter Behind Local Tests

Goal: prove the managed AI provider boundary in code before adding IAM,
Terraform, live Bedrock invocation, or schedule automation.

Status: implementation complete as a code-only local proof.

Completed scope:

- added `energy_market/managed_ai.py` with Bedrock Runtime request and response
  parsing helpers
- added `MergeAiInsightManaged` beside `MergeAiInsightDeterministic`
- kept deterministic merge as fallback
- validated managed output against `ai_insight_v1`
- added fake-client proof through
  `scripts/check_phase17a_managed_ai_adapter.py`

Guardrails kept:

- no Terraform apply
- no IAM or state-machine deployment change
- no live Bedrock invocation
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes

Next implementation slice:

- Phase 17B: controlled live Bedrock invocation plan/apply preflight, with
  explicit token budget, model choice, IAM delta review, and rollback path
- OpenClaw/local model comparison remains a later cost-control and creativity
  slice after the AWS-managed boundary is proven

### Phase 17B: Controlled Live Bedrock Invocation Preflight

Goal: decide whether the first live Bedrock invocation is safe, cheap, and
controlled.

Status: preflight complete; **do not invoke yet**.

Working document:
`docs/phase-17b-controlled-bedrock-invocation-preflight.md`

Evidence:
`docs/evidence/phase17b-bedrock-preflight-readonly-20260523.md`

Decision:

- Claude 3 Haiku matches the current Anthropic-compatible adapter, but its
  Bedrock agreement availability is `NOT_AVAILABLE` in the read-only check
- Mistral Ministral 8B is available in `eu-west-2` and has a lower London
  pricing profile, but the current adapter needs provider-specific Mistral
  request/response support before a live call
- Phase 17B remains preflight-only
- no Terraform apply, IAM change, live model invocation, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes

Next implementation slice:

- Phase 17C: choose either the Anthropic access path or the Mistral
  compatibility path
- recommendation: implement Mistral request/response support behind fake-client
  tests first, then run one controlled Mistral live invocation in a later
  explicitly approved boundary
- keep OpenClaw/local model comparison as a later cost-control and creativity
  slice after one AWS-managed live invocation is proven

### Phase 17C: Mistral Compatibility Proof

Goal: prove the lower-cost Mistral path locally before any live model
invocation, IAM change, Terraform apply, or state-machine deployment.

Status: implementation complete as a code-only local proof.

Completed scope:

- added provider-aware Bedrock request construction in
  `energy_market/managed_ai.py`
- kept the Anthropic-compatible request path intact
- added Mistral chat-completion request support for
  `mistral.ministral-3-8b-instruct`
- added Mistral `choices[].message.content` response parsing
- expanded the fake-client proof in
  `scripts/check_phase17a_managed_ai_adapter.py`
- preserved deterministic fallback through `MergeAiInsightDeterministic`

Evidence:
`docs/evidence/phase17c-mistral-compatibility-proof-20260523.md`

Guardrails kept:

- no live Bedrock invocation
- no Terraform apply
- no IAM change
- no Step Functions/state-machine deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes

Next implementation slice:

- Phase 17D: one controlled live Mistral invocation, only after explicit
  approval, with a hard `$0.10` one-run budget cap and no retries unless
  approved

### Phase 17D: One Controlled Live Mistral Invocation

Goal: perform one live Bedrock Runtime invocation against Mistral Ministral 8B
under the Phase 17B cost and safety boundary.

Status: complete; live invocation attempted once and rejected by validation.

Evidence:

- summary:
  `docs/evidence/phase17d-mistral-live-invocation-summary-20260523.md`
- sanitized metadata:
  `docs/evidence/phase17d-mistral-live-invocation-metadata-20260523.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- output did not validate as `ai_insight_v1`
- the public dashboard snapshot was not changed
- the model output was not published
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes

Next implementation slice:

- Phase 17E: local prompt/response-shape hardening for Mistral
- prove the corrected Mistral output contract with fake-client tests before
  any second live invocation
- keep the failed Phase 17D invocation as evidence that schema gates protected
  the public dashboard

### Phase 17E: Local Mistral Prompt/Response-Shape Hardening

Goal: harden the Mistral output contract locally before any second paid model
call.

Status: implementation complete as a code-only local proof.

Evidence:
`docs/evidence/phase17e-mistral-response-shape-hardening-20260523.md`

Completed scope:

- tightened the managed AI prompt so the JSON root must be the
  `ai_insight_v1` object itself
- explicitly instructs Mistral not to wrap the payload in `ai_insight`,
  `result`, `output`, `response`, `data`, or any other key
- added narrow parser support for the observed one-key `ai_insight` wrapper
  from Phase 17D
- left unsafe or broader wrapper objects to fail the existing `ai_insight_v1`
  validation gate
- expanded the local fake-client proof to cover direct Mistral output, the
  observed wrapper shape, and an unsafe wrapper failure case

Guardrails kept:

- no live Bedrock invocation
- no Terraform apply
- no IAM change
- no Step Functions/state-machine deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes
- no raw Phase 17D model response was committed

Next implementation slice:

- Phase 17F: one controlled second live Mistral invocation may be considered
  only after explicit approval, using the same hard one-run budget cap and no
  retries unless separately approved

### Phase 17F: One Controlled Second Live Mistral Invocation

Goal: run one controlled second live Mistral invocation after Phase 17E local
hardening and decide whether the result is schema-valid and safe for a later
publish or deployment boundary.

Status: complete; live invocation attempted once and stopped before validation.

Evidence:

- summary:
  `docs/evidence/phase17f-mistral-second-live-invocation-summary-20260524.md`
- sanitized metadata:
  `docs/evidence/phase17f-mistral-second-live-invocation-metadata-20260524.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- estimated invocation cost: `$0.00135217`
- output did not parse as complete JSON
- sanitized response shape shows `finish_reason` was `length`
- the output started with a markdown fence and did not end as a complete JSON
  object
- no validated `ai_insight_v1` evidence was produced
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes

Red-green evidence:

- Red: live Mistral output must not be accepted unless it parses and validates
  as `ai_insight_v1`
- Green: the second live output failed safely and was not published
- Regression: local adapter proof and deterministic fallback remain intact

Next implementation slice:

- Phase 17G: local Mistral JSON-completion hardening before any third live call
- decide locally whether prompt wording or `max_tokens` should change
- prove fenced/incomplete JSON handling with fake-client tests
- keep any further live Mistral invocation behind explicit approval

### Phase 17G: Local Mistral JSON-Completion Hardening

Goal: harden the local managed-AI prompt, parser, and output-token budget after
Phase 17F returned incomplete fenced JSON.

Status: implementation complete as a local-only proof.

Evidence:
`docs/evidence/phase17g-mistral-json-completion-hardening-20260524.md`

Completed scope:

- tightened the prompt to require complete JSON, discourage markdown fences,
  and shorten prose rather than truncate JSON
- raised the managed AI default output-token cap from `800` to `1600`
- aligned the Lambda managed path so `BEDROCK_MAX_TOKENS` defaults to the
  shared managed-AI constant
- added sanitized parser errors for incomplete markdown fences and truncated
  JSON
- expanded fake-client proof for complete fenced JSON, incomplete fenced JSON,
  truncated JSON, exact wrapper handling, unsafe wrapper rejection, and
  managed-handler success/failure paths

Guardrails kept:

- no live Bedrock invocation
- no Terraform apply
- no IAM change
- no Step Functions/state-machine deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes
- no raw model response was committed

Next implementation slice:

- Phase 17H: one controlled third live Mistral invocation may be considered
  only after explicit approval
- use the raised `1600` output-token cap
- keep one-call discipline, no retry unless separately approved, no dashboard
  publish, and sanitized evidence only

### Phase 17H: One Controlled Third Live Mistral Invocation

Goal: test the Phase 17G local hardening with one controlled live Mistral
invocation using the raised `1600` output-token cap.

Status: complete; live invocation attempted once and rejected by validation.

Evidence:

- summary:
  `docs/evidence/phase17h-mistral-third-live-invocation-summary-20260524.md`
- sanitized metadata:
  `docs/evidence/phase17h-mistral-third-live-invocation-metadata-20260524.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- estimated invocation cost: `$0.00126615`
- raised `1600` output-token cap prevented the Phase 17F truncation failure
- sanitized response shape shows `finish_reason` was `stop`
- output still failed `ai_insight_v1` validation because the model returned a
  root wrapper key named `ai_insight_v1`
- no validated `ai_insight_v1` evidence was produced
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes

Red-green evidence:

- Red: the third live call must not accept managed output unless it validates
  as `ai_insight_v1`
- Green: the token-budget failure was fixed, but schema validation still
  rejected the wrapper shape safely
- Regression: local adapter proof and deterministic fallback remain intact

Next implementation slice:

- Phase 17I: local Mistral root-wrapper hardening before any fourth live call
- decide whether to accept the exact `ai_insight_v1` wrapper shape locally
- keep broad wrapper unwrapping rejected
- add fake-client coverage before any further live invocation

### Phase 17I: Local Mistral Root-Wrapper Hardening

Goal: harden the local managed-AI parser for the exact root-wrapper shape
observed in Phase 17H while keeping broad wrapper handling rejected.

Status: implementation complete as a local-only proof.

Evidence:
`docs/evidence/phase17i-mistral-root-wrapper-hardening-20260524.md`

Completed scope:

- added narrow parser support for a single-key `ai_insight_v1` wrapper
- accepts the wrapper only when the nested object declares
  `schema_version: ai_insight_v1`
- keeps broad `ai_insight_v1` wrappers with sibling keys rejected by schema
  validation
- updated prompt wording to explicitly reject both `ai_insight_v1` and
  `ai_insight` wrapper keys
- expanded fake-client proof for direct and Mistral response wrapper shapes

Guardrails kept:

- no live Bedrock invocation
- no Terraform apply
- no IAM change
- no Step Functions/state-machine deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes
- no raw model response was committed

Next implementation slice:

- Phase 17J: one controlled fourth live Mistral invocation may be considered
  only after explicit approval
- keep one-call discipline, no retry unless separately approved, no dashboard
  publish, and sanitized evidence only

### Phase 17J: Live Mistral Invocation Preflight Decision

Goal: decide whether a fourth controlled live Mistral invocation is justified
after Phase 17I local root-wrapper hardening.

Status: preflight complete; **do not invoke until explicitly approved**.

Evidence:
`docs/evidence/phase17j-live-mistral-preflight-decision-20260526.md`

Decision:

- recommendation: **GO candidate, pending explicit approval**
- Phase 17I addresses the exact Phase 17H failure locally
- local adapter proof passed
- Bedrock model lookup confirmed `mistral.ministral-3-8b-instruct` in
  `eu-west-2`
- estimated one-call cost is approximately `$0.001294210`
- hard budget cap remains `$0.10`

Guardrails retained:

- no live Bedrock invocation in this preflight state
- no Terraform apply
- no IAM change
- no Step Functions/state-machine deployment
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes
- no dashboard publish

Next implementation slice:

- Phase 17J execution may perform one controlled fourth live Mistral invocation
  only after explicit approval
- keep one-call discipline, no retry unless separately approved, no dashboard
  publish, and sanitized evidence only

### Phase 17J: One Controlled Fourth Live Mistral Invocation

Goal: run the explicitly approved Phase 17J execution substate and test the
Phase 17I root-wrapper hardening against one live Mistral invocation.

Status: complete; live invocation attempted once and rejected by validation.

Evidence:

- summary:
  `docs/evidence/phase17j-mistral-fourth-live-invocation-summary-20260526.md`
- sanitized metadata:
  `docs/evidence/phase17j-mistral-fourth-live-invocation-metadata-20260526.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- estimated invocation cost: `$0.00127788`
- Phase 17I root-wrapper normalization worked live: root schema version parsed
  as `ai_insight_v1`
- output still failed `ai_insight_v1` validation because the nested insight
  object missed required fields and used a `references` substitute
- no validated `ai_insight_v1` evidence was produced
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes

Red-green evidence:

- Red: Phase 17H failed on exact root wrapper shape
- Green: Phase 17J live output parsed through that wrapper to
  `schema_version: ai_insight_v1`
- Regression: schema validation still rejected nested insight fields, so
  nothing was published

Next implementation slice:

- Phase 17K: local Mistral schema-field hardening before any fifth live call
- make required insight fields harder to omit
- require `validation_notes` as an array
- reject `references` as a substitute for separate energy/news references
- keep any further live invocation behind explicit approval

### Phase 17K: Local Mistral Schema-Field Hardening

Goal: harden the nested insight-field contract locally after Phase 17J proved
root-wrapper normalization worked live, but `ai_insight_v1` validation still
rejected the nested `insights[0]` object.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17k-mistral-schema-field-hardening-20260526.md`

Completed scope:

- no live Bedrock invocation was made
- no Terraform apply, IAM change, state-machine deploy, EventBridge schedule
  enablement, DNS, ACM, alarms, budgets, dashboard hosting change, or dashboard
  publish was performed
- prompt now explicitly lists required fields for each insight
- prompt rejects generic `references` as a substitute for
  `energy_references` and `news_references`
- prompt requires `validation_notes` as an array of strings
- local fake-client proof reproduces the Phase 17J nested-field failure shape
  and confirms schema validation still rejects unsafe output
- deterministic fallback remains unchanged

Red-green evidence:

- Red: Phase 17J parsed to `schema_version: ai_insight_v1`, but nested insight
  validation rejected missing required fields, generic `references`, and a
  string `validation_notes` value.
- Green: Phase 17K tightens the prompt contract and proves the Phase 17J
  failure shape is still rejected locally before any further live call.
- Regression: root-wrapper normalization, broad-wrapper rejection, fenced JSON
  handling, and deterministic fallback remain covered by the local proof.

Next implementation slice:

- Phase 17L should begin as a preflight decision before any fifth live Mistral
  invocation.
- Any further live invocation remains explicit-approval only, one call only, no
  retry, no dashboard publish, and sanitized evidence only.

### Phase 17L: Live Mistral Preflight Decision

Goal: decide whether a fifth controlled live Mistral invocation is justified
after Phase 17K local schema-field hardening.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17l-live-mistral-preflight-decision-20260526.md`

Decision:

- recommendation is **go-candidate**, not automatic execution
- a fifth live call remains blocked until explicit approval in a separate
  execution substate
- if approved, the next call must remain one invocation only, no retry, hard
  `$0.10` cap, sanitized metadata only, no dashboard publish, and no Terraform
  or AWS infrastructure changes

Preflight facts:

- four controlled live Mistral calls have been made so far
- cumulative estimated live Mistral cost is `$0.00516320`
- every prior live call used no retry and performed no dashboard publish
- Phase 17K locally targets the exact Phase 17J nested schema-field failure

Red-green evidence:

- Red: Phase 17J failed nested `ai_insight_v1` validation after root-wrapper
  normalization worked live.
- Green: Phase 17K locally tightened the prompt contract and kept the observed
  unsafe shape rejected.
- Regression: deterministic fallback, root-wrapper normalization, broad-wrapper
  rejection, and dashboard publish blocking remain intact.

Next implementation slice:

- Phase 17L execution may perform one controlled fifth live Mistral invocation
  only after explicit approval.
- If approval is not granted, keep the next slice local-only.

### Phase 17L: One Controlled Fifth Live Mistral Invocation

Goal: run the explicitly approved Phase 17L execution substate and test the
Phase 17K schema-field hardening against one live Mistral invocation.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17l-mistral-fifth-live-invocation-summary-20260527.md`
- `docs/evidence/phase17l-mistral-fifth-live-invocation-metadata-20260527.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- estimated invocation cost: `$0.00134251`
- parsed payload had `schema_version: ai_insight_v1`
- Phase 17K improved the live output shape: the output produced one insight
  and no longer failed on the broad missing-field pattern from Phase 17J
- output still failed `ai_insight_v1` validation because nested object fields
  were shaped incorrectly
- no validated `ai_insight_v1` evidence was produced
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes

Red-green evidence:

- Red: Phase 17J failed on missing nested insight fields, generic
  `references`, and string `validation_notes`.
- Green: Phase 17L live output parsed as `ai_insight_v1` and advanced to
  narrower nested object-shape validation failures.
- Regression: schema validation still rejected invalid output, so nothing was
  published.

Next implementation slice:

- Phase 17M: local Mistral object-shape hardening before any sixth live call
- require `time_window` as an object with `start` and `end`
- forbid extra fields such as `value` in `energy_references`
- keep any further live invocation behind explicit approval

### Phase 17M: Local Mistral Object-Shape Hardening

Goal: harden the nested object-shape contract locally after Phase 17L execution
showed Mistral output could parse as `ai_insight_v1`, but still failed schema
validation on exact object shapes.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17m-mistral-object-shape-hardening-20260528.md`

Completed scope:

- no live Bedrock invocation was made
- no Terraform apply, IAM change, state-machine deploy, EventBridge schedule
  enablement, DNS, ACM, alarms, budgets, dashboard hosting change, or dashboard
  publish was performed
- prompt now requires `time_window` as an object with `start` and `end`
  date-time strings
- prompt explicitly rejects string `time_window`
- prompt forbids extra fields such as `value`, `date`, and `timestamp` in
  reference objects
- local fake-client proof reproduces the Phase 17L object-shape failure and
  confirms schema validation still rejects unsafe output
- deterministic fallback remains unchanged

Red-green evidence:

- Red: Phase 17L parsed as `ai_insight_v1`, but validation rejected string
  `time_window` and extra `value` fields in `energy_references`.
- Green: Phase 17M tightens the prompt contract and locally proves the Phase
  17L object-shape failure remains rejected before any further live call.
- Regression: previous Phase 17 failure shapes, wrapper normalization,
  broad-wrapper rejection, fenced JSON handling, deterministic fallback, and
  dashboard publish blocking remain covered.

Next implementation slice:

- Phase 17N should begin as a preflight decision before any sixth live Mistral
  invocation.
- Any further live invocation remains explicit-approval only, one call only, no
  retry, no dashboard publish, and sanitized evidence only.

### Phase 17N: Live Mistral Preflight Decision

Goal: decide whether a sixth controlled live Mistral invocation is justified
after Phase 17M local object-shape hardening.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17n-live-mistral-preflight-decision-20260528.md`

Decision:

- recommendation is **go-candidate**, not automatic execution
- a sixth live call remains blocked until explicit approval in a separate
  execution substate
- if approved, the next call must remain one invocation only, no retry, hard
  `$0.10` cap, sanitized metadata only, no dashboard publish, and no Terraform
  or AWS infrastructure changes

Preflight facts:

- five controlled live Mistral calls have been made so far
- cumulative estimated live Mistral cost is `$0.00650571`
- every prior live call used no retry and performed no dashboard publish
- Phase 17M locally targets the exact Phase 17L nested object-shape failure

Red-green evidence:

- Red: Phase 17L failed nested `ai_insight_v1` object-shape validation after
  schema-field hardening improved the live output.
- Green: Phase 17M locally tightened object-shape instructions and kept the
  observed unsafe shape rejected.
- Regression: deterministic fallback, wrapper handling, schema validation, and
  dashboard publish blocking remain intact.

Next implementation slice:

- Phase 17N execution may perform one controlled sixth live Mistral invocation
  only after explicit approval.
- If approval is not granted, keep the next slice local-only.

### Phase 17N: One Controlled Sixth Live Mistral Invocation

Goal: run the explicitly approved Phase 17N execution substate and test the
Phase 17M object-shape hardening against one live Mistral invocation.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17n-mistral-sixth-live-invocation-summary-20260528.md`
- `docs/evidence/phase17n-mistral-sixth-live-invocation-metadata-20260528.json`

Result:

- one `bedrock-runtime invoke-model` call was made against
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- hard budget cap: `$0.10`
- estimated invocation cost: `$0.00136229`
- parsed payload had `schema_version: ai_insight_v1`
- output passed `ai_insight_v1` validation in memory
- no validated payload was committed because the approved boundary was
  sanitized metadata only
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deployment, EventBridge
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
- total estimated live Mistral cost across all controlled calls so far is
  `$0.00786800`

Red-green evidence:

- Red: Phase 17L failed on nested object shape after schema-field hardening
  improved the live output.
- Green: Phase 17N live output parsed and validated as `ai_insight_v1`.
- Regression: deterministic fallback remains intact and nothing was published.

Next implementation slice:

- Phase 17O: managed AI publish/deployment preflight before any dashboard update
  or handler/state-machine switch
- decide whether to capture a public-safe validated payload in a future
  controlled run
- keep dashboard publish, Terraform, IAM, schedules, DNS, ACM, alarms, and
  budgets unchanged unless a future phase explicitly targets them

### Phase 17O: Managed AI Publish/Deployment Preflight

Goal: decide whether Phase 17N validation success is sufficient to publish
managed AI output or deploy managed handler/state-machine wiring.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17o-managed-ai-publish-deployment-preflight-20260528.md`

Decision:

- **no-go** for immediate dashboard publish
- **no-go** for immediate handler/state-machine deployment
- next safest boundary is public-safe validated payload capture, not dashboard
  mutation or workflow switching

Preflight facts:

- Phase 17N produced schema-valid `ai_insight_v1` in memory
- Phase 17N did not commit the parsed payload because the approved boundary was
  sanitized metadata only
- public dashboard snapshot remains unchanged
- current Terraform Step Functions definition still uses
  `MergeAiInsightDeterministic`
- `MergeAiInsightManaged` exists in code, but production deployment would need
  IAM, environment, state-machine, rollback, and failure-path proof

Recommended next boundary:

- Phase 17P: managed AI validated payload capture
- one controlled managed invocation only after explicit approval
- commit only a public-safe parsed `ai_insight_v1` artifact if it validates
- do not publish the dashboard and do not deploy handler/state-machine changes
- keep raw prompt and raw model response uncommitted
- preserve deterministic fallback

Future decisions must stay separate:

- validated payload capture
- dashboard snapshot publish
- Step Functions managed-handler deployment

### Phase 17P: Managed AI Validated Payload Capture

Goal: capture a public-safe validated `ai_insight_v1` payload as evidence before
any dashboard publish or handler/state-machine switch.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17p-managed-ai-validated-payload-capture-summary-20260528.md`
- `docs/evidence/phase17p-managed-ai-validated-payload-capture-metadata-20260528.json`
- `docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00135608`
- parsed payload had `schema_version: ai_insight_v1`
- parsed payload passed `ai_insight_v1` validation
- public-safe validated payload was committed as evidence
- one private lake S3 reference from the model output was replaced with a
  public-safe curated dataset reference before committing
- raw prompt and raw model response were not committed
- public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Red-green evidence:

- Red: Phase 17N proved in-memory validation but did not commit the payload.
- Green: Phase 17P captures a schema-valid, public-safe managed AI payload as
  evidence.
- Regression: deterministic fallback remains intact and nothing was published.

Next implementation slice:

- Phase 17Q: managed AI dashboard publish preflight
- decide whether the validated evidence payload should be converted into a
  dashboard snapshot
- review rollback for the current live dashboard snapshot
- keep handler/state-machine deployment separate from dashboard publish

### Phase 17Q: Managed AI Dashboard Publish Preflight

Goal: decide whether the Phase 17P validated managed AI payload can safely move
toward the public `dashboard_snapshot_v1.json` path.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17q-managed-ai-dashboard-publish-preflight-20260529.md`
- `docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json`
- `docs/evidence/phase17q-current-live-dashboard-snapshot-http-check-20260529.txt`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, dashboard hosting change, S3 write, CloudFront
  invalidation, or public dashboard publish was performed
- Phase 17P `ai_insight_v1` evidence was converted locally into a candidate
  `dashboard_snapshot_v1` evidence file
- the candidate validates against `schemas/dashboard_snapshot_v1.schema.json`
- the current live CloudFront `dashboard_snapshot_v1.json` remains healthy
- the preflight found a publish-quality issue: the React dashboard renders
  every insight source as an anchor, and the managed energy reference currently
  becomes a non-URL `href`

Red-green evidence:

- Red: Phase 17P captured a valid managed AI payload but did not prove public
  dashboard publish readiness.
- Green: Phase 17Q proves the payload can produce a schema-valid candidate
  dashboard snapshot locally.
- Regression: live dashboard remains unchanged, deterministic fallback remains
  intact, and managed workflow deployment remains blocked.

Next implementation slice:

- Phase 17R: local managed AI dashboard source-link hardening
- keep Bedrock invocation and dashboard publish out of scope
- normalize managed energy references into dashboard-safe labels and links
- preserve external news URLs and reject or neutralize private/non-public
  source references before any publish boundary

### Phase 17R: Local Managed AI Dashboard Source-Link Hardening

Goal: remove the Phase 17Q source-link blocker locally before any managed AI
dashboard publish decision.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17r-managed-ai-dashboard-source-link-hardening-20260529.md`
- `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`
- `scripts/check_phase17r_dashboard_source_links.py`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, dashboard hosting change, S3 write, CloudFront
  invalidation, or public dashboard publish was performed
- dashboard source-link generation now preserves public `http` and `https`
  news URLs
- private, custom-scheme, or plain-text managed energy references now use the
  public dashboard fallback `dashboard-data.json`
- the original managed energy reference context is retained in the source label
- the Phase 17R candidate validates against
  `schemas/dashboard_snapshot_v1.schema.json`

Red-green evidence:

- Red: Phase 17Q produced a valid candidate, but one managed energy source
  rendered as a non-URL anchor target.
- Green: Phase 17R neutralizes non-public source links locally and produces a
  valid candidate with a public dashboard source target.
- Regression: local managed AI adapter proof passes, deterministic fallback
  remains intact, and dashboard publish remains blocked.

Next implementation slice:

- Phase 17S: managed AI dashboard publish decision
- require explicit approval before any S3 write or CloudFront invalidation
- keep Bedrock invocation, Terraform, IAM, schedules, DNS, ACM, alarms, budgets,
  and managed workflow deployment out of scope

### Phase 17S: Managed AI Dashboard Publish Decision

Goal: decide whether the Phase 17R managed AI dashboard candidate is ready for
a controlled public dashboard publish execution boundary.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17s-managed-ai-dashboard-publish-decision-20260529.md`
- `docs/evidence/phase17s-current-live-dashboard-snapshot-http-check-20260529.txt`
- `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, dashboard hosting change, S3 write, CloudFront
  invalidation, or public dashboard publish was performed
- current live CloudFront snapshot still matches the Phase 16 rollback payload
  by SHA256
- Phase 17R source-link proof remains green
- Phase 17R candidate remains the intended publish payload
- decision is go-candidate for publish execution, but execution still requires
  explicit approval

Red-green evidence:

- Red: Phase 17Q blocked publish because managed source links were not public
  dashboard ready.
- Green: Phase 17R removed that blocker, and Phase 17S confirms the candidate
  and rollback baseline.
- Regression: live dashboard remains unchanged, deterministic fallback remains
  intact, and managed workflow deployment remains blocked.

Next implementation slice:

- Phase 17S execution substate: managed AI dashboard publish
- publish latest plus immutable snapshot only after explicit approval
- invalidate only `/dashboard_snapshot_v1.json` and the immutable snapshot path
- keep Bedrock invocation, Terraform, IAM, schedules, DNS, ACM, alarms, budgets,
  static-site rebuild, and managed workflow deployment out of scope

### Phase 17S Execution: Managed AI Dashboard Publish

Goal: publish the approved Phase 17R managed AI dashboard candidate to the live
CloudFront-backed dashboard snapshot path.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17s-managed-ai-dashboard-publish-execution-summary-20260529.md`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-http-check-20260529.txt`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-invalidation-status-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-latest-head-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-immutable-head-20260529.json`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, static-site rebuild, or managed workflow
  deployment was performed
- approved Phase 17R candidate was uploaded as latest
  `dashboard_snapshot_v1.json`
- the same payload was uploaded as immutable snapshot
  `snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`
- CloudFront invalidation `I9MCXBX6M0BCO1HN0BWCKZO5H9` completed
- CloudFront serves both latest and immutable paths with SHA256
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`

Red-green evidence:

- Red: live dashboard still served the Phase 16 deterministic snapshot before
  publish.
- Green: live dashboard now serves the approved managed AI dashboard snapshot.
- Regression: local managed AI adapter proof and Phase 17R source-link proof
  remain green; managed workflow deployment remains blocked.

Next implementation slice:

- Phase 17T: managed AI dashboard post-publish demo verification
- keep the next slice read-only
- verify the hosted dashboard experience and update demo notes
- keep Bedrock invocation, Terraform, IAM, schedules, DNS, ACM, alarms, budgets,
  and managed workflow deployment out of scope

### Phase 17T: Managed AI Dashboard Post-Publish Demo Verification

Goal: verify the hosted dashboard demo after Phase 17S published the managed AI
dashboard snapshot.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17t-managed-ai-dashboard-demo-verification-20260529.md`
- `docs/evidence/phase17t-managed-ai-dashboard-demo-http-check-20260529.txt`
- `docs/evidence/phase17t-managed-ai-dashboard-demo-json-check-20260529.txt`

Result:

- read-only verification only
- no Bedrock invocation was run
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, S3 write, CloudFront invalidation, static-site
  rebuild, or managed workflow deployment was performed
- CloudFront returned `200` for `/`, `/index.html`, `dashboard-data.json`,
  latest `dashboard_snapshot_v1.json`, and the immutable managed AI snapshot
  path
- latest and immutable snapshot paths match the approved Phase 17R candidate
  SHA256
- both snapshot paths validate against `dashboard_snapshot_v1`
- source-link hardening is visible in the live snapshot

Red-green evidence:

- Red: Phase 17S execution published the managed AI snapshot, but still needed
  a read-only hosted-demo verification pass.
- Green: Phase 17T confirms the hosted dashboard and snapshot paths serve the
  expected managed AI payload.
- Regression: no dashboard mutation occurred in Phase 17T; managed workflow
  deployment remains blocked.

Next implementation slice:

- Phase 17U: managed workflow deployment preflight
- keep the next slice preflight-only unless explicitly approved
- review IAM, Lambda environment, Step Functions routing, rollback, and
  failure-path controls before any managed handler/state-machine deployment

### Phase 17U: Managed Workflow Deployment Preflight

Goal: decide whether the managed AI handler can be deployed into the Step
Functions workflow after the managed AI dashboard snapshot has been published
and read-only verified.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17u-managed-workflow-deployment-preflight-20260529.md`

Result:

- preflight-only review
- no Bedrock invocation was run
- no Terraform apply, IAM change, Lambda deploy, state-machine deploy, schedule
  enablement, DNS, ACM, alarms, budgets, S3 write, CloudFront invalidation, or
  dashboard publish was performed
- `MergeAiInsightManaged` exists in the Lambda handler and remains covered by
  the local fake-client proof
- Terraform still sets `AI_ORCHESTRATION_MODE = "deterministic"` and routes
  the Step Functions workflow through `MergeAiInsightDeterministic`
- the managed workflow deployment remains a no-go until the IAM, Lambda
  environment, Step Functions routing, rollback, and failure-path delta is
  reviewed as a plan-only slice

Red-green evidence:

- Red: Phase 17S and Phase 17T proved the managed AI snapshot can be published
  and demo-verified, but the deployed workflow still uses deterministic merge.
- Green: Phase 17U identifies the exact deployment gap without mutating AWS.
- Regression: local managed AI adapter proof, source-link proof, Terraform
  validation, and deterministic fallback remain green.

Next implementation slice:

- Phase 17V: managed workflow Terraform/IAM delta preflight
- keep the next slice plan-only unless explicitly approved
- model the least-privilege Bedrock permission, managed mode variables,
  state-machine routing change, rollback path, and schedule-disabled posture
  before any deployment

### Phase 17V: Managed Workflow Terraform/IAM Delta Preflight

Goal: model the managed AI workflow deployment delta in Terraform without
applying it.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17v-managed-workflow-terraform-iam-delta-preflight-20260529.md`
- `docs/evidence/phase17v-managed-workflow-terraform-plan-refreshfalse-20260529.txt`
- `docs/evidence/phase17v-managed-workflow-terraform-plan-isolated-refreshfalse-20260529.txt`
- `docs/evidence/phase17v-deterministic-rollback-terraform-plan-refreshfalse-20260529.txt`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, Lambda deploy, Step Functions deploy,
  schedule enablement, DNS, ACM, alarms, budgets, S3 write, CloudFront
  invalidation, dashboard publish, or live workflow execution was performed
- added opt-in Terraform variables for managed AI workflow routing and Bedrock
  invocation settings
- added an opt-in, least-privilege `bedrock:InvokeModel` policy scoped to the
  configured Bedrock foundation model ARN
- made Lambda environment variables switch between deterministic and managed
  mode based on `ai_orchestration_managed_ai_enabled`
- made Step Functions route to `MergeAiInsightManaged` only when the managed
  toggle is enabled
- schedule enablement remains controlled by
  `ai_orchestration_schedule_enabled`, which stays false in the example
  configuration

Plan evidence:

- the first local `-refresh=false` plan surfaced unrelated CloudFront destroys
  because local tfvars did not preserve `dashboard_cloudfront_enabled = true`
- the isolated managed plan explicitly preserved CloudFront and showed
  `Plan: 1 to add, 4 to change, 0 to destroy`
- the deterministic rollback/default plan with CloudFront preserved showed
  `No changes`

Red-green evidence:

- Red: Phase 17U found that managed workflow deployment needed Terraform/IAM,
  Lambda environment, Step Functions routing, rollback, and failure-path review.
- Green: Phase 17V models that delta plan-only and proves the isolated managed
  plan has no destroys.
- Regression: deterministic rollback plan is no-op when managed mode is
  disabled, local managed AI adapter proof remains green, and dashboard publish
  is unchanged.

Next implementation slice:

- Phase 17W: managed workflow deployment decision
- review the Phase 17V plans before any apply
- require explicit approval before any Terraform apply, IAM mutation, Lambda
  deploy, Step Functions deploy, managed workflow execution, or schedule
  enablement

### Phase 17W: Managed Workflow Deployment Decision

Goal: decide whether the Phase 17V Terraform/IAM delta is safe to move into a
separate managed workflow deployment execution boundary.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17w-managed-workflow-deployment-decision-20260529.md`

Decision:

- managed workflow deployment is a **go-candidate**, not an automatic apply
- execution remains blocked until explicit approval in a separate substate
- any execution must preserve `dashboard_cloudfront_enabled = true`
- any execution must keep `ai_orchestration_schedule_enabled = false`
- the unsafe local Phase 17V plan must not be applied
- the apply candidate is the isolated managed plan shape only:
  `Plan: 1 to add, 4 to change, 0 to destroy`

Result:

- no Bedrock invocation was run
- no Terraform apply, IAM change, Lambda deploy, Step Functions deploy,
  schedule enablement, DNS, ACM, alarms, budgets, S3 write, CloudFront
  invalidation, dashboard publish, or live workflow execution was performed
- Phase 17V isolated plan evidence was reviewed
- deterministic rollback/default plan evidence was reviewed
- deployment and workflow execution remain separate from schedule enablement

Red-green evidence:

- Red: Phase 17V modeled an apply candidate, but one local plan showed
  unrelated CloudFront destroys when the hosting toggle was not preserved.
- Green: Phase 17W narrows the decision to the isolated no-destroy plan and
  keeps execution approval separate.
- Regression: deterministic rollback remains no-op, dashboard publish remains
  unchanged, and schedules remain disabled.

Next implementation slice:

- Phase 17W execution substate may run one controlled Terraform apply only
  after explicit approval
- execution must use the isolated no-destroy apply shape
- keep managed workflow execution and EventBridge schedule enablement out of
  the apply boundary unless explicitly approved later

### Phase 17W: Controlled Managed Workflow Terraform Apply

Goal: apply the approved Phase 17W managed workflow Terraform delta while
preserving CloudFront hosting and keeping schedules disabled.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17w-managed-workflow-terraform-apply-summary-20260529.md`
- `docs/evidence/phase17w-managed-workflow-terraform-apply-plan-20260529.txt`
- `docs/evidence/phase17w-managed-workflow-terraform-apply-20260529.txt`
- `docs/evidence/phase17w-managed-workflow-postapply-plan-refreshfalse-20260529.txt`
- `docs/evidence/phase17w-managed-workflow-lambda-config-20260529.json`
- `docs/evidence/phase17w-managed-workflow-state-machine-20260529.json`
- `docs/evidence/phase17w-managed-workflow-schedule-state-20260529.json`
- `docs/evidence/phase17w-managed-workflow-bedrock-policy-20260529.json`
- `docs/evidence/phase17w-managed-workflow-cloudfront-status-20260529.json`
- `docs/evidence/phase17w-managed-workflow-cloudfront-snapshot-http-check-20260529.txt`

Result:

- explicit approval was granted for the controlled apply substate
- saved Terraform plan used the isolated no-destroy apply shape:
  `Plan: 1 to add, 4 to change, 0 to destroy`
- Terraform apply completed with `Resources: 1 added, 2 changed, 0 destroyed`
- added the AI orchestration Lambda Bedrock `InvokeModel` policy
- updated the AI orchestration Lambda environment to managed mode
- updated the Step Functions definition to route through
  `MergeAiInsightManaged`
- EventBridge schedule remains `DISABLED`
- CloudFront distribution `E2H9BGRGYAHKPN` remains deployed
- live dashboard snapshot still returns `200`
- no Bedrock invocation, live workflow execution, schedule enablement,
  dashboard publish, DNS, ACM, alarms, budgets, or CloudFront invalidation was
  performed

Red-green evidence:

- Red: managed workflow deployment was only a go-candidate before this apply.
- Green: Terraform applied the managed workflow routing delta with no destroys
  and kept schedules disabled.
- Regression: post-apply plan is no-op with managed mode and CloudFront
  preserved; hosted dashboard snapshot remains reachable.

Next implementation slice:

- Phase 17X: managed workflow smoke decision
- do not execute the managed workflow until an explicit decision reviews cost,
  failure path, dashboard impact, and rollback
- keep EventBridge schedule enablement blocked

### Phase 17X: Managed Workflow Smoke Decision

Goal: decide whether the managed Step Functions workflow is ready for one
controlled manual smoke execution after Phase 17W deployed managed routing.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17x-managed-workflow-smoke-decision-20260530.md`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-lambda-config-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-state-machine-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-schedule-state-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-recent-executions-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-dashboard-http-check-20260530.txt`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-latest-snapshot-head-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-immutable-snapshot-head-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-terraform-nochange-20260530.txt`

Decision:

- managed workflow smoke execution is a **go-candidate**, not automatic
- execution remains blocked until explicit approval in a separate substate
- the smoke execution is publish-capable because the deployed state machine
  ends at `PublishDashboardSnapshot`
- a successful smoke may overwrite live `dashboard_snapshot_v1.json`
- any execution must therefore capture rollback evidence before starting and
  stop after one manual execution
- EventBridge schedule enablement remains blocked

Result:

- no Bedrock invocation was run
- no Step Functions execution was started
- no Terraform apply, IAM change, Lambda deploy, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, S3 write, CloudFront
  invalidation, or dashboard publish was performed
- Lambda remains active in managed mode
- Step Functions remains active and routes from `CreateAiInputBundle` to
  `MergeAiInsightManaged`, then to `PublishDashboardSnapshot`
- EventBridge schedule remains `DISABLED`
- current live dashboard snapshot still returns `200`
- Terraform remains no-op only when managed mode, CloudFront preservation, and
  schedule-disabled variables are passed together

Red-green evidence:

- Red: Phase 17W deployed managed workflow routing, but no live workflow smoke
  had reviewed publish impact or rollback.
- Green: Phase 17X identifies the smoke execution as a controlled
  publish-capable boundary and records the rollback prerequisites.
- Regression: no workflow execution occurred, schedules remain disabled, and
  the current dashboard snapshot remains reachable.

Next implementation slice:

- Phase 17Y: controlled managed workflow smoke execution
- run only after explicit approval
- one manual Step Functions execution maximum, no manual retry
- capture generated run ID, execution history, S3 artifacts, dashboard impact,
  rollback path, estimated Bedrock cost, and post-run schedule-disabled proof

### Phase 17Y: Controlled Managed Workflow Smoke Execution

Goal: run one manual managed workflow smoke execution after Phase 17X approved
the smoke as a publish-capable go-candidate.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17y-managed-workflow-smoke-execution-summary-20260530.md`
- `docs/evidence/phase17y-managed-workflow-smoke-start-execution-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-describe-execution-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-execution-history-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-failure-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-root-cause-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-s3-artifacts-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-dashboard-impact-summary-20260530.txt`
- `docs/evidence/phase17y-managed-workflow-smoke-post-schedule-state-20260530.json`

Result:

- one manual Step Functions execution was started
- manual retries: `0`
- execution status: `FAILED`
- generated run ID: `ai-insight-20260530T205944Z-df1fdb6a`
- failure state: `MergeAiInsightManaged`
- sanitized failure reason: deployed Lambda handler did not recognize
  `MergeAiInsightManaged`
- Bedrock was not invoked because the Lambda failed before the managed action
  handler could call Bedrock
- estimated Bedrock cost: `$0.00`
- `energy_input`, `news_summary`, and `ai_input_bundle` artifacts were written
- no `ai_insight` artifact, run-scoped dashboard snapshot, or latest dashboard
  update was written
- latest dashboard snapshot version, ETag, and SHA256 did not change
- EventBridge schedule remains `DISABLED`
- no Terraform apply, schedule enablement, DNS, ACM, alarms, budgets,
  CloudFront invalidation, or dashboard publish was performed

Red-green evidence:

- Red: Phase 17W deployed managed workflow routing, but no manual managed
  workflow smoke had run.
- Green: Phase 17Y proved the deployed state machine reaches the managed merge
  state and fails safely before Bedrock when the live Lambda package lacks the
  managed action handler.
- Regression: no retry was run, no Bedrock cost was incurred, no dashboard
  publish occurred, and schedule remains disabled.

Next implementation slice:

- Phase 17Z: Lambda package refresh preflight
- rebuild and deploy the AI orchestration Lambda package containing
  `MergeAiInsightManaged` before any second managed workflow smoke
- keep schedules disabled
- do not retry the managed workflow until package refresh evidence is reviewed

### Phase 17Z: Lambda Package Refresh Preflight

Goal: prove the Lambda package refresh boundary after Phase 17Y showed that the
deployed workflow reaches `MergeAiInsightManaged`, but the live Lambda package
does not contain that handler action.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17z-lambda-package-refresh-preflight-20260601.md`
- `docs/evidence/phase17z-lambda-package-local-before-build-20260601.txt`
- `docs/evidence/phase17z-lambda-package-local-after-build-20260601.txt`
- `docs/evidence/phase17z-current-lambda-config-sanitized-20260601.json`
- `docs/evidence/phase17z-current-schedule-state-20260601.json`
- `docs/evidence/phase17z-lambda-package-refresh-terraform-plan-refreshfalse-20260601.txt`
- `docs/evidence/phase17z-lambda-package-refresh-targeted-terraform-plan-refreshfalse-20260601.txt`

Result:

- no Step Functions execution, Bedrock invocation, Terraform apply, Lambda
  deploy, IAM mutation, schedule enablement, S3 write, CloudFront invalidation,
  or dashboard publish was performed
- the deployed Lambda `CodeSha256` still matches the stale local Terraform
  package hash captured before rebuild
- that stale package did not contain `MergeAiInsightManaged`
- the repo source already contains `MergeAiInsightManaged`
- the rebuilt package contains `MergeAiInsightManaged` and
  `energy_market/managed_ai.py`
- the root refresh-false Terraform plan shows
  `Plan: 0 to add, 2 to change, 0 to destroy`
- the root plan would update the Lambda code hash and re-render the Step
  Functions IAM role policy in place
- a targeted comparison plan shows
  `Plan: 0 to add, 1 to change, 0 to destroy`
- EventBridge schedule remains `DISABLED`

Red-green evidence:

- Red: Phase 17Y failed at `MergeAiInsightManaged` because the deployed Lambda
  package was stale.
- Green: Phase 17Z rebuilt the local Terraform package and proved the package
  now contains the managed action handler and managed AI module.
- Regression: no retry, Bedrock invocation, dashboard publish, Terraform
  apply, or schedule enablement occurred.

Next implementation slice:

- Phase 17Z execution substate: controlled Lambda package refresh apply
- require explicit approval before apply
- choose deliberately between the normal root plan and the targeted Lambda-only
  plan
- keep schedules disabled
- do not run Step Functions or Bedrock during the package refresh
- run a second managed workflow smoke only after the deployed Lambda package is
  refreshed and verified

### Phase 17Z: Controlled Lambda Package Refresh Apply

Goal: refresh the deployed AI orchestration Lambda package so it contains the
managed action handler proven locally in Phase 17Z preflight.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17z-lambda-package-refresh-apply-summary-20260602.md`
- `docs/evidence/phase17z-execution-lambda-package-after-rebuild-20260602.txt`
- `docs/evidence/phase17z-execution-terraform-apply-plan-20260602.txt`
- `docs/evidence/phase17z-execution-terraform-apply-20260602.txt`
- `docs/evidence/phase17z-execution-postapply-lambda-config-20260602.json`
- `docs/evidence/phase17z-execution-postapply-schedule-state-20260602.json`
- `docs/evidence/phase17z-execution-postapply-dashboard-http-check-20260602.txt`
- `docs/evidence/phase17z-execution-postapply-terraform-nochange-20260602.txt`
- `docs/evidence/phase17z-execution-recent-executions-20260602.json`

Result:

- normal root saved plan was used after explicit approval
- saved plan showed `Plan: 0 to add, 2 to change, 0 to destroy`
- apply completed with `Resources: 0 added, 1 changed, 0 destroyed`
- changed resource was `aws_lambda_function.ai_orchestration[0]`
- deployed Lambda `CodeSha256` now matches the rebuilt package hash
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- the rebuilt package contains `MergeAiInsightManaged`
- no Step Functions execution or Bedrock invocation was run
- no dashboard publish, S3 write, CloudFront invalidation, schedule enablement,
  DNS, ACM, alarms, or budgets change was performed
- EventBridge schedule remains `DISABLED`
- live dashboard snapshot SHA-256 remains
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- post-apply Terraform plan reports `No changes`

Red-green evidence:

- Red: Phase 17Y failed at `MergeAiInsightManaged` because the deployed Lambda
  package was stale.
- Green: Phase 17Z execution refreshed the deployed Lambda package and the live
  code hash now matches the package that contains `MergeAiInsightManaged`.
- Regression: no managed workflow retry, Bedrock invocation, dashboard publish,
  or schedule enablement occurred.

Next implementation slice:

- Phase 17AA: managed workflow second-smoke decision
- decide whether one controlled second managed workflow smoke is justified now
  that the Lambda package is refreshed
- treat the smoke as publish-capable and explicit-approval only
- capture rollback evidence before any execution
- keep schedules disabled

### Phase 17AA: Managed Workflow Second-Smoke Decision

Goal: decide whether one controlled second managed workflow smoke is justified
after Phase 17Z execution refreshed the deployed Lambda package.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17aa-managed-workflow-second-smoke-decision-20260602.md`
- `docs/evidence/phase17aa-second-smoke-decision-lambda-config-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-state-machine-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-recent-executions-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-schedule-state-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-dashboard-http-check-20260602.txt`
- `docs/evidence/phase17aa-second-smoke-decision-latest-snapshot-head-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-immutable-snapshot-head-20260602.json`
- `docs/evidence/phase17aa-second-smoke-decision-terraform-nochange-20260602.txt`

Decision:

- one controlled second managed workflow smoke is a go-candidate, not automatic
  execution
- execution remains blocked until explicit approval in a separate substate
- if approved, use one manual Step Functions execution maximum and no manual
  retry
- capture rollback snapshot metadata before execution
- capture execution ARN, history, output, generated run ID, S3 artifacts,
  dashboard impact, estimated Bedrock cost, and post-run schedule-disabled
  proof

Read-only facts:

- deployed Lambda `CodeSha256` is
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- deployed Lambda remains in managed mode
- Step Functions routes `CreateAiInputBundle` to `MergeAiInsightManaged`, then
  to `PublishDashboardSnapshot`
- recent execution evidence shows no new managed workflow run after Phase 17Y
- EventBridge schedule remains `DISABLED`
- latest dashboard snapshot still returns `200`
- latest dashboard snapshot version remains `qYxpit3hmGzpSByvhG07nrOG4kBrz1qn`
- dashboard snapshot SHA-256 remains
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741`
- Terraform reports `No changes`

Red-green evidence:

- Red: Phase 17Y reached `MergeAiInsightManaged` but failed because the
  deployed Lambda package was stale.
- Green: Phase 17Z execution refreshed the package, and Phase 17AA confirms
  the live Lambda code hash matches the refreshed package.
- Regression: no workflow retry, Bedrock invocation, dashboard publish,
  Terraform apply, or schedule enablement occurred.

Next implementation slice:

- Phase 17AA execution substate: one controlled second managed workflow smoke
- run only after explicit approval
- keep the smoke publish-capable, rollback-first, one-run only, and no manual
  retry
- keep schedules disabled

### Phase 17AA: Controlled Managed Workflow Second-Smoke Execution

Goal: run one controlled managed workflow smoke after the Phase 17Z Lambda
package refresh and confirm the managed Step Functions path can complete.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17aa-managed-workflow-second-smoke-execution-summary-20260603.md`
- `docs/evidence/phase17aa-second-smoke-start-execution-20260603.json`
- `docs/evidence/phase17aa-second-smoke-describe-execution-20260603.json`
- `docs/evidence/phase17aa-second-smoke-execution-history-20260603.json`
- `docs/evidence/phase17aa-second-smoke-output-summary-20260603.json`
- `docs/evidence/phase17aa-second-smoke-s3-artifacts-20260603.json`
- `docs/evidence/phase17aa-second-smoke-schema-validation-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-dashboard-impact-summary-20260603.txt`
- `docs/evidence/phase17aa-second-smoke-post-schedule-state-20260603.json`
- `docs/evidence/phase17aa-second-smoke-post-terraform-nochange-20260603.txt`

Result:

- one manual Step Functions execution was started
- execution status: `SUCCEEDED`
- generated run ID: `ai-insight-20260603T010744Z-4d89a62a`
- workflow status: `dashboard_snapshot_published`
- manual retries: `0`
- redrive count: `0`
- Bedrock was invoked through the managed Mistral path
- estimated direct model cost is `$0.00132618`
- generated `ai_insight_v1` validates
- generated `dashboard_snapshot_v1` validates
- latest dashboard snapshot version changed from
  `qYxpit3hmGzpSByvhG07nrOG4kBrz1qn` to
  `b9PUPbupwFRcRCIHTcMwFhylWsuDCkSv`
- latest CloudFront snapshot SHA-256 changed from
  `d180b4a2bda131fae6088a650301f40b696ba67929ffcea78be42731adb3a741` to
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`
- latest and immutable CloudFront snapshot paths returned `200`
- EventBridge schedule remains `DISABLED`
- post-run Terraform reports `No changes`
- no CloudFront invalidation was requested
- raw AI payloads and raw model output were not committed

Red-green evidence:

- Red: Phase 17Y reached `MergeAiInsightManaged` but failed because the
  deployed Lambda package was stale.
- Green: Phase 17AA execution proved the refreshed deployed Lambda package can
  run the managed Bedrock/Mistral merge and publish a valid dashboard snapshot
  through the managed workflow.
- Regression: one execution only, no manual retry, schedules remain disabled,
  Terraform remains no-change, and generated artifacts validate.

Next implementation slice:

- Phase 17AB: managed workflow post-smoke demo verification
- keep the next slice read-only
- verify hosted dashboard behavior after the workflow-published snapshot
- keep schedule enablement as a later explicit decision boundary

### Phase 17AB: Managed Workflow Post-Smoke Demo Verification

Goal: verify the hosted dashboard and workflow-published snapshot after the
successful Phase 17AA managed workflow smoke, without mutating AWS or starting
another workflow execution.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17ab-managed-workflow-post-smoke-demo-verification-20260604.md`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-demo-http-check-20260604.txt`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-demo-json-check-20260604.txt`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-schedule-state-20260604.json`
- `docs/evidence/phase17ab-managed-workflow-post-smoke-recent-executions-20260604.json`

Result:

- no Bedrock invocation, Step Functions execution, Terraform apply, IAM
  mutation, Lambda deploy, Step Functions deploy, schedule enablement, S3
  write, CloudFront invalidation, static-site rebuild, or dashboard publish
  was performed
- CloudFront returns `200` for `/`, `/index.html`, `/dashboard-data.json`,
  latest `dashboard_snapshot_v1.json`, and the Phase 17AA immutable snapshot
- latest and immutable snapshot paths match SHA-256
  `d4806fbbd0a2045ad1bc79c511601ad5f342ebe8a12fe276448cec1b6fb1d515`
- both snapshot paths validate against `dashboard_snapshot_v1`
- latest and immutable snapshot payloads match each other
- EventBridge schedule remains `DISABLED`
- recent execution evidence still shows the Phase 17AA execution as the latest
  Step Functions run
- Phase 17AB found one demo/public-surface hardening gap: the source URL is
  dashboard-safe as `dashboard-data.json`, but the source label still carries
  private lake S3 context from the managed workflow snapshot

Red-green evidence:

- Red: Phase 17AA proved the managed workflow can publish the dashboard
  snapshot, but the hosted demo still needed read-only proof after cache
  propagation.
- Green: Phase 17AB confirms hosted routes, latest workflow snapshot,
  immutable workflow snapshot, schedule-disabled posture, and schema
  validation are healthy.
- Regression: no workflow execution or dashboard mutation occurred, and the
  public source URL fallback remains safe.

Next implementation slice:

- Phase 17AC: managed workflow source-label sanitization
- keep the next slice local/preflight first
- sanitize managed workflow source labels before any schedule enablement,
  repeated managed workflow run, or dashboard publish

### Phase 17AC: Managed Workflow Source-Label Sanitization

Goal: harden managed workflow dashboard source-label generation locally after
Phase 17AB found private lake S3 context in a public source label.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17ac-managed-workflow-source-label-sanitization-20260604.md`
- `docs/evidence/phase17ac-managed-workflow-source-label-sanitization-candidate-20260604.json`
- `scripts/check_phase17ac_source_label_sanitization.py`

Result:

- no Bedrock invocation, Step Functions execution, Terraform apply, IAM
  mutation, Lambda deploy, Step Functions deploy, schedule enablement, S3
  write, CloudFront invalidation, static-site rebuild, or dashboard publish
  was performed
- private S3, ARN, local file, AWS account, Amazon-hosted, and curated lake
  references are treated as non-public label context
- private managed workflow source labels now collapse to
  `curated dashboard evidence`
- partition date context such as `date=2026-05-07` is preserved as
  `curated dashboard evidence for 2026-05-07`
- existing public curated labels remain unchanged
- private/non-public source URLs still use the Phase 17R
  `dashboard-data.json` fallback
- public news URLs remain preserved
- the Phase 17AC candidate validates against `dashboard_snapshot_v1`

Red-green evidence:

- Red: Phase 17AB proved the hosted workflow-published snapshot was healthy but
  found source-label public-surface drift.
- Green: Phase 17AC proves locally that private lake references no longer
  appear in dashboard source labels, while useful date context and the safe
  dashboard source URL remain.
- Regression: Phase 17R source-link hardening, managed AI adapter proof, and
  contract validation remain green.

Next implementation slice:

- Phase 17AD: managed workflow source-label publish/deployment decision
- keep the next slice decision/preflight-only unless explicitly approved
- decide whether to deploy the sanitizer into the managed workflow Lambda
  package and whether any controlled workflow smoke or dashboard publish is
  justified

### Phase 17AD: Managed Workflow Source-Label Publish/Deployment Decision

Goal: decide whether the Phase 17AC source-label sanitizer is ready for
dashboard publication or managed workflow Lambda deployment.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17ad-managed-workflow-source-label-publish-deployment-decision-20260604.md`
- `docs/evidence/phase17ad-current-lambda-config-sanitized-20260604.json`
- `docs/evidence/phase17ad-current-lambda-package-status-20260604.txt`
- `docs/evidence/phase17ad-current-schedule-state-20260604.json`
- `docs/evidence/phase17ad-current-recent-executions-20260604.json`
- `docs/evidence/phase17ad-local-source-label-sanitization-check-20260604.txt`
- `docs/evidence/phase17ad-current-terraform-plan-refreshfalse-20260604.txt`

Decision:

- no-go for immediate dashboard publish
- no-go for immediate managed workflow smoke or schedule enablement
- no-go for immediate Terraform apply from the current root plan

Reasons:

- Phase 17AC proves the sanitizer locally, but no deployed Lambda package
  contains the sanitizer yet
- current deployed Lambda and current local Terraform zip still match package
  hash `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- current local Terraform zip contains `MergeAiInsightManaged` but does not
  contain `source_label_context`
- recent executions still show the Phase 17AA smoke as the latest Step
  Functions run
- EventBridge schedule remains `DISABLED`
- current root `-refresh=false` plan is unsafe:
  `Plan: 0 to add, 4 to change, 5 to destroy`, including unrelated CloudFront
  destroys and Bedrock IAM policy removal

Red-green evidence:

- Red: Phase 17AB found source-label public-surface drift in the
  workflow-published snapshot.
- Green: Phase 17AD prevents a local-only code fix from being treated as
  deployed and blocks an unsafe root plan from becoming an apply path.
- Regression: no workflow execution, dashboard mutation, schedule enablement,
  or Terraform apply occurred.

Next implementation slice:

- Phase 17AE: Lambda package refresh preflight for source-label sanitizer
- rebuild the AI orchestration Lambda package locally
- prove the rebuilt package contains `source_label_context`
- capture a safe no-apply Terraform plan that preserves CloudFront hosting and
  managed Bedrock IAM settings
- keep apply, workflow smoke, schedule enablement, S3 writes, CloudFront
  invalidation, and dashboard publish out of scope

### Phase 17AE: Lambda Package Refresh Preflight For Source-Label Sanitizer

Goal: prove the Lambda package refresh boundary for the Phase 17AC
source-label sanitizer before any deployment, workflow execution, or dashboard
publish.

Status: complete and ready for review.

Evidence:

- `docs/evidence/phase17ae-lambda-package-refresh-preflight-20260604.md`
- `docs/evidence/phase17ae-current-lambda-config-sanitized-20260604.json`
- `docs/evidence/phase17ae-current-schedule-state-20260604.json`
- `docs/evidence/phase17ae-current-recent-executions-20260604.json`
- `docs/evidence/phase17ae-lambda-package-before-rebuild-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-rebuild-command-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-rebuild-output-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-after-rebuild-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-refresh-root-plan-preserve-refreshfalse-20260604.txt`
- `docs/evidence/phase17ae-lambda-package-refresh-targeted-plan-refreshfalse-20260604.txt`

Result:

- no Bedrock invocation, Step Functions execution, Terraform apply, IAM
  mutation, Lambda deploy, Step Functions deploy, schedule enablement, S3
  write, CloudFront invalidation, static-site rebuild, or dashboard publish
  was performed
- deployed Lambda remains active, managed, and on package hash
  `Eeeg+InzSBuAUcrQPN9glMbw3hSWLBPkspiH0Ly2puE=`
- pre-rebuild local package matched the deployed Lambda hash and did not
  contain `source_label_context`
- rebuilt local package contains `source_label_context`,
  `PRIVATE_REFERENCE_DATE_RE`, `MergeAiInsightManaged`, and
  `energy_market/managed_ai.py`
- rebuilt local package hash is
  `V/PZH22YFXzyYarXT+dglN/JJ0CasL0G1zFqbVFk1Zc=`
- EventBridge schedule remains `DISABLED`
- recent executions still show the Phase 17AA smoke as the latest Step
  Functions run
- preserved-variable root no-apply plan shows
  `Plan: 0 to add, 2 to change, 0 to destroy`
- root plan updates the AI orchestration Lambda package in place and re-renders
  the Step Functions IAM policy in place
- no CloudFront destroy, dashboard bucket-policy destroy, Bedrock IAM policy
  destroy, or schedule enablement appears in the preserved root plan
- targeted comparison plan shows
  `Plan: 0 to add, 1 to change, 0 to destroy` with Terraform's expected
  `-target` warning

Decision:

- Lambda package refresh is a go-candidate, not automatic execution
- execution remains blocked until explicit approval in a separate substate
- preferred execution shape is the normal root saved plan with CloudFront and
  managed Bedrock IAM preservation variables, not the targeted plan unless a
  future blocker justifies targeted recovery

Next implementation slice:

- Phase 17AE execution substate: controlled Lambda package refresh apply
- require explicit approval before apply
- do not run Step Functions, invoke Bedrock, enable schedules, write dashboard
  S3 objects, invalidate CloudFront, or publish a new dashboard snapshot

## Suggested Immediate Next Steps

1. Phase 17AA controlled managed workflow second-smoke execution evidence is
   merged.
2. Review and merge Phase 17AB read-only post-smoke demo verification.
3. Review and merge Phase 17AC managed workflow source-label sanitization.
4. Review and merge Phase 17AD source-label publish/deployment decision.
5. Review and merge Phase 17AE Lambda package refresh preflight.
6. If approved later, run Phase 17AE execution as a controlled Lambda package
   refresh apply only; keep workflow smoke, schedule enablement, and dashboard
   publish separate.
7. Keep DNS, ACM, alarms, schedules, repeated live invocation, Terraform apply,
   and schedule enablement decisions deferred until a phase explicitly targets
   those operating boundaries.
8. Keep the hosted dashboard demo path reproducible from
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
