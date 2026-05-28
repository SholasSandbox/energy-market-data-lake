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

## Suggested Immediate Next Steps

1. Review and merge Phase 17N preflight decision evidence.
2. Decide whether to approve Phase 17N execution as one controlled sixth live
   Mistral invocation.
3. Keep DNS, ACM, alarms, schedules, repeated live invocation, and Terraform apply
   deferred until a phase explicitly targets those operating boundaries.
4. Keep the hosted dashboard demo path reproducible from
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
