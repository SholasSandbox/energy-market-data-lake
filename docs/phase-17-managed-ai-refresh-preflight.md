# Phase 17: Managed AI Refresh Path Preflight

## Goal

Define the safest path to move from deterministic AI insight generation to a
managed AI refresh step while preserving the proven Phase 8 orchestration
contract, Phase 16 live snapshot restore, and CloudFront-hosted dashboard
health.

This is a planning and preflight state only. It does not invoke a managed
model, run Terraform apply, enable schedules, change DNS/ACM, add alarms, or
publish a new dashboard snapshot.

## Current State

- Phase 8 Step Functions orchestration exists and can be run manually.
- The current AI insight step is `MergeAiInsightDeterministic`.
- The public dashboard snapshot is published only after schema validation.
- Phase 16 restored the live `dashboard_snapshot_v1.json` and immutable
  snapshot object for run `ai-insight-20260511T114815Z-927685a3`.
- EventBridge scheduling remains disabled.
- CloudFront serves the static dashboard and the restored snapshot path.

Read-only preflight evidence:
`docs/evidence/phase17-managed-ai-refresh-preflight-readonly-20260522.md`

## Guardrails

- Do not run `terraform apply`.
- Do not enable EventBridge schedules.
- Do not change DNS, ACM, CloudWatch alarms, budgets, or dashboard hosting.
- Do not publish raw model text directly to the public dashboard.
- Do not expose secrets, model prompts with private data, or pre-signed URLs in
  evidence.
- Keep S3 as the artifact store of record and Step Functions as the observable
  workflow boundary.

## Decision

Recommended Phase 17 implementation path: **Bedrock-first managed AI refresh
behind the existing validation and publish boundary, with deterministic fallback
kept available.**

Why this path:

- Bedrock is the clearest AWS-native managed AI story for a Cloud Solution
  Architect portfolio.
- It avoids introducing ECS/Fargate/OpenClaw runtime operations before the
  managed model boundary is proven.
- The existing `ai_input_bundle_v1` already contains model instructions and
  validated energy/news context.
- The existing `ai_insight_v1` and `dashboard_snapshot_v1` schemas already
  provide the safety gate.
- The current deterministic merge remains useful as a rollback and comparison
  path.

OpenClaw remains a valid later option if the project needs explicit open model
runtime ownership, but it should not be the first managed-AI refresh slice.

## Target Architecture

The target workflow keeps the existing state-machine shape and introduces a
managed model action where the deterministic merge currently sits.

```text
CreateAiInputBundle
  -> MergeAiInsightManaged
  -> ValidateAiInsight
  -> PublishDashboardSnapshot
```

The managed action should:

- read `ai_input_bundle_v1` from the private lake bucket
- construct a constrained prompt from the validated bundle
- call Bedrock `InvokeModel`
- parse the model response as JSON
- validate against `ai_insight_v1`
- write the validated result to
  `curated/source=ai_orchestration/dataset=ai_insight/run_id=<run_id>/payload.json`
- write failures to the existing failed-path convention
- preserve the previous public dashboard snapshot when validation fails

## Terraform Impact

Expected future Terraform scope:

- add an opt-in variable such as `ai_orchestration_managed_ai_enabled`
- add a model identifier variable such as `ai_orchestration_bedrock_model_id`
- add least-privilege `bedrock:InvokeModel` permission to the AI orchestration
  Lambda role only when managed AI is enabled
- optionally add a state-machine branch or replacement state name for
  `MergeAiInsightManaged`
- keep `ai_orchestration_schedule_enabled = false`

Out of scope for the first implementation slice:

- EventBridge schedule enablement
- DNS, ACM, or custom domain
- CloudWatch alarms and budgets
- OpenClaw/ECS/Fargate runtime
- multi-agent orchestration
- model fine-tuning

## Proof Commands

Use these read-only checks before any managed-AI implementation branch:

```bash
git switch main
git pull --ff-only origin main
git status --short --branch

STATE_MACHINE_ARN="arn:aws:states:eu-west-2:464975959576:"\
"stateMachine:energy-market-ai-insight-orchestration"
LAMBDA_QUERY="{FunctionName:FunctionName,Runtime:Runtime,Handler:Handler,"\
"Timeout:Timeout,MemorySize:MemorySize,LastModified:LastModified,State:State}"

aws events describe-rule \
  --name energy-market-ai-orchestration-schedule \
  --query '{name:Name,state:State,schedule:ScheduleExpression}' \
  --output json

aws stepfunctions describe-state-machine \
  --state-machine-arn "$STATE_MACHINE_ARN" \
  --query '{name:name,status:status,type:type,roleArn:roleArn}' \
  --output json

aws lambda get-function-configuration \
  --function-name energy-market-news-ai-orchestration \
  --query "$LAMBDA_QUERY" \
  --output json
```

Use these local checks after a future code-only managed-AI implementation:

```bash
.venv/bin/python scripts/check_phase8_runtime.py
.venv/bin/python scripts/check_phase8_handlers.py
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
.venv/bin/python -m compileall energy_market scripts lambda
npx markdownlint-cli2 README.md PLANS.md docs/phase-17-managed-ai-refresh-preflight.md
git diff --check
```

## Rollback Path

If a future managed-AI implementation produces invalid or low-confidence
results:

1. Keep `ai_orchestration_schedule_enabled = false`.
2. Revert the state-machine action to `MergeAiInsightDeterministic`.
3. Remove or disable `bedrock:InvokeModel` permission from the AI
   orchestration Lambda role.
4. Leave the existing public `dashboard_snapshot_v1.json` untouched.
5. Rerun the deterministic manual Step Functions workflow only after the
   rollback plan is reviewed.

## Phase 17 Exit Criteria

This planning/preflight state is complete when:

- PLANS and README identify managed AI refresh as the next controlled path.
- The target operating model remains aligned with the chosen Bedrock-first
  boundary.
- Read-only evidence confirms the current schedule remains disabled and the
  manual orchestration path exists.
- The next implementation slice has explicit guardrails, proof commands, and a
  rollback path.

## Phase 17A Result

Phase 17A implemented the first code-only managed AI boundary:

- `energy_market/managed_ai.py` builds constrained Bedrock Runtime requests
  and parses common Bedrock response shapes
- `lambda/news_ai_orchestration.py` now has a `MergeAiInsightManaged` action
  beside `MergeAiInsightDeterministic`
- `scripts/check_phase17a_managed_ai_adapter.py` proves the managed path with a
  fake Bedrock client
- invalid managed output is rejected by `ai_insight_v1` validation and routed
  to the existing failed-path convention

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
were made.

## Next Implementation Slice

Recommended next slice: **Phase 17B: controlled live Bedrock invocation
preflight**.

Scope:

- choose one low-cost Bedrock model and region
- define a hard token and cost budget for a single manual invocation
- review the exact IAM delta for `bedrock:InvokeModel`
- decide whether to deploy the handler/state-machine change or perform a
  separate proof script first
- keep EventBridge schedules, DNS, ACM, alarms, OpenClaw runtime, and repeated
  model invocation out of scope

OpenClaw/local model comparison remains a later cost-control and creativity
slice after the AWS-managed boundary is proven.

## Phase 17B Result

Phase 17B completed the live-invocation preflight and decided **not** to invoke
Bedrock yet.

Read-only evidence:
`docs/evidence/phase17b-bedrock-preflight-readonly-20260523.md`

Detailed plan:
`docs/phase-17b-controlled-bedrock-invocation-preflight.md`

Decision summary:

- Claude 3 Haiku matches the current Anthropic-compatible adapter, but the
  model agreement is not available yet in this account/region.
- Mistral Ministral 8B is available in `eu-west-2` and is lower cost, but it
  needs provider-specific request/response support before live invocation.
- No Terraform apply, IAM change, live model invocation, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made.

Recommended next slice: **Phase 17C: Mistral compatibility proof**, unless the
Anthropic access prerequisite is deliberately completed first.

## Phase 17C Result

Phase 17C completed the Mistral compatibility proof locally.

Evidence:
`docs/evidence/phase17c-mistral-compatibility-proof-20260523.md`

Result:

- the existing Anthropic request path remains intact
- Mistral chat-completion request shape is supported
- Mistral `choices[].message.content` responses parse into the existing
  `ai_insight_v1` validation path
- invalid managed output still reaches the failed-path quarantine
- no live model invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
  were made

Recommended next slice: **Phase 17D: one controlled live Mistral invocation**,
only after explicit approval.

## Phase 17D Result

Phase 17D performed one controlled live Mistral invocation and stopped at the
validation boundary.

Evidence:

- `docs/evidence/phase17d-mistral-live-invocation-summary-20260523.md`
- `docs/evidence/phase17d-mistral-live-invocation-metadata-20260523.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- output failed `ai_insight_v1` validation
- the public dashboard snapshot was not changed
- raw model output was not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17E: local Mistral prompt/response-shape
hardening**, before any second live invocation.

## Phase 17E Result

Phase 17E completed the local Mistral prompt and response-shape hardening
without a live model call.

Evidence:
`docs/evidence/phase17e-mistral-response-shape-hardening-20260523.md`

Result:

- the prompt now states that the root JSON object must be the `ai_insight_v1`
  object itself
- the prompt explicitly rejects wrapper keys such as `ai_insight`, `result`,
  `output`, `response`, and `data`
- the parser accepts only the observed one-key `ai_insight` wrapper from
  Phase 17D before validation
- broader or unsafe wrapper objects continue into the existing validation
  failure path
- local fake-client proof covers direct Mistral output, the observed wrapper
  shape, and unsafe wrapper rejection
- no live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
  were made

Recommended next slice: **Phase 17F: one controlled second live Mistral
invocation**, only after explicit approval and using the same hard one-run
budget cap with no retries unless separately approved.

## Phase 17F Result

Phase 17F performed one controlled second live Mistral invocation and stopped
before validation because the response text was not complete JSON.

Evidence:

- `docs/evidence/phase17f-mistral-second-live-invocation-summary-20260524.md`
- `docs/evidence/phase17f-mistral-second-live-invocation-metadata-20260524.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00135217`
- output failed at parse time before `ai_insight_v1` validation
- sanitized response shape shows `finish_reason` was `length`
- no validated AI insight was produced
- the public dashboard snapshot was not changed
- raw prompt and raw model output were not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17G: local Mistral JSON-completion
hardening**, before any third live invocation.

## Phase 17G Result

Phase 17G completed local Mistral JSON-completion hardening without a live model
call.

Evidence:
`docs/evidence/phase17g-mistral-json-completion-hardening-20260524.md`

Result:

- prompt wording now requires complete JSON and tells the model to shorten
  prose rather than truncate JSON
- the managed AI default output-token cap is raised from `800` to `1600`
- the Lambda managed path uses the shared managed-AI default when
  `BEDROCK_MAX_TOKENS` is unset
- incomplete markdown fences and truncated JSON now produce sanitized local
  parser errors
- local fake-client proof covers complete fenced JSON, incomplete fenced JSON,
  truncated JSON, exact wrapper handling, unsafe wrapper rejection, and the
  existing managed-handler paths
- no live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
  were made

Recommended next slice: **Phase 17H: one controlled third live Mistral
invocation**, only after explicit approval and using the raised `1600`
output-token cap with no retry unless separately approved.

## Phase 17H Result

Phase 17H performed one controlled third live Mistral invocation and stopped at
the validation boundary.

Evidence:

- `docs/evidence/phase17h-mistral-third-live-invocation-summary-20260524.md`
- `docs/evidence/phase17h-mistral-third-live-invocation-metadata-20260524.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00126615`
- raised `1600` output-token cap prevented truncation
- sanitized response shape shows `finish_reason` was `stop`
- output failed `ai_insight_v1` validation because it used a root
  `ai_insight_v1` wrapper
- no validated AI insight was produced
- the public dashboard snapshot was not changed
- raw prompt and raw model output were not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17I: local Mistral root-wrapper hardening**,
before any fourth live invocation.

## Phase 17I Result

Phase 17I completed local Mistral root-wrapper hardening without a live model
call.

Evidence:
`docs/evidence/phase17i-mistral-root-wrapper-hardening-20260524.md`

Result:

- exact one-key `ai_insight_v1` wrappers now normalize before schema validation
- broad `ai_insight_v1` wrappers with sibling keys remain rejected
- prompt wording explicitly rejects both `ai_insight_v1` and `ai_insight`
  wrapper keys
- local fake-client proof covers direct and Mistral response forms
- no live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
  were made

Recommended next slice: **Phase 17J: one controlled fourth live Mistral
invocation**, only after explicit approval with no retry unless separately
approved.

## Phase 17J Preflight Decision

Phase 17J preflight reviewed whether a fourth controlled live Mistral
invocation is justified after Phase 17I local root-wrapper hardening.

Evidence:
`docs/evidence/phase17j-live-mistral-preflight-decision-20260526.md`

Decision:

- recommendation: **GO candidate, pending explicit approval**
- no live Bedrock invocation was made in this preflight state
- Phase 17I locally addresses the exact Phase 17H root-wrapper failure
- local adapter proof passed
- Bedrock model lookup confirmed `mistral.ministral-3-8b-instruct` in
  `eu-west-2`
- estimated one-call cost is approximately `$0.001294210`
- dashboard publish remains blocked

Recommended next slice: **Phase 17J execution**, only after explicit approval
and with one-call, no-retry, no-publish guardrails.

## Phase 17J Execution Result

Phase 17J execution performed one controlled fourth live Mistral invocation and
stopped at the validation boundary.

Evidence:

- `docs/evidence/phase17j-mistral-fourth-live-invocation-summary-20260526.md`
- `docs/evidence/phase17j-mistral-fourth-live-invocation-metadata-20260526.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00127788`
- Phase 17I root-wrapper normalization worked live
- parsed payload had `schema_version: ai_insight_v1`
- output failed `ai_insight_v1` validation because nested insight fields were
  missing or shaped incorrectly
- no validated AI insight was produced
- the public dashboard snapshot was not changed
- raw prompt and raw model output were not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17K: local Mistral schema-field hardening**,
before any fifth live invocation.

## Phase 17K Result

Phase 17K completed local Mistral schema-field hardening without a live model
call.

Evidence:

- `docs/evidence/phase17k-mistral-schema-field-hardening-20260526.md`

Result:

- no live Bedrock Runtime call was made
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, dashboard hosting change, or dashboard publish was
  performed
- prompt now explicitly lists required insight fields
- prompt rejects generic `references` as a substitute for separate
  `energy_references` and `news_references`
- prompt requires `validation_notes` as an array of strings
- local fake-client proof reproduces the Phase 17J nested-field failure shape
  and confirms schema validation still rejects unsafe output
- deterministic fallback remains unchanged

Recommended next slice: **Phase 17L preflight decision**, before any fifth live
Mistral invocation.

## Phase 17L Preflight Decision

Phase 17L reviewed whether a fifth controlled live Mistral invocation is
justified after Phase 17K local schema-field hardening.

Evidence:

- `docs/evidence/phase17l-live-mistral-preflight-decision-20260526.md`

Decision:

- recommendation: **go-candidate for one controlled fifth live invocation**
- no live Bedrock Runtime call was made in this preflight state
- execution remains blocked until explicit approval in a separate substate

Preflight facts:

- four controlled Mistral live calls have been made so far
- cumulative estimated live Mistral cost is `$0.00516320`
- previous calls used no retry and did not publish to the public dashboard
- Phase 17K locally targets the exact Phase 17J nested schema-field failure

Recommended next slice: **Phase 17L execution**, only after explicit approval
for one controlled fifth live Mistral invocation.

## Phase 17L Execution Result

Phase 17L execution performed one controlled fifth live Mistral invocation and
stopped at the validation boundary.

Evidence:

- `docs/evidence/phase17l-mistral-fifth-live-invocation-summary-20260527.md`
- `docs/evidence/phase17l-mistral-fifth-live-invocation-metadata-20260527.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00134251`
- parsed payload had `schema_version: ai_insight_v1`
- output still failed `ai_insight_v1` validation, but the remaining failures
  were narrower nested object-shape issues
- `time_window` was returned as a string instead of an object
- `energy_references` included unexpected `value` fields
- no validated AI insight was produced
- the public dashboard snapshot was not changed
- raw prompt and raw model output were not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17M: local Mistral object-shape hardening**,
before any sixth live invocation.

## Phase 17M Result

Phase 17M completed local Mistral object-shape hardening without a live model
call.

Evidence:

- `docs/evidence/phase17m-mistral-object-shape-hardening-20260528.md`

Result:

- no live Bedrock Runtime call was made
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, dashboard hosting change, or dashboard publish was
  performed
- prompt now requires `time_window` as an object with `start` and `end`
  date-time strings
- prompt explicitly rejects string `time_window`
- prompt forbids extra reference fields such as `value`, `date`, and
  `timestamp`
- local fake-client proof reproduces the Phase 17L object-shape failure and
  confirms schema validation still rejects unsafe output
- deterministic fallback remains unchanged

Recommended next slice: **Phase 17N preflight decision**, before any sixth live
Mistral invocation.

## Phase 17N Preflight Decision

Phase 17N reviewed whether a sixth controlled live Mistral invocation is
justified after Phase 17M local object-shape hardening.

Evidence:

- `docs/evidence/phase17n-live-mistral-preflight-decision-20260528.md`

Decision:

- recommendation: **go-candidate for one controlled sixth live invocation**
- no live Bedrock Runtime call was made in this preflight state
- execution remains blocked until explicit approval in a separate substate

Preflight facts:

- five controlled Mistral live calls have been made so far
- cumulative estimated live Mistral cost is `$0.00650571`
- previous calls used no retry and did not publish to the public dashboard
- Phase 17M locally targets the exact Phase 17L nested object-shape failure

Recommended next slice: **Phase 17N execution**, only after explicit approval
for one controlled sixth live Mistral invocation.

## Phase 17N Execution Result

Phase 17N execution performed one controlled sixth live Mistral invocation and
stopped before any dashboard publish.

Evidence:

- `docs/evidence/phase17n-mistral-sixth-live-invocation-summary-20260528.md`
- `docs/evidence/phase17n-mistral-sixth-live-invocation-metadata-20260528.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00136229`
- parsed payload had `schema_version: ai_insight_v1`
- output passed `ai_insight_v1` validation in memory
- no validated payload was committed because the approved boundary was
  sanitized metadata only
- the public dashboard snapshot was not changed
- raw prompt and raw model output were not committed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17O: managed AI publish/deployment
preflight**, before any dashboard update or handler/state-machine switch.

## Phase 17O Publish/Deployment Preflight

Phase 17O reviewed whether Phase 17N validation success is enough to publish
managed AI output or deploy managed handler/state-machine wiring.

Evidence:

- `docs/evidence/phase17o-managed-ai-publish-deployment-preflight-20260528.md`

Decision:

- no-go for immediate dashboard publish
- no-go for immediate handler/state-machine deployment
- next safest boundary is public-safe validated payload capture

Preflight facts:

- Phase 17N proved Mistral can produce schema-valid `ai_insight_v1` in memory
- Phase 17N did not commit the parsed payload because the approved boundary was
  sanitized metadata only
- current Terraform Step Functions definition still routes through
  `MergeAiInsightDeterministic`
- `MergeAiInsightManaged` exists in code, but production deployment needs IAM,
  environment, state-machine, rollback, and failure-path proof

Recommended next slice: **Phase 17P: managed AI validated payload capture**,
before any dashboard publish or handler/state-machine switch.

## Phase 17P Validated Payload Capture Result

Phase 17P captured a public-safe validated `ai_insight_v1` payload as evidence.

Evidence:

- `docs/evidence/phase17p-managed-ai-validated-payload-capture-summary-20260528.md`
- `docs/evidence/phase17p-managed-ai-validated-payload-capture-metadata-20260528.json`
- `docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json`

Result:

- one live Bedrock Runtime call was made to
  `mistral.ministral-3-8b-instruct`
- manual retries: `0`
- estimated invocation cost: `$0.00135608`
- parsed payload passed `ai_insight_v1` validation
- public-safe validated payload was committed as evidence
- one private lake S3 reference from the model output was replaced with a
  public-safe curated dataset reference before commit
- raw prompt and raw model output were not committed
- the public dashboard snapshot was not changed
- no Terraform apply, IAM change, state-machine deploy, schedule enablement,
  DNS, ACM, alarms, budgets, or dashboard hosting changes were made

Recommended next slice: **Phase 17Q: managed AI dashboard publish preflight**,
before any public dashboard update.

## Phase 17Q Dashboard Publish Preflight Result

Phase 17Q converted the Phase 17P validated managed AI payload into a local
candidate `dashboard_snapshot_v1` evidence file.

Evidence:

- `docs/evidence/phase17q-managed-ai-dashboard-publish-preflight-20260529.md`
- `docs/evidence/phase17q-managed-ai-dashboard-publish-candidate-20260529.json`
- `docs/evidence/phase17q-current-live-dashboard-snapshot-http-check-20260529.txt`

Result:

- no Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, hosting change, S3 write,
  CloudFront invalidation, or dashboard publish was performed
- the local candidate validates against `dashboard_snapshot_v1`
- the current live CloudFront snapshot remains healthy and unchanged
- dashboard publish remains a no-go because one managed energy source becomes a
  non-URL anchor target in the React dashboard
- managed handler/state-machine deployment remains separate and blocked

Recommended next slice: **Phase 17R: local managed AI dashboard source-link
hardening**, before any public dashboard publish.

## Phase 17R Source-Link Hardening Result

Phase 17R locally hardened dashboard source-link generation before any managed
AI dashboard publish.

Evidence:

- `docs/evidence/phase17r-managed-ai-dashboard-source-link-hardening-20260529.md`
- `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`
- `scripts/check_phase17r_dashboard_source_links.py`

Result:

- no Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, hosting change, S3 write,
  CloudFront invalidation, or dashboard publish was performed
- public `http` and `https` news source URLs are preserved
- private, custom-scheme, or plain-text managed energy references use the
  public dashboard fallback `dashboard-data.json`
- source context is retained in the source label
- the Phase 17R candidate validates against `dashboard_snapshot_v1`

Recommended next slice: **Phase 17S: managed AI dashboard publish decision**,
only with explicit approval before any S3 write or CloudFront invalidation.

## Phase 17S Dashboard Publish Decision Result

Phase 17S reviewed whether the Phase 17R managed AI dashboard candidate is
ready for public dashboard publish execution.

Evidence:

- `docs/evidence/phase17s-managed-ai-dashboard-publish-decision-20260529.md`
- `docs/evidence/phase17s-current-live-dashboard-snapshot-http-check-20260529.txt`
- `docs/evidence/phase17r-managed-ai-dashboard-source-link-candidate-20260529.json`

Result:

- no Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, hosting change, S3 write,
  CloudFront invalidation, or dashboard publish was performed
- current live CloudFront snapshot still matches the Phase 16 rollback payload
  by SHA256
- Phase 17R source-link proof remains green
- decision is go-candidate for publish execution, but execution still requires
  explicit approval
- managed handler/state-machine deployment remains separate and blocked

Recommended next slice: **Phase 17S execution substate: managed AI dashboard
publish**, only after explicit approval for S3 writes and CloudFront
invalidation.

## Phase 17S Dashboard Publish Execution Result

Phase 17S execution published the approved Phase 17R managed AI dashboard
candidate to the live CloudFront-backed dashboard snapshot path.

Evidence:

- `docs/evidence/phase17s-managed-ai-dashboard-publish-execution-summary-20260529.md`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-http-check-20260529.txt`
- `docs/evidence/phase17s-dashboard-publish-cloudfront-invalidation-status-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-latest-head-20260529.json`
- `docs/evidence/phase17s-dashboard-publish-immutable-head-20260529.json`

Result:

- no Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, static-site rebuild, or
  managed workflow deployment was performed
- latest `dashboard_snapshot_v1.json` now serves the approved managed AI
  dashboard snapshot
- immutable snapshot was published at
  `snapshots/run_id=managed-ai-phase17p-20260528T213401Z/dashboard_snapshot_v1.json`
- CloudFront invalidation `I9MCXBX6M0BCO1HN0BWCKZO5H9` completed
- CloudFront latest and immutable paths both match the approved candidate
  SHA256

Recommended next slice: **Phase 17T: managed AI dashboard post-publish demo
verification**, read-only.

## Phase 17T Post-Publish Demo Verification Result

Phase 17T performed read-only hosted dashboard verification after Phase 17S
published the managed AI dashboard snapshot.

Evidence:

- `docs/evidence/phase17t-managed-ai-dashboard-demo-verification-20260529.md`
- `docs/evidence/phase17t-managed-ai-dashboard-demo-http-check-20260529.txt`
- `docs/evidence/phase17t-managed-ai-dashboard-demo-json-check-20260529.txt`

Result:

- no Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, S3 write, CloudFront
  invalidation, static-site rebuild, or managed workflow deployment was
  performed
- CloudFront returned `200` for root, `index.html`, `dashboard-data.json`,
  latest snapshot, and immutable managed AI snapshot paths
- latest and immutable snapshot paths match the approved Phase 17R candidate
  SHA256
- both snapshot paths validate against `dashboard_snapshot_v1`
- managed source-link hardening is visible in the live snapshot

Recommended next slice: **Phase 17U: managed workflow deployment preflight**,
before any Step Functions routing, IAM, or schedule changes.

## Phase 17U Managed Workflow Deployment Preflight Result

Phase 17U reviewed whether the managed AI handler is ready to become the
deployed Step Functions workflow path.

Evidence:

- `docs/evidence/phase17u-managed-workflow-deployment-preflight-20260529.md`

Decision:

- immediate managed workflow deployment is a no-go
- the next safest boundary is a plan-only Terraform/IAM/Step Functions delta
  preflight
- schedules remain disabled
- deterministic fallback remains the rollback posture

Preflight facts:

- `MergeAiInsightManaged` exists in the Lambda handler and remains covered by
  the local fake-client proof
- Terraform still sets `AI_ORCHESTRATION_MODE = "deterministic"`
- Terraform still routes the state machine through
  `MergeAiInsightDeterministic`
- Terraform does not yet model the Bedrock invocation IAM delta for the
  orchestration Lambda
- managed dashboard publication has been proven separately from managed
  workflow deployment

Recommended next slice: **Phase 17V: managed workflow Terraform/IAM delta
preflight**, plan-only unless explicitly approved.

## Phase 17V Managed Workflow Terraform/IAM Delta Preflight Result

Phase 17V modeled the managed workflow deployment delta in Terraform without
applying it.

Evidence:

- `docs/evidence/phase17v-managed-workflow-terraform-iam-delta-preflight-20260529.md`
- `docs/evidence/phase17v-managed-workflow-terraform-plan-isolated-refreshfalse-20260529.txt`
- `docs/evidence/phase17v-deterministic-rollback-terraform-plan-refreshfalse-20260529.txt`

Result:

- no Bedrock invocation, Terraform apply, IAM mutation, Lambda deploy,
  Step Functions deploy, schedule enablement, dashboard publish, or live
  workflow execution was performed
- managed workflow routing is now modeled behind
  `ai_orchestration_managed_ai_enabled`
- the isolated managed plan showed `Plan: 1 to add, 4 to change, 0 to destroy`
- the deterministic rollback/default plan showed `No changes`
- an unsafe local plan also showed unrelated CloudFront destroys when
  `dashboard_cloudfront_enabled = true` was not preserved, so any future apply
  review must explicitly keep the live dashboard hosting toggle enabled

Decision:

- immediate managed workflow deployment remains a no-go
- next slice should be a deployment decision, not an automatic apply

Recommended next slice: **Phase 17W: managed workflow deployment decision**.

## Phase 17W Managed Workflow Deployment Decision Result

Phase 17W reviewed the Phase 17V Terraform/IAM delta and decided whether it is
safe to proceed toward a managed workflow deployment execution boundary.

Evidence:

- `docs/evidence/phase17w-managed-workflow-deployment-decision-20260529.md`

Decision:

- managed workflow deployment is a go-candidate, not an automatic apply
- execution requires explicit approval in a separate substate
- the unsafe local Phase 17V plan must not be applied
- any execution must preserve `dashboard_cloudfront_enabled = true`
- any execution must keep `ai_orchestration_schedule_enabled = false`
- managed workflow execution and schedule enablement remain separate later
  boundaries

Recommended next slice: **Phase 17W execution substate**, only after explicit
approval for a controlled Terraform apply.

## Phase 17W Controlled Terraform Apply Result

Phase 17W execution applied the managed workflow Terraform delta after explicit
approval.

Evidence:

- `docs/evidence/phase17w-managed-workflow-terraform-apply-summary-20260529.md`
- `docs/evidence/phase17w-managed-workflow-terraform-apply-plan-20260529.txt`
- `docs/evidence/phase17w-managed-workflow-terraform-apply-20260529.txt`
- `docs/evidence/phase17w-managed-workflow-postapply-plan-refreshfalse-20260529.txt`

Result:

- saved plan showed `Plan: 1 to add, 4 to change, 0 to destroy`
- apply completed with `Resources: 1 added, 2 changed, 0 destroyed`
- Lambda environment now uses managed mode
- Step Functions now routes through `MergeAiInsightManaged`
- EventBridge schedule remains disabled
- CloudFront dashboard distribution remains deployed
- no Bedrock invocation, live workflow execution, dashboard publish, schedule
  enablement, DNS, ACM, alarms, budgets, or CloudFront invalidation was
  performed

Recommended next slice: **Phase 17X: managed workflow smoke decision** before
any live workflow execution.

## Phase 17X Managed Workflow Smoke Decision Result

Phase 17X reviewed whether the managed Step Functions workflow should be run
once as a manual smoke execution.

Evidence:

- `docs/evidence/phase17x-managed-workflow-smoke-decision-20260530.md`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-lambda-config-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-state-machine-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-schedule-state-20260530.json`
- `docs/evidence/phase17x-managed-workflow-smoke-decision-dashboard-http-check-20260530.txt`

Decision:

- managed workflow smoke execution is a go-candidate, not automatic
- execution requires explicit approval in a separate substate
- the deployed smoke is publish-capable because the state machine ends at
  `PublishDashboardSnapshot`
- rollback evidence must be captured before execution
- EventBridge schedule enablement remains blocked

Recommended next slice: **Phase 17Y: controlled managed workflow smoke
execution**, only after explicit approval.

## Phase 17Y Managed Workflow Smoke Execution Result

Phase 17Y ran one explicitly approved manual managed workflow smoke execution.

Evidence:

- `docs/evidence/phase17y-managed-workflow-smoke-execution-summary-20260530.md`
- `docs/evidence/phase17y-managed-workflow-smoke-describe-execution-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-execution-history-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-s3-artifacts-20260530.json`
- `docs/evidence/phase17y-managed-workflow-smoke-dashboard-impact-summary-20260530.txt`

Result:

- execution status: `FAILED`
- manual retries: `0`
- generated run ID: `ai-insight-20260530T205944Z-df1fdb6a`
- failure state: `MergeAiInsightManaged`
- deployed Lambda handler did not recognize `MergeAiInsightManaged`
- Bedrock was not invoked
- estimated Bedrock cost: `$0.00`
- latest dashboard snapshot did not change
- EventBridge schedule remains disabled

Recommended next slice: **Phase 17Z: Lambda package refresh preflight** before
any second managed workflow smoke execution.

## Phase 17Z Lambda Package Refresh Preflight Result

Phase 17Z reviewed the Lambda package refresh boundary without running another
workflow execution or applying Terraform.

Evidence:

- `docs/evidence/phase17z-lambda-package-refresh-preflight-20260601.md`
- `docs/evidence/phase17z-lambda-package-local-before-build-20260601.txt`
- `docs/evidence/phase17z-lambda-package-local-after-build-20260601.txt`
- `docs/evidence/phase17z-current-lambda-config-sanitized-20260601.json`
- `docs/evidence/phase17z-current-schedule-state-20260601.json`
- `docs/evidence/phase17z-lambda-package-refresh-terraform-plan-refreshfalse-20260601.txt`
- `docs/evidence/phase17z-lambda-package-refresh-targeted-terraform-plan-refreshfalse-20260601.txt`

Result:

- the deployed Lambda `CodeSha256` still matches the stale pre-rebuild package
- the stale package did not contain `MergeAiInsightManaged`
- the rebuilt package contains `MergeAiInsightManaged` and
  `energy_market/managed_ai.py`
- root refresh-false plan shows `Plan: 0 to add, 2 to change, 0 to destroy`
- targeted comparison plan shows
  `Plan: 0 to add, 1 to change, 0 to destroy`
- EventBridge schedule remains disabled
- no Bedrock invocation, Step Functions execution, Terraform apply, Lambda
  deploy, IAM mutation, schedule enablement, S3 write, CloudFront invalidation,
  or dashboard publish was performed

Decision:

- Lambda package refresh is a go-candidate, not automatic execution
- any apply remains blocked until explicit approval in a separate execution
  substate
- do not run a second managed workflow smoke until the deployed Lambda package
  is refreshed and verified

Recommended next slice: **Phase 17Z execution substate: controlled Lambda
package refresh apply**, only after explicit approval.
