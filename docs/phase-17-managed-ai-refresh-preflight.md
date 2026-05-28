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
