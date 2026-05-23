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
