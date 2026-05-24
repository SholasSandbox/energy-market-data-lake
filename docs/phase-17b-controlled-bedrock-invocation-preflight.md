# Phase 17B: Controlled Live Bedrock Invocation Preflight

## Goal

Decide whether the first live Bedrock invocation is safe, cheap, and controlled
after Phase 17A added the local Bedrock adapter proof.

This phase is preflight-only. It does not invoke a model, run Terraform apply,
change IAM, deploy Step Functions, enable schedules, alter DNS/ACM, add alarms,
change budgets, or touch dashboard hosting.

## Current State

- Phase 17A is merged.
- `MergeAiInsightManaged` exists beside `MergeAiInsightDeterministic`.
- The managed path is locally proven with a fake Bedrock client.
- The current request body is Anthropic-compatible.
- Deterministic merge remains the fallback and comparison path.
- EventBridge schedule remains disabled.

Read-only evidence:
`docs/evidence/phase17b-bedrock-preflight-readonly-20260523.md`

## Pricing And Model Access References

Official AWS references used for this decision:

- Amazon Bedrock pricing:
  <https://aws.amazon.com/bedrock/pricing/>
- Amazon Bedrock model access:
  <https://docs.aws.amazon.com/bedrock/latest/userguide/model-access.html>
- Amazon Bedrock foundation model discovery:
  <https://docs.aws.amazon.com/bedrock/latest/APIReference/API_ListFoundationModels.html>

AWS pricing is model-dependent. For this project, the sample
`ai_input_bundle_v1` prompt is approximately 4,092 input tokens using the simple
characters-divided-by-four estimate, and Phase 17A caps planned output at 800
tokens.

## Candidate Models

### Claude 3 Haiku

Model ID:
`anthropic.claude-3-haiku-20240307-v1:0`

Why it is attractive:

- It matches the current Anthropic-compatible adapter.
- It is designed as a lower-cost Claude model.
- It supports on-demand text output in `eu-west-2`.

Why it is not ready for live invocation:

- Read-only access check returned `agreementAvailability.status =
  NOT_AVAILABLE`.
- AWS documentation says Anthropic models require first-time use-case details
  before invocation.

### Mistral Ministral 8B

Model ID:
`mistral.ministral-3-8b-instruct`

Why it is attractive:

- It is available in `eu-west-2`.
- Read-only access check returned `agreementAvailability.status = AVAILABLE`.
- AWS pricing page lists Europe (London) pricing for Mistral Ministral 8B at a
  very low token rate.

Why it is not ready for live invocation:

- The current Phase 17A adapter builds an Anthropic-compatible request body.
- A Mistral invocation should have provider-specific request/response handling
  and fake-client proof before any live call.

## Cost Envelope

Phase 17B decision budget:

- one manual invocation only
- no retries without explicit approval
- planned input estimate: about 4,092 tokens
- planned output cap: 800 tokens
- hard budget cap: **$0.10**

The `$0.10` cap is intentionally much higher than the estimated one-shot cost,
so it still protects against modest token-estimation error while keeping the
demo inexpensive.

## IAM Delta

The current AI orchestration Lambda role has S3 permissions for the private
lake and dashboard buckets. It does not currently include Bedrock runtime
permissions.

Minimum future IAM delta:

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:eu-west-2::foundation-model/<chosen-model-id>"
}
```

Do not use `"Resource": "*"` for the first proof unless the chosen model
requires an inference profile ARN or a broader service pattern that is
documented and reviewed.

## Decision

Phase 17B remains **preflight-only**.

Do not proceed directly to a live invocation from the current branch.

Reasons:

- Claude 3 Haiku matches the current adapter but model agreement is not
  available yet.
- Mistral Ministral 8B appears cheaper and available, but the adapter should be
  extended for provider-specific request and response handling first.
- The first live invocation should happen only after the chosen model has a
  local fake-client proof, exact IAM delta, and explicit one-run budget.

## Recommended Next State

Phase 17C should be one of these, selected deliberately:

1. **Anthropic access path**: complete the Anthropic first-time-use prerequisite
   outside the codebase, then run one controlled Claude 3 Haiku invocation
   using the existing adapter.
2. **Mistral compatibility path**: add Mistral request/response support behind
   fake-client tests, then run one controlled Mistral Ministral 8B invocation.

Recommendation: choose the **Mistral compatibility path** first if cost control
is the priority, because read-only evidence already shows the model agreement
is available in `eu-west-2`.

OpenClaw/local model comparison remains a later cost-control and creativity
slice after one AWS-managed live invocation is proven.

## Phase 17C Result

Phase 17C implemented the Mistral compatibility path locally.

Completed scope:

- provider-aware Bedrock request construction
- Mistral chat-completion request support for
  `mistral.ministral-3-8b-instruct`
- Mistral `choices[].message.content` response parsing
- fake-client proof for both Anthropic and Mistral response shapes
- existing deterministic fallback preserved

Evidence:
`docs/evidence/phase17c-mistral-compatibility-proof-20260523.md`

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
were made.

Next boundary: one controlled live Mistral invocation may be considered in a
separate Phase 17D after explicit approval.

## Phase 17D Result

Phase 17D performed one live Mistral invocation and stopped after validation
failed.

Evidence:

- `docs/evidence/phase17d-mistral-live-invocation-summary-20260523.md`
- `docs/evidence/phase17d-mistral-live-invocation-metadata-20260523.json`

Result:

- one live `bedrock-runtime invoke-model` call was made
- no retry was performed
- output failed `ai_insight_v1` validation
- raw model output was not committed
- no dashboard publish was performed
- no deployed AWS resources were changed

Next boundary: local Mistral prompt/response-shape hardening before any second
live invocation.

## Phase 17E Result

Phase 17E hardened the Mistral prompt and local response-shape handling before
any second paid call.

Evidence:
`docs/evidence/phase17e-mistral-response-shape-hardening-20260523.md`

Result:

- prompt wording now requires the `ai_insight_v1` object at the JSON root
- exact one-key `ai_insight` wrapper output is unwrapped locally before schema
  validation
- unsafe wrappers are not broadly unwrapped and still fail validation
- fake-client proof covers the Phase 17D wrapper pattern without recording raw
  Phase 17D output
- no live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
  schedule enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes
  were made

Next boundary: one controlled second live Mistral invocation may be considered
in a separate Phase 17F only after explicit approval.

## Phase 17F Result

Phase 17F performed the approved second live Mistral invocation and stopped
safely.

Evidence:

- `docs/evidence/phase17f-mistral-second-live-invocation-summary-20260524.md`
- `docs/evidence/phase17f-mistral-second-live-invocation-metadata-20260524.json`

Result:

- one live `bedrock-runtime invoke-model` call was made
- no retry was performed
- output failed before schema validation because it was not complete JSON
- sanitized response shape shows a `length` finish reason
- no validated `ai_insight_v1` payload was produced
- raw model output was not committed
- no dashboard publish was performed
- no deployed AWS resources were changed

Next boundary: local Mistral JSON-completion hardening before any third live
invocation.

## Rollback Path

Because Phase 17B performs no live changes, rollback is simple:

- leave `MergeAiInsightManaged` unused
- keep `MergeAiInsightDeterministic` in the state-machine definition
- do not add `bedrock:InvokeModel`
- do not enable schedules
- keep the existing CloudFront dashboard and live snapshot untouched

If a future live invocation fails, the rollback path remains:

- remove or disable the model-specific `bedrock:InvokeModel` permission
- revert the state machine to `MergeAiInsightDeterministic`
- leave the previous public `dashboard_snapshot_v1.json` untouched
- record failed output only under the existing failed-path convention

## Verification

```bash
npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/phase-17b-controlled-bedrock-invocation-preflight.md \
  docs/evidence/phase17b-bedrock-preflight-readonly-20260523.md

git diff --check
```
