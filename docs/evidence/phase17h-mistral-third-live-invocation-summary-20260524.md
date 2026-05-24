# Phase 17H Third Live Mistral Invocation Summary

Date: 2026-05-24

## Boundary

One controlled third live Bedrock Runtime invocation was performed against
Mistral Ministral 8B after Phase 17G local JSON-completion hardening.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- The Phase 17F live result showed `finish_reason=length`, incomplete fenced
  JSON, and no validated `ai_insight_v1` output.
- The next live proof must show whether the raised `1600` output-token cap
  prevents truncation without accepting schema-invalid output.

Green:

- The invocation completed with `finish_reason=stop`.
- The response was no longer truncated.
- The response still failed validation because the model returned a root wrapper
  key named `ai_insight_v1` instead of placing `schema_version`, `generated_at`,
  and `insights` at the JSON root.
- No validated `ai_insight_v1` evidence was produced.
- No dashboard snapshot was published.

Regression:

- Local managed AI adapter proof still passes.
- Deterministic fallback remains unchanged.
- Schema validation continues to protect the public dashboard from
  schema-invalid managed output.

## Invocation

- Model: `mistral.ministral-3-8b-instruct`
- Region: `eu-west-2`
- Max output tokens: `1600`
- Temperature: `0.1`
- Budget cap: `$0.10`
- Manual invocation count: `1`
- Manual retries: `0`
- Prompt text was not written to evidence.
- Raw model response was not written to evidence.

## Result

- Status: `validation_failed`
- Sanitized failure reason: root object did not match `ai_insight_v1`.
- Sanitized response shape:
  - finish reason: `stop`
  - output started with a markdown fence
  - output had balanced braces
  - output did not end directly with a JSON object because it was fenced
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Metadata evidence:
  `docs/evidence/phase17h-mistral-third-live-invocation-metadata-20260524.json`

## Usage

- Prompt tokens: `5127`
- Completion tokens: `378`
- Total tokens: `5505`
- Estimated invocation cost: `$0.00126615`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

## Decision

Do not retry live invocation in this state. The raised output-token cap fixed
the truncation failure, but the model still produced a schema-invalid wrapper.

The likely next state is **Phase 17I: local Mistral root-wrapper hardening**:

- decide whether to accept the exact `ai_insight_v1` wrapper shape locally
- keep broad wrapper unwrapping rejected
- add fake-client coverage for `ai_insight_v1` wrapper output
- keep any fourth live invocation behind explicit approval

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17H evidence and documentation
changes from the branch. Do not run a live retry unless a later state explicitly
approves it.
