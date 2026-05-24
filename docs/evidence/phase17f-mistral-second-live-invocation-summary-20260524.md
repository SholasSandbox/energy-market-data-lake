# Phase 17F Second Live Mistral Invocation Summary

Date: 2026-05-24

## Boundary

One controlled second live Bedrock Runtime invocation was performed against
Mistral Ministral 8B after Phase 17E local prompt and response-shape hardening.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Live Mistral output must not be accepted unless it parses and validates as
  `ai_insight_v1`.
- The public dashboard must remain unchanged unless a later state explicitly
  approves publishing validated output.

Green:

- The one live invocation completed, but the result was not accepted.
- The parser stopped before schema validation because the text output was not
  complete JSON.
- No validated `ai_insight_v1` evidence was produced.
- No dashboard snapshot was published.

Regression:

- Local managed AI adapter proof still passes.
- Unsafe wrapper rejection remains covered by the Phase 17E fake-client proof.
- Deterministic fallback remains unchanged.

## Invocation

- Model: `mistral.ministral-3-8b-instruct`
- Region: `eu-west-2`
- Max output tokens: `800`
- Temperature: `0.1`
- Budget cap: `$0.10`
- Manual invocation count: `1`
- Manual retries: `0`
- Prompt text was not written to evidence.
- Raw model response was not written to evidence.

## Result

- Status: `parse_failed`
- Sanitized failure reason: model output was not valid JSON.
- Sanitized response shape:
  - finish reason: `length`
  - output started with a markdown fence
  - output did not end as a complete JSON object
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Metadata evidence:
  `docs/evidence/phase17f-mistral-second-live-invocation-metadata-20260524.json`

## Usage

- Prompt tokens: `5079`
- Completion tokens: `800`
- Total tokens: `5879`
- Estimated invocation cost: `$0.00135217`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

## Decision

Do not retry live invocation in this state. The next safe boundary is a local
prompt/token-budget hardening slice before any further paid call.

The likely next state is **Phase 17G: local Mistral JSON-completion hardening**:

- remove or further discourage markdown fences
- decide whether `max_tokens=800` is too low for this schema
- add local fake-client coverage for fenced but incomplete JSON
- keep any third live invocation behind explicit approval

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17F evidence and documentation
changes from the branch. Do not run a live retry unless a later state explicitly
approves it.
