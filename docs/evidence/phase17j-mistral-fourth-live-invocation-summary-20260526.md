# Phase 17J Fourth Live Mistral Invocation Summary

Date: 2026-05-26

## Boundary

One controlled fourth live Bedrock Runtime invocation was performed against
Mistral Ministral 8B after Phase 17I local root-wrapper hardening and Phase 17J
preflight approval.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Phase 17H failed because the model returned an exact one-key
  `ai_insight_v1` wrapper instead of the schema object at the root.
- Phase 17I locally normalized only that exact safe wrapper shape.

Green:

- The fourth live invocation parsed to a root `schema_version` of
  `ai_insight_v1`, confirming the Phase 17I wrapper normalization worked live.
- The result still failed schema validation because the nested insight object
  did not match required `ai_insight_v1` fields.
- No validated `ai_insight_v1` evidence was produced.
- No dashboard snapshot was published.

Regression:

- Local managed AI adapter proof still passes.
- Broad wrapper rejection remains covered locally.
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
- Root schema version parsed as `ai_insight_v1`.
- Sanitized failure reason: nested insight object did not match
  `ai_insight_v1`.
- Example sanitized validation categories:
  - unexpected `references` property
  - missing required `id`, `title`, `region`, `time_window`,
    `energy_references`, and `news_references`
  - `validation_notes` returned as a string instead of an array
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Metadata evidence:
  `docs/evidence/phase17j-mistral-fourth-live-invocation-metadata-20260526.json`

## Usage

- Prompt tokens: `5134`
- Completion tokens: `422`
- Total tokens: `5556`
- Estimated invocation cost: `$0.00127788`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

## Decision

Do not retry live invocation in this state. The root-wrapper failure is fixed,
but the model still needs local schema-field hardening before another live
call.

The likely next state is **Phase 17K: local Mistral schema-field hardening**:

- make required `insights[0]` fields harder to omit
- explicitly reject `references` as a substitute for `energy_references` and
  `news_references`
- require `validation_notes` as an array
- add fake-client coverage for the Phase 17J validation failure shape
- keep any fifth live invocation behind explicit approval

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17J execution evidence and
documentation changes from the branch. Do not run a live retry unless a later
state explicitly approves it.
