# Phase 17L Fifth Live Mistral Invocation Summary

Date: 2026-05-27

## Boundary

One controlled fifth live Bedrock Runtime invocation was performed against
Mistral Ministral 8B after Phase 17K local schema-field hardening and Phase 17L
preflight approval.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Phase 17J parsed to `schema_version: ai_insight_v1`, but nested insight
  validation rejected missing required fields, generic `references`, and a
  string `validation_notes` value.
- Phase 17K locally tightened the nested insight-field prompt contract before
  any further live call.

Green:

- The fifth live invocation parsed to `schema_version: ai_insight_v1`.
- The response produced one insight and kept `risk_level` and `confidence`
  usable in sanitized metadata.
- The result still failed schema validation, but the failure moved from missing
  top-level insight fields to narrower object-shape issues.
- No validated `ai_insight_v1` evidence was produced.
- No dashboard snapshot was published.

Regression:

- Local managed AI adapter proof still passes.
- Schema validation continues to block invalid managed output.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked.

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
- Sanitized failure reason: nested object fields did not exactly match
  `ai_insight_v1`.
- Sanitized validation categories:
  - `energy_references` objects included an unexpected `value` field
  - `time_window` was returned as a string instead of an object with `start`
    and `end`
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Metadata evidence:
  `docs/evidence/phase17l-mistral-fifth-live-invocation-metadata-20260527.json`

## Usage

- Prompt tokens: `5221`
- Completion tokens: `616`
- Total tokens: `5837`
- Estimated invocation cost: `$0.00134251`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

## Decision

Do not retry live invocation in this state. Phase 17L execution shows progress,
but the schema still needs local object-shape hardening before another live
call.

The likely next state is **Phase 17M: local Mistral object-shape hardening**:

- require `time_window` as an object with `start` and `end` date-time strings
- forbid extra fields such as `value` in `energy_references`
- restate the exact allowed fields for `energy_references` and
  `news_references`
- add fake-client coverage for the Phase 17L validation failure shape
- keep any sixth live invocation behind explicit approval

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17L execution evidence and
documentation changes from the branch. Do not run a live retry unless a later
state explicitly approves it.
