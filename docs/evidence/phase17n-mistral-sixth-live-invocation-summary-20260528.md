# Phase 17N Sixth Live Mistral Invocation Summary

Date: 2026-05-28

## Boundary

One controlled sixth live Bedrock Runtime invocation was performed against
Mistral Ministral 8B after Phase 17M local object-shape hardening and Phase 17N
preflight approval.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, raw model-response commit, or validated
payload commit was performed.

## Red-Green Evidence

Red:

- Phase 17L parsed to `schema_version: ai_insight_v1`, but validation rejected
  nested object shapes.
- Phase 17M locally tightened object-shape instructions for `time_window` and
  reference fields before any further live call.

Green:

- The sixth live invocation parsed to `schema_version: ai_insight_v1`.
- The payload passed `ai_insight_v1` validation in memory.
- The response produced one insight and kept `risk_level` and `confidence`
  usable in sanitized metadata.
- No dashboard snapshot was published.

Regression:

- Local managed AI adapter proof still passes.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked.
- Raw prompt, raw response, and validated payload remain uncommitted.

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

- Status: `validation_passed`
- Root schema version parsed as `ai_insight_v1`.
- Sanitized validation result: no schema errors.
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Validated payload not committed because this phase approved sanitized
  metadata only.
- Metadata evidence:
  `docs/evidence/phase17n-mistral-sixth-live-invocation-metadata-20260528.json`

## Usage

- Prompt tokens: `5260`
- Completion tokens: `663`
- Total tokens: `5923`
- Estimated invocation cost: `$0.00136229`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

Total estimated live Mistral cost across Phases 17D, 17F, 17H, 17J, 17L, and
17N is `$0.00786800`.

## Decision

Do not publish in this state. Phase 17N proves that the managed Mistral path can
produce schema-valid `ai_insight_v1` output, but the approved boundary was
sanitized metadata only.

The likely next state is **Phase 17O: managed AI publish/deployment preflight**:

- decide whether to capture a public-safe validated payload in a future
  controlled run
- decide whether managed AI output should be published as a dashboard snapshot
  or remain evidence-only
- review rollback before changing any live dashboard object
- keep deterministic fallback intact
- keep Terraform, IAM, schedules, DNS, ACM, alarms, and budgets unchanged unless
  a future phase explicitly targets them

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17N execution evidence and
documentation changes from the branch. Do not publish managed output unless a
later state explicitly approves it.
