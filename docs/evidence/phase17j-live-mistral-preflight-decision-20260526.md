# Phase 17J Live Mistral Preflight Decision

Date: 2026-05-26

## Boundary

Phase 17J preflight reviewed whether a fourth controlled live Mistral
invocation is justified after Phase 17I local root-wrapper hardening.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Reviewed Evidence

- Phase 17H live invocation:
  `docs/evidence/phase17h-mistral-third-live-invocation-summary-20260524.md`
- Phase 17I local hardening:
  `docs/evidence/phase17i-mistral-root-wrapper-hardening-20260524.md`
- Managed AI adapter:
  `energy_market/managed_ai.py`
- Local adapter proof:
  `scripts/check_phase17a_managed_ai_adapter.py`

## Red-Green State

Red:

- Phase 17H failed because the model returned an exact one-key
  `ai_insight_v1` wrapper instead of placing `schema_version`, `generated_at`,
  and `insights` at the JSON root.

Green:

- Phase 17I locally normalizes only exact one-key `ai_insight_v1` and
  `ai_insight` wrappers when the nested object declares
  `schema_version: ai_insight_v1`.

Regression:

- Broad wrapper shapes remain rejected.
- Incomplete fenced JSON and truncated JSON remain rejected.
- Deterministic fallback remains intact.
- Dashboard publish remains blocked unless a later state explicitly approves
  publication.

## Read-Only Checks

- Local adapter proof passed:
  `.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py`
- Bedrock model lookup confirmed `mistral.ministral-3-8b-instruct` in
  `eu-west-2`.

## Cost And Risk

The next one-call estimate uses the Phase 17H prompt-token count as the
baseline and assumes roughly 500 output tokens:

- Estimated input tokens: `5127`
- Estimated output tokens: `500`
- Europe London price assumption: `$0.23` per 1M input tokens and `$0.23` per
  1M output tokens
- Estimated single-call cost: `$0.001294210`
- Hard budget cap remains: `$0.10`

Risk is acceptable for a fourth controlled invocation because the last failure
mode has a local proof. The remaining risk is that the model may choose another
schema-invalid shape. That risk is contained by the existing parser, schema
validation, no-publish guardrail, no-retry rule, and sanitized evidence
boundary.

## Decision

Recommendation: **GO candidate, pending explicit approval**.

Do not invoke Bedrock as part of this preflight state. A separate Phase 17J
execution step may perform one controlled fourth live Mistral invocation only
after explicit approval.

If approved, keep the execution boundary:

- one live invocation only
- no retry unless separately approved after reviewing the result
- no dashboard publish
- no Terraform apply
- no IAM, schedule, DNS, ACM, alarm, budget, or hosting change
- no raw prompt or raw model output committed
- sanitized metadata and red-green evidence only

## Rollback

Rollback is documentation-only:

1. Revert the Phase 17J preflight decision commit.
2. Keep Phase 17I local hardening intact.
3. Do not invoke Bedrock until a new explicit approval is given.
