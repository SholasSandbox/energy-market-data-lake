# Phase 17K Mistral Schema-Field Hardening Evidence

Date: 2026-05-26

## Boundary

Phase 17K was a local-only hardening slice after Phase 17J execution showed
root-wrapper normalization worked live, but nested `insights[0]` fields still
failed `ai_insight_v1` validation.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Phase 17J parsed to `schema_version: ai_insight_v1`, but validation rejected
  the nested insight object.
- The rejected output used a generic `references` field, omitted required
  insight fields, and returned `validation_notes` as a string instead of an
  array.

Green:

- The prompt now explicitly lists the required fields for each insight.
- The prompt rejects generic `references` as a substitute for
  `energy_references` and `news_references`.
- The prompt requires `validation_notes` as an array of strings.
- Local fake-client proof reproduces the Phase 17J nested-field failure shape
  and confirms schema validation still rejects it.

Regression:

- Root-wrapper normalization remains intact.
- Broad wrapper rejection remains intact.
- Complete fenced JSON handling remains intact.
- Deterministic fallback remains unchanged.

## Verification

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python -m compileall energy_market lambda scripts
```

## Decision

Do not run a fifth live Mistral invocation in this state. Phase 17K locally
proves the nested schema-field failure mode and tightens the prompt contract.
Any future live call must remain behind explicit approval, one-call discipline,
no retry, no dashboard publish, and sanitized evidence only.

## Rollback

Rollback is code-only:

1. Revert the Phase 17K commit.
2. Re-run the local managed AI adapter proof.
3. Do not perform another live invocation until nested schema-field behavior is
   locally proven again.
