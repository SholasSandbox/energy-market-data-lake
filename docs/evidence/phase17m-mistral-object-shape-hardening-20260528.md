# Phase 17M Mistral Object-Shape Hardening Evidence

Date: 2026-05-28

## Boundary

Phase 17M was a local-only hardening slice after Phase 17L execution showed
managed output could parse as `ai_insight_v1`, but still failed nested
object-shape validation.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Phase 17L parsed to `schema_version: ai_insight_v1`, but validation rejected
  nested object shapes.
- `time_window` was returned as a plain string instead of an object with
  `start` and `end`.
- `energy_references` objects included an unexpected `value` field.

Green:

- The prompt now explicitly requires `time_window` as an object with `start`
  and `end` date-time strings.
- The prompt explicitly rejects string `time_window` values.
- The prompt forbids `value`, `date`, `timestamp`, and other extra fields in
  references.
- Local fake-client proof reproduces the Phase 17L object-shape failure and
  confirms schema validation still rejects it.

Regression:

- Phase 17J missing-field rejection remains covered.
- Root-wrapper normalization remains intact.
- Broad wrapper rejection remains intact.
- Complete fenced JSON handling remains intact.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked.

## Verification

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python -m compileall energy_market lambda scripts
```

## Decision

Do not run a sixth live Mistral invocation in this state. Phase 17M locally
proves the remaining Phase 17L object-shape failure mode and tightens the
prompt contract.

Any future live call must remain behind explicit approval, one-call discipline,
no retry, no dashboard publish, and sanitized evidence only.

## Rollback

Rollback is code-only:

1. Revert the Phase 17M commit.
2. Re-run the local managed AI adapter proof.
3. Do not perform another live invocation until object-shape behavior is
   locally proven again.
