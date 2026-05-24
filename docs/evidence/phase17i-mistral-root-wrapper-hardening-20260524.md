# Phase 17I Mistral Root-Wrapper Hardening Evidence

Date: 2026-05-24

## Boundary

Phase 17I was a local-only hardening slice after Phase 17H showed Mistral could
complete output with `finish_reason=stop` but still return a schema-invalid
root wrapper named `ai_insight_v1`.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- An exact one-key `ai_insight_v1` wrapper should normalize locally when the
  nested object itself declares `schema_version: ai_insight_v1`.
- Any `ai_insight_v1` wrapper with sibling keys must remain rejected by schema
  validation.

Green:

- Exact `ai_insight_v1` wrappers now normalize before schema validation.
- Broad or unsafe `ai_insight_v1` wrappers still fail validation.
- The prompt explicitly tells Mistral not to use `ai_insight_v1` or
  `ai_insight` wrapper keys.
- Local fake-client proof covers direct and Mistral response forms.

Regression:

- Existing exact `ai_insight` wrapper handling remains intact.
- Complete fenced JSON handling remains intact.
- Incomplete fenced JSON and truncated JSON remain rejected with sanitized
  parser errors.
- Deterministic fallback remains unchanged.

## Verification

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python -m compileall energy_market lambda scripts
```

## Decision

Do not run a fourth live Mistral invocation in this state. Phase 17I locally
proves the exact root-wrapper behavior observed in Phase 17H. Any future live
call must remain behind explicit approval, one-call discipline, no retry, no
dashboard publish, and sanitized evidence only.

## Rollback

Rollback is code-only:

1. Revert the Phase 17I commit.
2. Re-run the local managed AI adapter proof.
3. Do not perform another live invocation until wrapper behavior is locally
   proven again.
