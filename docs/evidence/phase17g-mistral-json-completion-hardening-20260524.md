# Phase 17G Mistral JSON-Completion Hardening Evidence

Date: 2026-05-24

## Boundary

Phase 17G was a local-only hardening slice after Phase 17F showed the second
live Mistral invocation stopped with `finish_reason=length` and incomplete
fenced JSON.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- A managed AI response that starts a markdown fence but never closes it must
  fail locally with a sanitized parser error.
- A managed AI response that starts JSON but appears truncated must fail
  locally with a sanitized parser error.
- The default `800` output-token cap is treated as too low for the observed
  Phase 17F schema attempt.

Green:

- The prompt now explicitly instructs Mistral to avoid fences, keep the payload
  concise, return complete JSON, and shorten prose rather than truncate JSON.
- The managed AI default output-token cap is raised to `1600`.
- The Lambda managed path uses the shared managed-AI default when
  `BEDROCK_MAX_TOKENS` is not set.
- Local fake-client proof covers complete fenced JSON, incomplete fenced JSON,
  truncated JSON, exact wrapper handling, unsafe wrapper rejection, and
  managed-handler success/failure paths.

Regression:

- Complete fenced JSON is still accepted because providers may return harmless
  fences despite prompt instructions.
- Unsafe wrappers still fail validation.
- Deterministic fallback remains unchanged.

## Verification

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python -m compileall energy_market lambda scripts
```

## Decision

Do not run a third live Mistral invocation in this state. Phase 17G locally
proves the parser and prompt hardening needed after Phase 17F. Any future live
call should use the raised `1600` output-token cap and must remain behind
explicit approval, one-call discipline, no retry, and sanitized evidence only.

## Rollback

Rollback is code-only:

1. Revert the Phase 17G commit.
2. Re-run the local managed AI adapter proof.
3. Do not perform another live invocation until prompt, parser, and token
   budget behavior are locally proven again.
