# Phase 17E Mistral Response-Shape Hardening Evidence

Date: 2026-05-23

## Boundary

Phase 17E was a local-only hardening slice after the Phase 17D live Mistral
invocation returned a public-safe but schema-invalid wrapper shape.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, or raw model-output commit was performed.

## Change

- Tightened the managed AI prompt so the JSON root must be the `ai_insight_v1`
  object itself.
- Explicitly instructed the model not to wrap the payload in `ai_insight`,
  `result`, `output`, `response`, `data`, or any other key.
- Added narrow parser support for the exact observed one-key `ai_insight`
  wrapper shape.
- Kept broad wrappers unsafe: if any sibling key is present beside
  `ai_insight`, the object is not unwrapped and the existing schema validation
  gate rejects it.
- Expanded the fake-client adapter proof to cover direct Mistral output, the
  observed wrapper shape, and unsafe wrapper rejection.

## Verification

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
.venv/bin/python scripts/check_phase8_handlers.py
.venv/bin/python scripts/check_phase8_runtime.py
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
.venv/bin/python -m compileall energy_market scripts lambda
npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/phase-17b-controlled-bedrock-invocation-preflight.md \
  docs/evidence/phase17e-mistral-response-shape-hardening-20260523.md
git diff --check
```

## Result

- Local managed AI adapter proof passed.
- Exact one-key `ai_insight` wrapper is normalized before validation.
- Unsafe wrapper output remains rejected by `ai_insight_v1` validation.
- Deterministic fallback remains unchanged.

## Next Boundary

Phase 17F may perform one controlled second live Mistral invocation only after
explicit approval, using the same hard one-run budget cap and no retries unless
separately approved.

## Rollback

Rollback is code-only:

1. Revert the Phase 17E commit.
2. Re-run the local adapter proof.
3. Do not run another live model invocation until the parser and prompt
   contract are locally proven again.
