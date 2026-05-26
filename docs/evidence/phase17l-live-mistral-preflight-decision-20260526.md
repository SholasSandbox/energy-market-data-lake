# Phase 17L Live Mistral Preflight Decision Evidence

Date: 2026-05-26

## Boundary

Phase 17L is a preflight decision state after Phase 17K local Mistral
schema-field hardening.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Current Evidence Reviewed

- Phase 17J fourth live invocation:
  `docs/evidence/phase17j-mistral-fourth-live-invocation-summary-20260526.md`
- Phase 17J sanitized metadata:
  `docs/evidence/phase17j-mistral-fourth-live-invocation-metadata-20260526.json`
- Phase 17K local schema-field hardening:
  `docs/evidence/phase17k-mistral-schema-field-hardening-20260526.md`

## Live-Call Count and Budget

Controlled Mistral live calls so far:

| Phase | Status | Estimated cost | Retries | Dashboard publish |
| --- | --- | ---: | ---: | --- |
| 17D | failed | `$0.00126700` | `0` | `false` |
| 17F | parse_failed | `$0.00135217` | `0` | `false` |
| 17H | validation_failed | `$0.00126615` | `0` | `false` |
| 17J | validation_failed | `$0.00127788` | `0` | `false` |

Total estimated live Mistral cost so far: `$0.00516320`.

A fifth controlled invocation remains low cost if it keeps the same discipline:
one call only, no retry, `mistral.ministral-3-8b-instruct`, `eu-west-2`, hard
budget cap `$0.10`, and sanitized metadata only.

## Red-Green State

Red:

- Phase 17J proved root-wrapper normalization worked live, but the nested
  insight object still failed `ai_insight_v1` validation.
- The observed failure categories were generic `references`, missing required
  insight fields, and `validation_notes` as a string instead of an array.

Green:

- Phase 17K tightened the local prompt contract for exact insight fields.
- Phase 17K explicitly rejects generic `references` as a substitute for
  `energy_references` and `news_references`.
- Phase 17K requires `validation_notes` as an array of strings.
- Phase 17K fake-client proof reproduces the Phase 17J failure shape and keeps
  unsafe output rejected.

Regression:

- Local managed AI adapter proof passes.
- Root-wrapper normalization remains intact.
- Broad wrapper rejection remains intact.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked unless a later state explicitly approves
  publication of validated output.

## Decision

Recommendation: **go-candidate for one controlled fifth live Mistral
invocation**, but do not run it in this state.

The next execution substate may perform exactly one live invocation only after
explicit approval in-session. If approved, it should preserve:

- no retry
- no Terraform apply
- no IAM, Step Functions, EventBridge, DNS, ACM, alarm, budget, hosting, or
  dashboard publish change
- no raw prompt or raw model output in evidence
- sanitized metadata only
- deterministic fallback unchanged
- no public dashboard update unless validated output is explicitly approved in
  a later publish boundary

## Rollback

No AWS rollback is required because this state performs no live AWS mutation.

To abandon this preflight, remove only this evidence and the related
documentation updates from the branch. Do not run a fifth live invocation
without a later explicit approval boundary.

## Proof Commands

```bash
git status --short --branch
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
python3 -m json.tool docs/evidence/phase17j-mistral-fourth-live-invocation-metadata-20260526.json
npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17l-live-mistral-preflight-decision-20260526.md
git diff --check
```
