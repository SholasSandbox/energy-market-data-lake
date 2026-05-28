# Phase 17N Live Mistral Preflight Decision Evidence

Date: 2026-05-28

## Boundary

Phase 17N is a preflight decision state after Phase 17M local Mistral
object-shape hardening.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, alarm, budget, dashboard hosting
change, dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Current Evidence Reviewed

- Phase 17L fifth live invocation:
  `docs/evidence/phase17l-mistral-fifth-live-invocation-summary-20260527.md`
- Phase 17L sanitized metadata:
  `docs/evidence/phase17l-mistral-fifth-live-invocation-metadata-20260527.json`
- Phase 17M local object-shape hardening:
  `docs/evidence/phase17m-mistral-object-shape-hardening-20260528.md`

## Live-Call Count and Budget

Controlled Mistral live calls so far:

| Phase | Status | Estimated cost | Retries | Dashboard publish |
| --- | --- | ---: | ---: | --- |
| 17D | failed | `$0.00126700` | `0` | `false` |
| 17F | parse_failed | `$0.00135217` | `0` | `false` |
| 17H | validation_failed | `$0.00126615` | `0` | `false` |
| 17J | validation_failed | `$0.00127788` | `0` | `false` |
| 17L | validation_failed | `$0.00134251` | `0` | `false` |

Total estimated live Mistral cost so far: `$0.00650571`.

A sixth controlled invocation remains low cost if it keeps the same discipline:
one call only, no retry, `mistral.ministral-3-8b-instruct`, `eu-west-2`, hard
budget cap `$0.10`, and sanitized metadata only.

## Red-Green State

Red:

- Phase 17L parsed to `schema_version: ai_insight_v1`, but validation rejected
  nested object shapes.
- The observed failure categories were string `time_window` and unexpected
  `value` fields in `energy_references`.

Green:

- Phase 17M requires `time_window` as an object with `start` and `end`
  date-time strings.
- Phase 17M explicitly rejects string `time_window`.
- Phase 17M forbids extra reference fields such as `value`, `date`, and
  `timestamp`.
- Phase 17M fake-client proof reproduces the Phase 17L object-shape failure and
  keeps unsafe output rejected.

Regression:

- Local managed AI adapter proof passes.
- Phase 17J missing-field rejection remains covered.
- Root-wrapper normalization remains intact.
- Broad wrapper rejection remains intact.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked unless a later state explicitly approves
  publication of validated output.

## Decision

Recommendation: **go-candidate for one controlled sixth live Mistral
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
documentation updates from the branch. Do not run a sixth live invocation
without a later explicit approval boundary.

## Proof Commands

```bash
git status --short --branch
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
python3 -m json.tool \
  docs/evidence/phase17l-mistral-fifth-live-invocation-metadata-20260527.json
npx markdownlint-cli2 README.md PLANS.md \
  docs/phase-17-managed-ai-refresh-preflight.md \
  docs/evidence/phase17n-live-mistral-preflight-decision-20260528.md
git diff --check
```
