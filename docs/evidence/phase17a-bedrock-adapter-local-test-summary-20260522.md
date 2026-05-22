# Phase 17A Bedrock Adapter Local Test Summary

Date: 2026-05-22

## Boundary

Phase 17A proves the managed AI provider boundary in local code only.

No Terraform apply, IAM change, live Bedrock invocation, EventBridge schedule
enablement, DNS, ACM, alarms, budgets, or dashboard hosting changes were made.

## Implemented

- Added `energy_market/managed_ai.py` for Bedrock Runtime request construction
  and response parsing.
- Added `MergeAiInsightManaged` beside `MergeAiInsightDeterministic`.
- Kept deterministic merge as the rollback and comparison path.
- Added `scripts/check_phase17a_managed_ai_adapter.py` with a fake Bedrock
  client.

## Proof

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
```

Result:

```text
Phase 17A managed AI adapter self-check passed for ai-insight-20260522T103000Z-17a00001
```

The check confirms:

- the prompt asks for JSON matching `ai_insight_v1`
- token and temperature settings are passed to the Bedrock request
- a fake Bedrock response is parsed into `ai_insight_v1`
- managed output is written to the normal `ai_insight` artifact key
- invalid managed output is rejected and quarantined in the failed path

## Next Boundary

Phase 17B should decide whether to perform one controlled live Bedrock
invocation. That decision needs an explicit model choice, token budget, IAM
delta review, rollback path, and no schedule enablement.
