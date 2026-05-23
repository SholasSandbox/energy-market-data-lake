# Phase 17C Mistral Compatibility Proof

Date: 2026-05-23

## Boundary

Phase 17C proves Mistral compatibility locally before any paid or deployed
change.

No live Bedrock invocation, Terraform apply, IAM change, state-machine deploy,
EventBridge schedule enablement, DNS, ACM, CloudWatch alarm, budget, or
dashboard hosting change was performed.

## Source Reference

AWS Bedrock Mistral chat-completion documentation describes a request with
`messages`, `max_tokens`, `top_p`, and `temperature`, and a response body with
`choices[].message.content`.

Reference:
<https://docs.aws.amazon.com/bedrock/latest/userguide/model-parameters-mistral-chat-completion.html>

## Implemented

- Added provider-aware request construction in `energy_market/managed_ai.py`.
- Kept the Anthropic-compatible request path intact.
- Added Mistral chat-completion request support for
  `mistral.ministral-3-8b-instruct`.
- Added Mistral `choices[].message.content` response parsing.
- Expanded `scripts/check_phase17a_managed_ai_adapter.py` to prove both
  Anthropic and Mistral fake-client paths.
- Preserved `MergeAiInsightDeterministic` as fallback.

## Proof

```bash
.venv/bin/python scripts/check_phase17a_managed_ai_adapter.py
```

Result:

```text
Phase 17 managed AI adapter self-check passed for ai-insight-20260522T103000Z-17a00001
```

The check now confirms:

- Anthropic request formatting still works.
- Mistral request formatting does not include Anthropic-only fields.
- Mistral message content is a string as expected by the chat-completion API.
- Mistral `choices[].message.content` responses parse to `ai_insight_v1`.
- Invalid managed output is rejected and written to the failed path.

## Next Boundary

Phase 17D may perform one controlled live Mistral invocation only after
explicit approval.

Guardrails for Phase 17D:

- one manual invocation only
- hard `$0.10` budget cap
- no retries unless explicitly approved
- no EventBridge schedule enablement
- no DNS, ACM, alarms, budgets, or dashboard hosting changes
- previous public dashboard snapshot remains untouched unless validation passes
