# Phase 17B Bedrock Live Invocation Preflight Read-Only Evidence

Date: 2026-05-23

## Boundary

This evidence is read-only. No `InvokeModel`, Terraform apply, IAM change,
state-machine deployment, EventBridge schedule enablement, DNS, ACM,
CloudWatch alarm, budget, or dashboard hosting change was performed.

## Git State

```text
## docs/phase17b-bedrock-live-invocation-preflight
```

## AWS Identity And Region

```json
{
  "account": "464975959576",
  "arn": "arn:aws:iam::464975959576:user/IAMUser1",
  "region": "eu-west-2"
}
```

## EventBridge Schedule

```json
{
  "name": "energy-market-ai-orchestration-schedule",
  "state": "DISABLED",
  "schedule": "cron(30 7 * * ? *)"
}
```

## Candidate Model Discovery

`aws bedrock list-foundation-models --region eu-west-2` returned these relevant
on-demand text models:

```text
anthropic.claude-3-haiku-20240307-v1:0
mistral.ministral-3-8b-instruct
```

`aws bedrock get-foundation-model` confirmed both models support on-demand text
output in `eu-west-2`.

## Access Availability Check

Claude 3 Haiku:

```json
{
  "modelId": "anthropic.claude-3-haiku-20240307-v1",
  "agreementAvailability": {
    "status": "NOT_AVAILABLE"
  },
  "authorizationStatus": "AUTHORIZED",
  "entitlementAvailability": "AVAILABLE",
  "regionAvailability": "AVAILABLE"
}
```

Mistral Ministral 8B:

```json
{
  "modelId": "mistral.ministral-3-8b-instruct",
  "agreementAvailability": {
    "status": "AVAILABLE"
  },
  "authorizationStatus": "AUTHORIZED",
  "entitlementAvailability": "AVAILABLE",
  "regionAvailability": "AVAILABLE"
}
```

## Prompt And Cost Envelope

Local prompt estimate from `build_ai_insight_prompt` using
`docs/evidence/ai/ai_input_bundle_v1.sample.json`:

```text
prompt_chars=16368
estimated_input_tokens_chars_div_4=4092
planned_output_token_cap=800
```

Estimated one-run costs:

```text
Claude 3 Haiku historical public assumption: $0.002023
Claude 3.5 Sonnet public extended access ceiling: $0.048552
Mistral Ministral 8B Europe London official: $0.001125
```

The practical live-invocation budget cap should be **$0.10 maximum** for one
manual run, with no retries unless explicitly approved.

## IAM Delta

The current AI orchestration Lambda inline policy includes S3 read/write
permissions for the lake and dashboard buckets. It does not include Bedrock
runtime invocation.

Minimum future IAM delta for one chosen model:

```json
{
  "Effect": "Allow",
  "Action": "bedrock:InvokeModel",
  "Resource": "arn:aws:bedrock:eu-west-2::foundation-model/<chosen-model-id>"
}
```

## Decision

Phase 17B should remain **preflight-only**.

Reasons:

- Claude 3 Haiku matches the current Anthropic-compatible adapter but agreement
  availability is `NOT_AVAILABLE`.
- Mistral Ministral 8B is available and cheaper in `eu-west-2`, but the current
  adapter request body is Anthropic-compatible and should not be used against a
  Mistral model without a provider-specific local proof.
- The safest next state is not a live invoke. It is a provider compatibility
  decision: either complete the Anthropic model-access prerequisite, or add a
  Mistral request/response adapter behind fake-client tests before a single
  live invocation.
