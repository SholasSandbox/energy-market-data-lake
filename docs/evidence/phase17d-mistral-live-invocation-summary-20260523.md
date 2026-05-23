# Phase 17D Live Mistral Invocation Summary

Date: 2026-05-23

## Boundary

One live Bedrock Runtime invocation was performed against Mistral Ministral
8B. No retry, Terraform apply, IAM change, state-machine deployment,
EventBridge schedule enablement, DNS, ACM, alarms, budgets, dashboard
hosting change, or dashboard publish was performed.

## Invocation

- Model: `mistral.ministral-3-8b-instruct`
- Region: `eu-west-2`
- Max output tokens: `800`
- Temperature: `0.1`
- Budget cap: `$0.10`
- Manual invocation count: `1`
- Manual retries: `0`
- Prompt text was not written to evidence.
- Raw model response was not written to evidence.

## Result

- Status: `failed`
- The response was rejected by `ai_insight_v1` validation.
- Sanitized validation reason: root object contained an unexpected
  `ai_insight` wrapper instead of matching the schema directly.
- Output did not reach validated evidence.
- Failure details are limited to sanitized metadata.
- Metadata evidence: `docs/evidence/phase17d-mistral-live-invocation-metadata-20260523.json`

## Decision

Do not retry live invocation in this state. The next safe boundary is local
prompt/response-shape hardening with fake-client proof before any second paid
call.

## Usage

- Prompt tokens: `5025`
- Completion tokens: `484`
- Total tokens: `5509`
- Estimated invocation cost: `$0.001267`

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed. To abandon this proof, remove only the
Phase 17D evidence files from the branch.
