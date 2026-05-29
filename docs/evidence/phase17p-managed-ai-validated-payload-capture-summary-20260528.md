# Phase 17P Managed AI Validated Payload Capture Summary

Date: 2026-05-28

## Boundary

One controlled live Bedrock Runtime invocation was performed against Mistral
Ministral 8B to capture a public-safe validated `ai_insight_v1` evidence
payload after Phase 17O publish/deployment preflight.

No retry, Terraform apply, IAM change, state-machine deploy, EventBridge
schedule enablement, DNS, ACM, alarm, budget, dashboard hosting change,
dashboard publish, raw prompt commit, or raw model-response commit was
performed.

## Red-Green Evidence

Red:

- Phase 17N proved Mistral could produce schema-valid `ai_insight_v1` in memory,
  but did not commit the parsed payload because that phase approved sanitized
  metadata only.
- Phase 17O held dashboard publish and managed workflow deployment as no-go
  decisions until a public-safe validated payload was captured separately.

Green:

- Phase 17P produced a parsed `ai_insight_v1` payload that passed schema
  validation.
- The parsed payload was committed as public-safe evidence.
- A private lake S3 URI in the model output was replaced with a public-safe
  curated dataset reference before committing the payload.
- Metadata records both the original model payload hash and the sanitized
  committed payload hash.
- The public dashboard snapshot was not changed.

Regression:

- Local managed AI adapter proof still passes.
- Deterministic fallback remains unchanged.
- Dashboard publish remains blocked.
- Handler/state-machine deployment remains blocked.

## Invocation

- Model: `mistral.ministral-3-8b-instruct`
- Region: `eu-west-2`
- Max output tokens: `1600`
- Temperature: `0.1`
- Budget cap: `$0.10`
- Manual invocation count: `1`
- Manual retries: `0`
- Prompt text was not written to evidence.
- Raw model response was not written to evidence.

## Result

- Status: `validation_passed`
- Root schema version parsed as `ai_insight_v1`.
- Public-safe payload evidence:
  `docs/evidence/phase17p-managed-ai-validated-ai-insight-20260528.json`
- Metadata evidence:
  `docs/evidence/phase17p-managed-ai-validated-payload-capture-metadata-20260528.json`
- Public dashboard snapshot unchanged.
- Raw prompt and raw response not committed.
- Handler/state-machine wiring unchanged.

## Usage

- Prompt tokens: `5260`
- Completion tokens: `636`
- Total tokens: `5896`
- Estimated invocation cost: `$0.00135608`

The estimate uses the Amazon Bedrock Europe London on-demand Mistral pricing
for Ministral 8B 3.0 at `$0.23` per 1M input tokens and `$0.23` per 1M output
tokens.

## Decision

Do not publish in this state. Phase 17P proves that a public-safe, schema-valid
managed AI payload can be captured as evidence.

The likely next state is **Phase 17Q: managed AI dashboard publish preflight**:

- decide whether the validated evidence payload should be converted into a
  dashboard snapshot
- verify rollback for the current live `dashboard_snapshot_v1.json`
- decide whether CloudFront invalidation is required
- keep handler/state-machine deployment separate
- keep Terraform, IAM, schedules, DNS, ACM, alarms, and budgets unchanged unless
  a future phase explicitly targets them

## Rollback

No AWS resource rollback is required because no deployed resources or public
dashboard objects were changed.

To abandon this proof, remove only the Phase 17P evidence and documentation
changes from the branch. Do not publish managed output unless a later state
explicitly approves it.
