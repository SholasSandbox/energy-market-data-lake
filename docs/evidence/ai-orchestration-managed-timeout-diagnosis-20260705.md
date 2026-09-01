# AI Orchestration Managed Timeout Diagnosis

Date: 2026-07-05

## 2026-09-01 Outcome

The historical diagnosis below was revalidated. The July failures remained the
only Lambda sandbox timeouts, so this was no longer an active outage, but the
deployed client still lacked a deadline below the Lambda timeout. Under an
explicit maintenance authorization, the parked fix was revised, deployed as
one in-place Lambda update, and verified with one successful controlled smoke.

Current evidence and the exact boundary are recorded in
`docs/evidence/ai-orchestration-managed-timeout-fix-20260901.md`. The remainder
of this document is retained as the historical incident diagnosis.

## Scope

Read-only diagnosis of the scheduled Energy Market AI insight orchestration
failure reported by SNS after the 2026-07-05 07:30 UTC EventBridge run.

Guardrails:

- no AWS deploy
- no Terraform plan or apply
- no Step Functions redrive
- no manual retry
- no S3 object writes
- no CloudFront invalidation

This is maintenance and observation of the existing lakehouse AI workflow, not
new AI orchestration expansion.

## Finding

The scheduled Step Functions execution failed in `MergeAiInsightManaged`.

The earlier states completed:

- `InitializeRun`
- `ExportEnergyInput`
- `IngestNewsSummary`
- `CreateAiInputBundle`

The managed merge state then invoked the Lambda and timed out after exactly
120 seconds:

- execution status: `FAILED`
- failed state: `MergeAiInsightManaged`
- Lambda error: `Sandbox.Timedout`
- timeout request ID: `3d9b94cd-e93b-4949-bc6c-3b6fe31558c6`
- Step Functions failure wrapper: `AIInsightOrchestrationFailed`

## Impact

Artifacts created for the 2026-07-05 run:

- `energy_input`
- `news_summary`
- `ai_input_bundle`

Artifacts not created for the 2026-07-05 run:

- `ai_insight`
- immutable dashboard snapshot

The latest dashboard snapshot was not updated by the failed run. The latest
public snapshot object remained the 2026-07-03 snapshot:

- latest snapshot `LastModified`: `2026-07-03T07:30:22Z`
- latest snapshot `generated_at`: `2026-07-03T07:30:21Z`

No new failed-zone S3 record was found for this timeout. That is expected for a
Lambda sandbox timeout because the runtime is stopped before the handler can
write its normal failed-record payload.

## Recurrence

Recent scheduled executions:

- 2026-07-05 07:30 UTC: `FAILED`
- 2026-07-04 07:30 UTC: `FAILED`
- 2026-07-03 07:30 UTC: `SUCCEEDED`
- 2026-07-02 07:30 UTC: `SUCCEEDED`
- 2026-07-01 07:30 UTC: `SUCCEEDED`

The 2026-07-04 failure had the same boundary:

- failed state: `MergeAiInsightManaged`
- Lambda error: `Sandbox.Timedout`
- timeout request ID: `2e366aff-138e-4126-a834-33107d3af0ed`

## Live Configuration Observed

Lambda configuration:

- function: `energy-market-news-ai-orchestration`
- runtime: `python3.11`
- state: `Active`
- timeout: `120`
- memory: `512 MB`
- mode: `managed`
- Bedrock provider: `mistral`
- Bedrock model ID: `mistral.ministral-3-8b-instruct`
- Bedrock max tokens: `1600`
- Bedrock temperature: `0.2`

The 2026-07-05 `ai_input_bundle` object size was 16,022 bytes. The successful
2026-07-03 input bundle was similar at 16,174 bytes, so bundle size alone does
not explain the new timeout pattern.

## Likely Cause

The workflow is not failing in S3 reads, schema validation, news ingestion, or
dashboard publishing. It is hanging long enough inside the managed Bedrock
merge path that Lambda reaches its configured 120-second timeout.

The most likely boundary is the `bedrock-runtime.invoke_model` call inside
`MergeAiInsightManaged`. The local Lambda code creates the Bedrock Runtime
client with the default boto3 client configuration, so there is no shorter
application-level read timeout, deadline-aware fallback, or deterministic merge
fallback before the Lambda sandbox timeout.

## Recommended Next Action

Do not redrive or manually retry without an explicit approval boundary. A
redrive would re-enter the managed Bedrock path and could publish a new
dashboard snapshot if the model call later succeeds.

Recommended fix path:

1. Keep the existing latest dashboard snapshot as the safe public state.
2. Treat the schedule as producing repeated failed notifications until the
   managed merge timeout behavior is changed or the schedule is paused.
3. Add a small reliability patch that gives the Bedrock client a shorter read
   timeout than the Lambda timeout, records a structured failure, and either
   fails fast or falls back to `MergeAiInsightDeterministic` under an explicit
   configured policy.
4. Validate locally with fake Bedrock timeout tests before any package refresh,
   Terraform apply, schedule change, or Step Functions redrive.

The parked solution is recorded in
`docs/planning/ai-orchestration-managed-timeout-parked-fix-20260705.md`.

Do not implement it until the current Domain 1 governance tracker tasks are
complete, unless the tracker is explicitly updated or a short maintenance
exception is approved.

## Tracker Mapping

This supports the SAP-C02 tracker through operational maintenance of the
already-proven lakehouse AI workflow, with Domain 2 resilience and Domain 3
continuous improvement evidence. It does not expand AI orchestration scope.
