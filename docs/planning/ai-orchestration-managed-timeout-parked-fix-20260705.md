# AI Orchestration Managed Timeout Parked Fix

Date: 2026-07-05<br>
Revalidated and implemented: 2026-09-01

## Status

Implemented and verified.

The original parking gate was superseded by explicit approval on 2026-09-01
for the bounded AWS reliability change. The implementation did not redrive an
old execution, change the schedule, increase the Lambda timeout, add fallback,
or expand the AI workflow.

This note captures the intended fix so the alert has a landing place without
pulling focus away from Security Tooling alternate contacts, delegated-admin
migration sequencing, and the scheduled governance implementation work.

## Problem

The scheduled managed AI workflow is failing in `MergeAiInsightManaged`.

The 2026-07-05 and 2026-07-04 scheduled executions reached the managed Bedrock
merge state, then Lambda hit its 120-second sandbox timeout before the handler
could write an `ai_insight` artifact, publish a dashboard snapshot, or record a
normal failed-zone payload.

The workflow is not currently failing in:

- S3 artifact reads or writes before the managed merge
- dashboard input export
- news ingestion
- bundle creation
- schema validation before the managed merge
- dashboard publishing

The likely failure boundary is the `bedrock-runtime.invoke_model` call to the
configured Mistral model.

Primary evidence:

- `docs/evidence/ai-orchestration-managed-timeout-diagnosis-20260705.md`

## Accepted Parked Solution

Add a deadline-aware, fail-fast managed AI merge path.

The implementation should make the Bedrock call time out before Lambda's
sandbox timeout, preserve a structured failure record, and keep the dashboard
on the last known-good snapshot when managed AI does not return in time.

Proposed behavior:

1. Configure the Bedrock Runtime client with explicit `connect_timeout`,
   `read_timeout`, and bounded retry settings.
2. Set the Bedrock read timeout materially below the Lambda timeout; for the
   current 120-second Lambda, start with a 60-75 second read timeout and leave
   enough time for failed-record S3 writes.
3. Catch Bedrock timeout/client timeout errors inside `MergeAiInsightManaged`.
4. Raise or map the timeout to a structured managed-AI failure before the
   sandbox kills the runtime.
5. Write a failed-zone record for the run.
6. Do not write `ai_insight`.
7. Do not publish `dashboard_snapshot_v1.json`.
8. Leave the latest public dashboard snapshot unchanged.

Suggested configuration names:

- `BEDROCK_CONNECT_TIMEOUT_SECONDS`
- `BEDROCK_READ_TIMEOUT_SECONDS`
- `BEDROCK_MAX_ATTEMPTS`

Suggested defaults:

- connect timeout: 5 seconds
- read timeout: 60 seconds
- max attempts: 1 or 2

## 2026-09-01 Revalidation

The historical diagnosis was accurate, but the issue was no longer an active
outage when the fix was reopened:

- the only `Sandbox.Timedout` workflow failures remained 2026-07-04 and
  2026-07-05;
- the 2026-08-08 and 2026-08-21 failures were strict-schema rejections for an
  unexpected model-added reference field, not timeouts;
- the 30 most recent executions inspected included successful daily runs
  through 2026-09-01; and
- the deployed Lambda still used the default Bedrock Runtime client
  configuration, so a future slow invocation could still consume the complete
  120-second Lambda window and bypass failed-record persistence.

The fix therefore remained valid as preventive resilience hardening. The
revised implementation uses:

- connect timeout: 5 seconds;
- read timeout: 60 seconds;
- total request attempts: 1, including the initial request; and
- a Terraform precondition requiring at least 30 seconds between the Bedrock
  read timeout and the Lambda timeout.

Botocore connect/read timeout exceptions are mapped to a named
`ManagedAITimeoutError`. The existing top-level handler then writes the normal
`merge_ai_insight_managed` failed-zone record before re-raising so Step
Functions retains its existing failure and SNS notification behaviour.

## Explicit Non-Goals

Do not use this parked fix to expand the AI workflow.

Out of scope until the tracker is updated or the parked item is reopened:

- prompt redesign
- model/provider exploration
- new AI orchestration states
- richer dashboard AI features
- automatic retries that may create repeated model calls
- silent fallback publication without a visible mode marker
- CloudFront invalidation or dashboard cache changes

## Rejected Alternatives

| Alternative | Why rejected for the parked fix |
|---|---|
| Increase Lambda timeout | Hides the failure boundary, increases worst-case cost, and still may not produce a structured failure before termination. |
| Redrive failed Step Functions executions | Could re-enter managed Bedrock and publish a new dashboard snapshot without a deliberate change boundary. |
| Immediate manual retry | Same risk as redrive and does not address repeated scheduled failures. |
| Switch model/provider now | Turns a reliability fix into new AI exploration, which conflicts with the tracker parking lot. |
| Publish deterministic fallback by default | Avoids the alert but silently changes dashboard provenance unless an explicit fallback policy and source label are added. |
| Disable the schedule in this note | That is a live AWS operating change and should only happen under explicit approval if repeated failure notifications become noisy. |

## Implementation Checklist

When the governance tracker tasks are complete and this item is reopened:

- [x] Add local tests with a fake Bedrock client that raises a timeout before
      returning a model response.
- [x] Prove that the handler writes a failed-zone record for managed AI
      timeout.
- [x] Prove that no `ai_insight` artifact is written on managed timeout.
- [x] Prove that no dashboard artifact is written after managed
      timeout.
- [x] Prove that deterministic merge behavior remains unchanged.
- [x] Add explicit Bedrock client timeout configuration in the Lambda runtime.
- [x] Rebuild the Lambda package locally.
- [x] Run a Terraform plan only after the package is rebuilt.
- [x] Apply only under explicit approval.
- [x] Run one controlled smoke execution after approval.
- [x] Keep schedule behavior unchanged as a separate operating decision.

Implementation and live validation evidence:
`docs/evidence/ai-orchestration-managed-timeout-fix-20260901.md`.

## Optional Later Policy

After the fail-fast path is proven, a separate decision may consider an
explicit deterministic fallback policy:

- disabled by default
- enabled only through a named environment variable
- writes clear provenance into the workflow summary
- labels dashboard insight source as deterministic fallback
- preserves the managed-AI failed-zone record for the same run

That policy is not part of the parked fix unless separately approved.

## Superseded Parking Gate

The original gate kept this item parked until the governance work completed or
the user explicitly superseded it. The user granted that explicit bounded AWS
authorization on 2026-09-01. This did not release unrelated parked work:

- configure `SECURITY`, `OPERATIONS`, and `BILLING` alternate contacts for
  `Security Tooling`
- migrate delegated-admin functions in order: AWS Config first, GuardDuty
  next, and Security Hub only if later adopted
- continue the scheduled governance phase through explicitly approved
  implementation changes

## Tracker Mapping

This parked solution supports maintenance of an already-proven lakehouse
workflow and maps to SAP-C02 Domain 2 resilience and Domain 3 continuous
improvement. It remains behind the Domain 1 governance workstream and does not
authorize new AI orchestration expansion.
