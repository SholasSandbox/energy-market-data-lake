# AI Orchestration Managed Timeout Fix

<!-- markdownlint-disable MD013 -->

Date: 2026-09-01

## Objective

Revalidate the parked July managed-AI timeout diagnosis and, if the reliability
gap still existed, implement the smallest AWS change that allowed the handler
to fail before the 120-second Lambda sandbox deadline and preserve a structured
failed-zone record.

## Authorization And Boundary

The repository owner explicitly authorized the AWS change for this issue.

In scope:

- read-only revalidation of the current Lambda, schedule, Step Functions
  history, artifacts, public surface, notifications, and budget;
- Bedrock SDK connect/read timeout and request-attempt configuration;
- local timeout and failure-path tests;
- one targeted Terraform Lambda update; and
- one controlled Step Functions smoke and post-change verification.

Out of scope:

- increasing the Lambda timeout;
- changing the Bedrock model, provider, prompt, or output token limit;
- deterministic fallback publication;
- Step Functions redesign or retry changes;
- schedule, IAM, SNS, budget, CloudFront, cache, DNS, or dashboard changes;
- redriving the July failures; and
- additional AI orchestration expansion.

## Revalidation Finding

The July diagnosis remained technically valid but no longer represented an
active outage.

- Four failed executions existed in the inspected history.
- The 2026-07-04 and 2026-07-05 executions failed with
  `Sandbox.Timedout` after 120 seconds in the managed merge.
- The 2026-08-08 and 2026-08-21 executions failed in the same state because
  strict schema validation rejected an unexpected model-added `value` field;
  they were not timeouts.
- The latest 30 executions were predominantly successful, including the
  scheduled 2026-09-01 run, which completed in under eight seconds.
- The deployed Lambda remained at 120 seconds and 512 MB, in managed Mistral
  mode, with no explicit Bedrock connect timeout, read timeout, or request
  attempt environment setting.
- The deployed handler and managed adapter matched the corresponding local
  sources before this patch. The only other packaged-source difference was a
  non-functional deterministic-fallback validation-note wording update.

The risk therefore remained: another slow Bedrock call could consume the
entire Lambda window and prevent normal failed-record persistence.

## Implemented Decision

The Lambda Bedrock Runtime client now uses:

| Control | Value | Reason |
|---|---:|---|
| Connect timeout | 5 seconds | Fail quickly when the service endpoint cannot be established. |
| Read timeout | 60 seconds | Bound the model response wait below the 120-second Lambda timeout. |
| Total attempts | 1 | Avoid an automatic second model call and repeated cost inside the same invocation. |
| Lambda reserve | At least 30 seconds | Preserve time for exception mapping and the failed-zone S3 write. |

`ReadTimeoutError` and `ConnectTimeoutError` are mapped to
`ManagedAITimeoutError`, with component `merge_ai_insight_managed` and contract
`ai_insight`. The existing handler failure boundary writes the structured
record and re-raises to the existing Step Functions catch/SNS path.

No deterministic fallback was enabled. On a managed timeout, the run writes no
`ai_insight` or dashboard artifact and the last-known-good public snapshot
remains in place.

## Local Validation

- Phase 8 runtime self-check: passed.
- Phase 8 handler self-check: passed.
- Phase 17 managed-AI adapter self-check: passed.
- A fake Bedrock `ReadTimeoutError` produced `ManagedAITimeoutError`.
- The timeout test found the expected `ai_insight_v1` failed-zone record.
- The timeout test found no `ai_insight` or dashboard artifact.
- Default client configuration was 5-second connect, 60-second read, and one
  total attempt; explicit environment overrides were also tested.
- Targeted Python compilation passed.
- Terraform formatting and configuration validation passed.
- The Lambda package rebuilt with pinned dependencies.

## Terraform Plan And Apply

The first full plan was rejected before apply because stale local variables
would have removed the SNS email subscription and changed budget notifications.
The existing private notification endpoints were then supplied to Terraform
without printing or recording them.

The corrected Lambda-targeted saved plan contained exactly one in-place
resource update:

- `aws_lambda_function.ai_orchestration[0]`;
- package hash update; and
- environment additions for connect timeout, read timeout, and total attempts.

The Lambda stayed at 120 seconds and 512 MB. No add or destroy was planned.
The saved plan was applied:

```text
Apply complete! Resources: 0 added, 1 changed, 0 destroyed.
```

Terraform reported the expected warning that a targeted apply requires a full
post-apply plan. The subsequent full plan found no real infrastructure
changes. Its only pending item was the unrelated state-only addition of the
empty `energy_specific_crawler_names = {}` output, which was not applied.

The pre-change deployed package was preserved locally in the ignored Terraform
build directory with its original SHA-256 as an immediate code rollback
artifact. It is not repository evidence or a committed binary.

## Controlled Smoke Result

One manual execution was started after the apply:

- execution name: `ai-timeout-fix-20260901T190144Z`;
- run ID: `ai-insight-20260901T190146Z-ff8a5793`;
- start: `2026-09-01T19:01:44Z`;
- stop: `2026-09-01T19:01:53Z`;
- status: `SUCCEEDED`; and
- terminal workflow status: `dashboard_snapshot_published`.

The run used Bedrock/Mistral and produced one insight from 18 news articles.
It wrote all four expected private Lakehouse artifacts, the latest approved
dashboard snapshot, and the immutable run-scoped snapshot.

## Post-Change Verification

- Lambda state: `Active`; last update: successful.
- Lambda timeout/memory: unchanged at 120 seconds and 512 MB.
- Timeout environment: connect 5, read 60, total attempts 1.
- EventBridge AI schedule: unchanged and enabled at `cron(30 7 * * ? *)`.
- Smoke execution: succeeded in approximately nine seconds.
- New run failed-zone record count: zero.
- AI insight, S3 latest snapshot, S3 immutable snapshot, CloudFront latest, and
  CloudFront immutable payloads all passed their repository JSON contracts.
- S3 latest and immutable snapshot SHA-256 values matched.
- CloudFront immutable snapshot matched S3 immediately.
- CloudFront latest still served the preceding cached snapshot, as expected
  without an invalidation; no cache change was made.
- The failure SNS subscription remained outside the apply and was preserved:
  one confirmed and zero pending subscriptions.
- The managed-workflow budget remained at USD 1 monthly; actual and forecast
  values were USD 0.002 and USD 0.092, and all three notifications remained
  `OK`.

## Rollback

If the new package causes a regression:

1. keep the schedule decision separate; disable it only if repeated failures
   require an operating stop;
2. set `ai_orchestration_lambda_package_path` temporarily to the ignored
   pre-timeout package in `.terraform/build/`;
3. plan with managed mode, schedule, CloudFront, SNS, and budget values
   preserved;
4. require a single Lambda in-place update with no add/delete; and
5. apply the saved rollback plan, then run one bounded verification.

The timeout environment variables are harmless to the old package because it
does not read them. A durable source rollback means reverting only this
timeout patch, rebuilding, and using the same bounded Terraform process.

## Result

The parked fix is complete. The successful path remains operational, and a
future Bedrock connection/read timeout can now return control early enough for
the existing structured failure and notification path instead of ending as an
unrecorded Lambda sandbox timeout.

## Tracker Mapping

This is maintenance of the verified Lakehouse managed-AI baseline. It supports
the Energy Data Lakehouse case study, SAP-C02 Domain 2 resilience, Domain 3
continuous improvement, and near-term Solution Architect positioning. It did
not resume unrelated parked work or advance the proposed ADR 0006 interactive
path.
