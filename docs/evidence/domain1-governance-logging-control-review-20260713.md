# Domain 1 Evidence - Logging and Governance Control Review - 2026-07-13

## Boundary

This is a public-safe, read-only review for the tracker week beginning
2026-07-27, started early under direct user instruction. It queried AWS
Organizations, CloudTrail, AWS Config, and GuardDuty. No trail, bucket, KMS
key, recorder, rule, delegated administrator, detector, feature, notification,
or IAM setting was changed.

## Verified Control Continuity

| Control | Fresh result | Status |
|---|---|---|
| Organization CloudTrail | Multi-Region organization trail is logging; latest log and digest delivery succeeded on 2026-07-13 | Verified |
| Organizations trusted access | CloudTrail, AWS Config, multi-account Config, GuardDuty, Account Management, and IAM Identity Center trusted access remain enabled | Verified |
| Delegated administration | The active Security Tooling boundary remains the organization delegated-administrator account | Verified at organization level |
| Lakehouse AWS Config | One `default` recorder is continuously recording all supported resource types; the current status is `SUCCESS`; delivery remains directed to the central Config archive | Verified |
| Lakehouse GuardDuty | Detector is enabled with CloudTrail, DNS, and VPC Flow Log foundations; findings publish every 15 minutes | Verified |

## Deliberate Current Limits

The following are not failures in the current low-volume Lakehouse scope and
are not changed by this review:

| Area | Current posture | Decision |
|---|---|---|
| GuardDuty S3 data events | Disabled | Leave deferred until S3-data-event volume, threat model, and cost justify it. |
| GuardDuty malware protection and runtime monitoring | Disabled | Leave deferred; no EKS/ECS/EC2 runtime-monitoring use case is in the current tracker scope. |
| Security Hub | Not adopted | Keep as a deliberate future Security Tooling decision, not an implied logging prerequisite. |
| CloudWatch cross-account observability/OAM | Not adopted | Keep as a future observability-design decision; CloudTrail and Config already provide the current governance evidence path. |
| Additional AWS Config rules | Not added | Retain the existing organization CloudTrail rule and avoid rule expansion until a named compliance question and cost/exception model exist. |

## Decision

The current logging/governance baseline is sufficient to support the active
Lakehouse platform and the previously selected narrow SCP design work. Do not
activate Security Hub, OAM, additional GuardDuty data sources, or additional
Config rules solely to advance the schedule.

Future changes must remain independent, approval-bound units. In particular,
an SCP that protects CloudTrail is not a substitute for trail-delivery checks,
and a security-service feature enablement must include service-specific cost,
coverage, rollback, and evidence criteria.

## Next Review Inputs

The scheduled 2026-07-27 follow-up should combine this control-continuity
baseline with the non-blocking cost-threshold review. It should confirm that
log and digest delivery remain current, the Lakehouse Config recorder remains
healthy, and GuardDuty foundational sources remain enabled. It should not
assume that a new security service is required unless a named risk or SAP-C02
decision requires it.

## SAP-C02 Relevance

This supports Domain 1 by separating centralized audit logging, configuration
recording, delegated security operations, and optional findings aggregation;
it also records a proportionate adoption decision rather than enabling
services without an operational or cost rationale.
