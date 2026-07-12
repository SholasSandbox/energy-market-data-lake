# Domain 1 Evidence - SecurityToolingAdmin GuardDuty Write Test - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

The representative custom-role write request succeeded and its GuardDuty
postconditions match the immediately preceding baseline. Delayed CloudTrail
Event History and organization-trail object evidence now both confirm the
idempotent write.

This result alone did not authorize removal of the temporary broad
`AdministratorAccess` assignment. That later, separately approved action is
recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-broad-assignment-removal-change-note-20260712.md`.

## Approved Test Boundary

Reapply the already-live GuardDuty organization setting
`AutoEnableOrganizationMembers=NONE` through the live
`SecurityToolingAdmin` Workforce Identity role in `Security Tooling`
(`668848431187`).

Do not enroll or remove members, enable optional GuardDuty features, alter the
detector, change Config, modify IAM, Organizations, SCPs, Security Hub, OAM,
archive storage, accounts, or workload resources.

## Immediate Precheck

Immediately before the write:

- the assumed role was `AWSReservedSSO_SecurityToolingAdmin_*` in Security
  Tooling;
- one GuardDuty detector existed and was enabled;
- the approved member accounts were exactly `464975959576`, `955659429518`,
  and `974893866311`;
- `AutoEnable=false` and `AutoEnableOrganizationMembers=NONE`;
- every configured optional organization feature and nested additional
  configuration had `AutoEnable=NONE`; and
- foundational CloudTrail, DNS, and VPC flow-log sources were enabled, while
  optional S3, Kubernetes audit, and malware-protection sources were disabled.

## Executed Write

The custom role called GuardDuty `UpdateOrganizationConfiguration` in
`eu-west-2` with only `AutoEnableOrganizationMembers=NONE`.

The API returned successfully with no response payload, which is normal for
this operation. No feature configuration was supplied or changed.

## Postchange Validation

Immediately after the call:

- the three approved member accounts were unchanged;
- `AutoEnable=false` and `AutoEnableOrganizationMembers=NONE` remained
  unchanged;
- all eight optional organization features still reported `AutoEnable=NONE`;
- the GuardDuty detector remained enabled; and
- foundational CloudTrail, DNS, and VPC flow-log sources remained enabled.

The custom permission set therefore proved a representative live write path
without changing the observed GuardDuty posture or expanding cost scope.

## Audit Evidence

The initial Security Tooling and management-account Event History refreshes did
not return `UpdateOrganizationConfiguration`. After the SSO session was
refreshed, a delayed Security Tooling Event History query returned the intended
event with these sanitized fields:

- `eventTime`: `2026-07-12T11:30:05Z`;
- `eventSource`: `guardduty.amazonaws.com`;
- `eventName`: `UpdateOrganizationConfiguration`;
- `awsRegion`: `eu-west-2`;
- `recipientAccountId`: `668848431187`;
- `readOnly`: `false`;
- `requestParameters.autoEnableOrganizationMembers`: `NONE`;
- `errorCode`: `null`; and
- caller: the `AWSReservedSSO_SecurityToolingAdmin_*` assumed role in Security
  Tooling.

The management-account Event History remained empty for this member-account
action, which is expected and is not evidence of a trail failure.

The management-owned `organization-management-events` trail was active in
`eu-west-2`, had log-file validation enabled, and reported no delivery error.
The current trail points to
`shola-cloudtrail-log-archive-955659429518-eu-west-2`. Its Security Tooling
organization path contained the 11:35Z delivery object
`AWSLogs/o-hmvgqmav88/668848431187/CloudTrail/eu-west-2/2026/07/12/668848431187_CloudTrail_eu-west-2_20260712T1135Z_qEprkILGQ8ZCKM8N.json.gz`.
Reading that object through the Security Log Archive account returned the same
sanitized event fields above.

Earlier 2026-06 storage evidence uses a different bucket name. It remains
historical evidence for that change; the current trail configuration and this
object-path check are the authoritative evidence for the live archive location.

## Completed Next Gate

Separate explicit approval was obtained to delete only the Security Tooling
`AdministratorAccess` group assignment. The deletion succeeded, while
`SecurityToolingAdmin`, `SecurityAudit`, and the management-only
`BreakGlassAdmin` path remained unchanged.

## SAP-C02 Relevance

This demonstrates a controlled least-privilege rollout: validate a custom
federated role with a minimal idempotent write, verify service and audit
postconditions, preserve rollback access, and require a separately approved
privilege reduction.
