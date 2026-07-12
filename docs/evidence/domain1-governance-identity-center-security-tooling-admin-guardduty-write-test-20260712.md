# Domain 1 Evidence - SecurityToolingAdmin GuardDuty Write Test - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

The representative custom-role write request succeeded and its GuardDuty
postconditions match the immediately preceding baseline. The CloudTrail
Event History evidence requirement remains open: the idempotent write event was
not present during the immediate refreshes.

This result does not authorize removal of the temporary broad
`AdministratorAccess` assignment.

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

## Audit Evidence Caveat

Security Tooling CloudTrail Event History recorded subsequent GuardDuty and
Config read calls from the custom role, but did not return an
`UpdateOrganizationConfiguration` event during the immediate Event History
refreshes. Management-account Event History also did not return that event.

Do not infer that the organization trail failed from this result. The successful
API call and postcondition checks prove the service action; the organization
trail/object-path evidence or a later Event History appearance is still needed
before treating the audit portion of the gate as complete.

## Next Gate

Collect the missing audit evidence, then obtain separate approval to delete
only the Security Tooling `AdministratorAccess` group assignment. Keep
`SecurityToolingAdmin`, `SecurityAudit`, and `BreakGlassAdmin` unchanged.

## SAP-C02 Relevance

This demonstrates a controlled least-privilege rollout: validate a custom
federated role with a minimal idempotent write, verify service postconditions,
preserve rollback access, and refuse to reduce fallback access before audit
evidence is complete.
