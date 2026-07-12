# Domain 1 Evidence - SecurityToolingAdmin Broad Assignment Removal - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

Completed under explicit approval. The Security Tooling
`AdministratorAccess` group assignment was deleted after the custom
`SecurityToolingAdmin` path passed portal, service, representative-write, and
audit validation.

No rollback was required.

## Approved Boundary

Delete only the `AdministratorAccess` IAM Identity Center account assignment
for the existing `security-tooling-admins` group in Security Tooling account
`668848431187`.

Do not change a permission set, inline policy, group, group membership, AWS
Config, GuardDuty, CloudTrail, Organizations, SCPs, archive storage, another
account, or workload resources.

## Immediate Precheck

Immediately before the deletion:

- the active IAM Identity Center instance was
  `arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7` with identity store
  `d-9c674fdf75`;
- `security-tooling-admins` existed as the target group;
- `AdministratorAccess` had exactly one Security Tooling assignment, to that
  group only;
- `SecurityToolingAdmin` had exactly one Security Tooling assignment, to that
  same group;
- `SecurityAudit` had exactly one Security Tooling assignment, through its
  separate auditor group; and
- `BreakGlassAdmin` was not provisioned or assigned in Security Tooling.

The prerequisite representative GuardDuty write and its Event History plus
organization-trail object evidence are recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-guardduty-write-test-20260712.md`.

## Change Executed

The management-account IAM Identity Center administrator submitted one
`DeleteAccountAssignment` request for the `AdministratorAccess` permission set,
the `security-tooling-admins` group, and Security Tooling account only.

The deletion request created at `2026-07-12T14:01:15.654+01:00` reached
`SUCCEEDED`. No second delete request was submitted.

## Postchange Validation

- `AdministratorAccess` has zero assignments in Security Tooling.
- The workforce portal now lists only `SecurityAudit` and
  `SecurityToolingAdmin` for Security Tooling.
- Fresh credentials successfully assumed the
  `AWSReservedSSO_SecurityToolingAdmin_*` role in account `668848431187`.
- Fresh credentials successfully assumed the
  `AWSReservedSSO_SecurityAudit_*` role in account `668848431187`.
- The custom administrator and auditor group assignments remained unchanged.

IAM Identity Center assignment removal prevents new portal role selection. Any
previously issued `AdministratorAccess` AWS credentials can remain valid until
their existing one-hour session expires; this change does not revoke an already
issued STS session.

## Rollback

The approved rollback was to recreate only the same
`AdministratorAccess` group assignment if validation showed unexpected loss of
the intended custom or audit access. Validation passed, so no rollback was
performed.

## SAP-C02 Relevance

This completes a staged least-privilege migration for Domain 1: add a
task-specific federated role, prove normal operations and auditability, retain
rollback during validation, then remove the broader routine entitlement within a
single account boundary.
