# Domain 1 Evidence - Security Tooling Workforce Administrator Access - 2026-07-12

## Outcome

The existing normal IAM Identity Center Workforce Identity now has routine
administrator access to the `Security Tooling` account (`668848431187`) through
a dedicated `security-tooling-admins` group and the existing
`AdministratorAccess` permission set.

No duplicate human identity was created. Reusing the named workforce identity
and granting access through a purpose-specific group is the preferred pattern;
it avoids another password, user lifecycle, and MFA registration.

## Important Distinction

`Security Tooling` is the delegated administrator account for AWS Config and
GuardDuty. The IAM Identity Center `AdministratorAccess` permission set grants
the workforce identity administrative permissions inside that account. The
permission set does not make an account a delegated administrator and does not
grant management-account Organizations or SCP authority.

## How The Change Was Completed

1. Confirmed the management-account SSO session, active Identity Center
   instance, existing normal workforce identity, and Security Tooling delegated
   services.
2. Confirmed `security-tooling-admins` did not exist and Security Tooling had no
   `AdministratorAccess` or `BreakGlassAdmin` assignment.
3. Confirmed the existing `AdministratorAccess` permission set uses a one-hour
   session and only the AWS-managed `AdministratorAccess` policy.
4. Created `security-tooling-admins` and added only the existing normal
   workforce identity.
5. Assigned that group and permission set to Security Tooling only. The
   asynchronous assignment reached `SUCCEEDED` at 01:41 BST.

The management account, emergency workforce identity, `BreakGlassAdmin`,
`SecurityAudit`, SCPs, Organizations structure, delegated services, and other
AWS accounts were not changed.

## Validation

- The new group contains exactly the normal workforce identity.
- Security Tooling has exactly one `AdministratorAccess` group assignment.
- Security Tooling still has no `BreakGlassAdmin` assignment.
- The existing `security-tooling-auditors` plus `SecurityAudit` path is
  unchanged.
- The AWS access portal exposes `AdministratorAccess` and `SecurityAudit` for
  Security Tooling.
- A short-lived `AWSReservedSSO_AdministratorAccess_*` session assumed into
  Security Tooling successfully and completed read-only IAM, GuardDuty, and AWS
  Config checks.
- CloudTrail event history records `CreateGroup`, `CreateGroupMembership`, and
  `CreateAccountAssignment` at the execution timestamps.

## How To Use It

Sign in to the existing AWS access portal with the normal Workforce Identity,
open `Security Tooling`, and choose `AdministratorAccess`. Use `SecurityAudit`
instead whenever no change is required. Sessions expire after one hour.

## Rollback

Delete only the Security Tooling `AdministratorAccess` account assignment and
wait for the asynchronous deletion to succeed. Confirm the portal role is no
longer available. If the group is no longer required, remove its sole
membership and delete the empty group. Do not alter the audit or break-glass
paths.

## SAP-C02 Revision Points

- IAM Identity Center centrally manages workforce access with temporary
  account roles instead of shared root credentials or long-lived IAM users.
- Assign permission sets to groups, then manage people through group
  membership.
- Delegated administration moves supported service administration to a member
  account; it is separate from human authorization.
- Permission sets grant permissions, while SCPs only limit the maximum
  available permissions.
- Broad administrator access is useful for this bounded lab, but the mature
  target is a custom least-privilege Security Tooling administrator permission
  set.
