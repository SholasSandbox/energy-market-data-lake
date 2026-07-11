# Domain 1 Governance Evidence - SecurityAudit Direct-Access Assignment - 2026-07-11

<!-- markdownlint-disable MD013 -->

## Status

Completed under the explicit approval recorded on 2026-07-11. This change
created one dedicated IAM Identity Center group, one custom inline-policy
permission set, and one group assignment in Security Tooling only.

## Approved Scope

- Group: `security-tooling-auditors`.
- Member: the existing normal Workforce Identity only.
- Permission set: `SecurityAudit`, with `PT1H` session duration.
- Policy: only
  `docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json`.
- Assignment: that group plus that permission set in Security Tooling only.

No existing administrator group, emergency Workforce Identity, management
account assignment, additional account, AWS-managed policy, customer-managed
policy, permissions boundary, SCP, Organizations, AWS Config, GuardDuty,
Security Hub, OAM, root, or budget setting was changed.

## Immediate Precheck

Immediately before execution:

- the management-account SSO caller and one IAM Identity Center instance were
  active;
- the normal Workforce Identity was enabled;
- `security-tooling-auditors` and `SecurityAudit` did not exist;
- Security Tooling had no `AdministratorAccess` or `BreakGlassAdmin`
  assignment; and
- IAM Access Analyzer returned no findings for the approved custom policy.

The precheck and managed-policy comparison are recorded in
`docs/evidence/domain1-governance-identity-center-security-audit-precheck-20260711.md`.

## Executed Change

1. Created `security-tooling-auditors` with no initial members.
2. Added only the normal Workforce Identity to that group.
3. Created `SecurityAudit` with `PT1H`.
4. Applied only the approved custom inline policy. There are no AWS-managed or
   customer-managed policy attachments and no permissions boundary.
5. Created the group assignment in Security Tooling. The asynchronous request
   reached `SUCCEEDED` on 2026-07-11 at 18:50 BST.

## Validation

- Security Tooling has exactly one `SecurityAudit` assignment, to the new
  auditor group.
- Security Tooling still has no `AdministratorAccess` or `BreakGlassAdmin`
  assignment.
- The dedicated emergency Workforce Identity remains outside all groups.
- `SecurityAudit` is provisioned to Security Tooling and is visible through
  the normal Workforce Identity's AWS access portal entitlement.
- A short-lived `AWSReservedSSO_SecurityAudit_*` session assumed into Security
  Tooling successfully.
- In `eu-west-2`, that session listed one GuardDuty detector, one AWS Config
  recorder, and one AWS Config aggregator.
- CloudTrail event history records `CreateGroup`, `CreatePermissionSet`, and
  `CreateAccountAssignment` events at the execution timestamps.

No prohibited mutation was attempted as part of validation.

## Cost And Audit Impact

No new security service, GuardDuty protection plan, delegated administrator,
or data-plane workload was enabled. The custom policy excludes the AWS-managed
`SecurityAudit` policy actions that can request Config snapshot delivery or IAM
report generation. Existing organization CloudTrail records the administrative
events.

## Rollback

If this direct access must be withdrawn, delete only the Security Tooling
`SecurityAudit` group assignment, wait for the deletion request to succeed,
and confirm the role no longer appears in the access portal. Leave the group
and permission set in place for separate, reviewable cleanup; neither grants
Security Tooling access without the assignment.

## SAP-C02 Relevance

This proves a least-privilege human access path to a delegated security
administrator, including precheck, custom-policy validation, provisioning,
portal entitlement, audit evidence, and a reversible access boundary. It
supports Domain 1 governance and Domain 3 operational evidence collection.
