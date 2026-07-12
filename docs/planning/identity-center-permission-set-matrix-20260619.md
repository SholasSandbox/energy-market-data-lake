# IAM Identity Center Permission-Set Matrix - 2026-06-19

<!-- markdownlint-disable MD013 -->

## Scope

This document converts the Domain 1 governance design into a human-access
matrix for IAM Identity Center.

It is documentation only. It does not create permission sets, assign users, add
groups, change IAM roles, or modify AWS accounts.

Related design:

- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/domain-1-governance-preflight-20260618.md`

## Design Principles

- Use the management account only for Organizations, billing, Identity Center,
  and SCP administration.
- Keep lakehouse operation in the workload account.
- Keep container labs in sandbox scope.
- Prefer task-specific permission sets over one broad administrator path.
- Keep emergency access separate from routine administration.
- Treat read-only visibility as a first-class access pattern.

## Account Targets

| Account | Purpose | Routine access model |
| --- | --- | --- |
| `management-account-alias` | Organizations management account | `OrganizationAdmin`, `BillingAdmin`, and `SecurityAudit` only. |
| `lakehouse-workload-account` | Energy Data Lakehouse workload account | `LakehouseOperator`, `LakehouseReadOnly`, and `SecurityAudit`. |
| `containers-lab.com` | Sandbox/container lab account | Future `SandboxOperator` or `SecurityAudit`; not lakehouse evidence. |
| `Security Tooling` | Config and GuardDuty delegated-administrator account | Live `SecurityAudit` and staged `SecurityToolingAdmin`, plus a bounded temporary `AdministratorAccess` path through `security-tooling-admins`. |
| `Security Log Archive` | Storage-only central logging boundary | `SecurityAudit`, limited log administration, and emergency access only. |

## Permission-Set Matrix

| Permission set | Persona | Target account | Access level | Intended scope | Not allowed |
| --- | --- | --- | --- | --- | --- |
| `OrganizationAdmin` | Organization administrator | `management-account-alias` | High privilege | Organizations, OU placement, SCP management, delegated-admin setup. | Lakehouse runtime work, routine data operations, dashboard changes. |
| `BillingAdmin` | Cost/governance operator | `management-account-alias` | Billing administration | Billing views, budgets, cost allocation tags, Cost Explorer. | Organizations account movement, SCP changes, workload administration. |
| `SecurityAudit` | Security reviewer | Management, workload, sandbox, future security account | Read-only | IAM, CloudTrail, Config, GuardDuty, Security Hub, S3 posture, logs, and evidence review. | Mutating resources, disabling controls, changing policies. |
| `AdministratorAccess` | Security Tooling administrator | `Security Tooling` only | Full account administrator | Bounded administration of delegated Config, GuardDuty, and supporting account resources. | Management-account Organizations/SCP work, other accounts, routine read-only review. |
| `SecurityToolingAdmin` | Security Tooling administrator | `Security Tooling` only | Least-privilege delegated-security operator | Existing Config aggregation/recording and GuardDuty organization/member operations in `eu-west-2`. | Control-plane delegation, IAM lifecycle, security-baseline teardown, archive storage, other accounts/Regions. |
| `LakehouseOperator` | Lakehouse maintainer | `lakehouse-workload-account` | Workload operator | Lambda, Glue, Athena, S3 lakehouse prefixes, EventBridge, Step Functions, SQS/DynamoDB if used by the lakehouse. | Organizations, SCPs, billing, account-level IAM administration. |
| `LakehouseReadOnly` | Reviewer/interviewer/demo user | `lakehouse-workload-account` | Read-only | Runtime posture, CloudWatch logs, Athena metadata, S3 inventory-style review, evidence checks. | Data mutation, IAM mutation, deployment, schedule changes. |
| `BreakGlassAdmin` | Emergency owner | As required | Emergency administrator | Account recovery only when normal Identity Center or delegated admin paths fail. | Routine use, convenience operations, long-running project work. |

## Candidate Policy Shape

### `OrganizationAdmin`

Use a tightly assigned administrative permission set in the management account.
The first implementation can use AWS-managed administrator access only if the
assignment is limited to a named owner and protected by MFA. A later hardening
pass should replace broad access with a custom policy for Organizations, IAM
Identity Center, and SCP administration.

### `BillingAdmin`

Use billing and cost-management permissions in the management account. The
candidate scope includes Budgets, Cost Explorer, Cost Allocation Tags, and
billing console visibility. It should not include Organizations account
movement or workload deployment permissions.

### `SecurityAudit`

Use read-only security visibility across accounts. Candidate services include
IAM, CloudTrail, AWS Config, GuardDuty, Security Hub, S3, KMS metadata, and
CloudWatch Logs read actions. It should not include mutation actions.

### `LakehouseOperator`

Use workload-scoped operator permissions in the lakehouse member account.
Access should be constrained to the lakehouse service set and known resource
prefixes where practical. It should not include Organizations, SCP, billing, or
broad account-administrator authority.

### `LakehouseReadOnly`

Use read-only workload visibility for portfolio review and operational checks.
It should support evidence collection without allowing data mutation,
deployment, or schedule changes.

### `BreakGlassAdmin`

Use only for emergency recovery. The permission set should be assigned to the
minimum possible number of owners, protected with MFA, monitored, and reviewed
after every use.

Same-day 2026-06-25 evidence now shows that the current IAM Identity Center
instance has one enabled live admin principal,
`org-admin-principal` / `[redacted-email]`, one dedicated
emergency principal,
`breakglass-principal` / `[redacted-email]`, and two visible
permission sets, `AdministratorAccess` and `BreakGlassAdmin`. The first staged
`BreakGlassAdmin` implementation is now live with `PT1H` session duration,
AWS-managed `AdministratorAccess`, and a direct management-account assignment
for `breakglass-principal`:
`docs/evidence/domain1-governance-identity-center-current-state-20260625.md`.

The current repository now uses the staged combination that was recommended:
`BreakGlassAdmin` is backed initially by the AWS-managed
`AdministratorAccess` policy, kept to a short one-hour session, targeted only
at the management account, and assigned only to the dedicated emergency
principal. A later hardening pass can replace that broad base with a custom
policy once the exact Organizations and recovery action set is proven.

Follow-on 2026-07-02 live cleanup removed the same emergency principal from
`cloud-lab-aws-admins` after read-only verification showed that the group
membership caused inherited management-account `AdministratorAccess` to appear
alongside `BreakGlassAdmin` in the AWS access portal. Postchange evidence shows
`breakglass-principal` now has no group memberships and retains only the direct
management-account `BreakGlassAdmin` assignment:
`docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`.

The first bounded routine-access change is live in Security Tooling:
`SecurityAudit` through a dedicated `security-tooling-auditors` group containing
only the normal Workforce Identity. The prepared inline policy excludes the
current AWS-managed `SecurityAudit` policy's Config snapshot-delivery and IAM
report-generation actions. Precheck, provisioning, portal-entitlement, service
read checks, and CloudTrail evidence are recorded in
`docs/evidence/domain1-governance-identity-center-security-audit-assignment-change-note-20260711.md`.

The first bounded routine administrator path is also live in Security Tooling:
the existing one-hour `AdministratorAccess` permission set is assigned through
the dedicated `security-tooling-admins` group containing only the normal
Workforce Identity. This is an account-scoped learning and administration path,
not the final least-privilege design. Precheck, assignment, portal-session,
read-only service validation, and rollback evidence are recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-assignment-change-note-20260712.md`.

The least-privilege replacement design is recorded in
`docs/planning/domain-1-identity-center-security-tooling-admin-permission-set-design-20260712.md`,
with its proposed inline policy in
`docs/policies/iam-identity-center-security-tooling-admin.inline-policy.example.json`.
Stage 1 is live through the existing `security-tooling-admins` group, with
portal and read-only service validation recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-staged-assignment-change-note-20260712.md`.
The temporary broad path remains until a representative write test and separate
removal approval are complete. The GuardDuty write call and unchanged
postcondition evidence are recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-guardduty-write-test-20260712.md`;
its missing immediate audit-event evidence keeps the broad path in place.

## Assignment Rules

- Assign management-account administration only from the management account.
- Do not assign `OrganizationAdmin` to workload-only personas.
- Do not use `BreakGlassAdmin` for normal work.
- Prefer read-only assignments when the user only needs evidence review.
- Review all assignments before attaching restrictive SCPs.

## Open Items Before Live Use

- Keep the existing `org-admin-principal` principal as the normal
  management-account administrator, not the primary dedicated break-glass
  identity.
- Decide whether the next hardening step should move the break-glass assignment
  from a direct user assignment to a single-purpose emergency group.
- Write custom permission-set policy documents where broad AWS-managed policies
  are too permissive.
- Validate and, under separate approval, stage the prepared
  `SecurityToolingAdmin` permission set before removing the bounded Security
  Tooling `AdministratorAccess` path.
- Reconcile notification recipients and MFA ownership with the actual live
  Identity Center principal inventory.
- Create rollback steps for removing each assignment.
- Keep explicit approval as a prerequisite before changing this permission set,
  broadening its account scope, or adding more principals.

## SAP-C02 Relevance

This matrix supports Domain 1 by showing how account boundaries, administrative
roles, and least privilege work together. It also reinforces that human access
is granted through IAM Identity Center permission sets and IAM roles, while
SCPs only define maximum allowed permissions.
