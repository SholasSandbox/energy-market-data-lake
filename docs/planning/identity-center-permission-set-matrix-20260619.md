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
| Future security/log archive account | Central audit and logging boundary | `SecurityAudit`, limited log administration, and emergency access only. |

## Permission-Set Matrix

| Permission set | Persona | Target account | Access level | Intended scope | Not allowed |
| --- | --- | --- | --- | --- | --- |
| `OrganizationAdmin` | Organization administrator | `management-account-alias` | High privilege | Organizations, OU placement, SCP management, delegated-admin setup. | Lakehouse runtime work, routine data operations, dashboard changes. |
| `BillingAdmin` | Cost/governance operator | `management-account-alias` | Billing administration | Billing views, budgets, cost allocation tags, Cost Explorer. | Organizations account movement, SCP changes, workload administration. |
| `SecurityAudit` | Security reviewer | Management, workload, sandbox, future security account | Read-only | IAM, CloudTrail, Config, GuardDuty, Security Hub, S3 posture, logs, and evidence review. | Mutating resources, disabling controls, changing policies. |
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
