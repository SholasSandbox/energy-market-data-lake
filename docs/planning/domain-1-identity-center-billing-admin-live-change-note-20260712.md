# Domain 1 Governance Change Note - BillingAdmin IAM Identity Center Assignment - Pending

## Status

Approval-readiness note only. It is **not approved for execution** and does not
create a permission set, policy, group, assignment, or IAM role.

This is deliberately separate from the future budget-change note. A billing
permission-set rollout must not be bundled with budget creation, threshold
updates, notification changes, tag changes, or any Organizations action.

## Target Account and Scope

| Field | Intended boundary |
|---|---|
| Target account | Organizations management account only |
| Identity Center scope | One future `BillingAdmin` permission set and the dedicated `billing-admins` Workforce Identity group only |
| Session duration | `PT1H` |
| Region | IAM Identity Center home Region; Billing, Budgets, and Cost Explorer use their applicable AWS endpoints |
| Excluded accounts | Lakehouse workload, Security Tooling, Security Log Archive, sandbox, and closed legacy account |

## Proposed Change

After all gates below are met, create the `BillingAdmin` permission set with
`PT1H`, attach only
`docs/policies/iam-identity-center-billing-admin.inline-policy.example.json`,
and assign the `billing-admins` group in the management account only. The
policy permits review and tightly scoped updates to future `BillingAdmin-*`
budgets carrying `GovernanceControl=BillingAdmin`. It denies budget actions and
tag administration, plus Organizations, workload, security-service,
IAM-lifecycle, payment, tax, and emergency-recovery actions. AWS combines
budget creation and modification under `budgets:ModifyBudget`, so the final
precheck must validate the named-resource/tag boundary and deletion behavior.

The primary/backup owners and group membership remain intentionally unpopulated.
Without them and the threshold evidence, this is not executable or approvable.

## Current Evidence

- `BillingAdmin` remains design-only; its accepted group, session, and policy
  boundary are recorded in
  `docs/planning/domain-1-billing-admin-foundation-design-20260713.md`.
- The budget and notification ownership decision remains pending stronger cost
  evidence and private owner confirmation in
  `docs/planning/domain-1-budget-threshold-notification-ownership-decision-20260712.md`.
- Existing `SecurityAudit`, `SecurityToolingAdmin`, and management-only
  emergency paths are outside this change.

## Required Immediate Precheck

Immediately before any execution, capture public-safe read-only evidence of:

1. the management-account AWS identity and IAM Identity Center instance;
2. the target group and its membership count, without publishing personal data;
3. `BillingAdmin` absence or the exact current permission-set state;
4. management-account assignment counts for the target group and all
   `AdministratorAccess`, `BreakGlassAdmin`, and `BillingAdmin` paths;
5. the final policy JSON validation and IAM Access Analyzer results; and
6. representative allowed and denied policy simulation results.

Stop if the policy, principal/group scope, target account, or existing
assignment state differs from the approved change request.

## Expected Blast Radius

Only new portal access for the approved Workforce Identity group in the
management account. If the final policy includes budget writes, that access can
alter budget thresholds and notifications; those actions still require the
separate budget-change approval note and must not be exercised as part of IAM
assignment validation.

## Rollback

Delete only the new `BillingAdmin` management-account assignment and wait for
the deletion request to succeed. Confirm the permission set is no longer
available in the affected portal. Do not alter existing routine administrator,
auditor, or emergency assignments. Permission-set deletion, if later desired,
is a separately reviewed cleanup decision.

## Validation

- Fresh portal session assumes the intended `BillingAdmin` role only in the
  management account.
- Read-only Billing, Budgets, Cost Explorer, and approved tag visibility match
  the final policy simulation.
- Organizations, SCP, workload, security-service, IAM-lifecycle, and
  cross-account actions remain denied.
- Existing administrator, audit, and emergency paths remain unchanged.
- CloudTrail records only the approved IAM Identity Center actions.

## Cost and Evidence Impact

Creating the permission set and assignment has no expected direct runtime
charge. It generates administrative audit events. Do not use the role to
create or change a budget until the separate budget note is explicitly approved.
Store subscriber identities and private ownership confirmations outside the
public repository; run `scripts/check_public_evidence_redaction.sh` before
staging evidence.

## Approval

**Pending.** Private primary/backup ownership was confirmed on 2026-07-12 and
is retained outside this public repository. Before execution, this section must
still identify the approver, date, approved `billing-admins` group membership,
target account, and the specific validation boundary. User authorization to
create this note is not approval to execute the IAM change.

## SAP-C02 Relevance

This supports Domain 1 by separating federated billing access from broad
administrator access and by requiring least-privilege simulation, rollback,
and audit evidence before an assignment is made.
