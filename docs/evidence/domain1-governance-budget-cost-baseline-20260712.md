# Domain 1 Evidence - Budget and Cost Baseline - 2026-07-12

## Boundary

This is public-safe, read-only evidence for the documentation-only IAM
foundation slice. No budget, notification, tag, IAM Identity Center, or AWS
resource was created, updated, or deleted.

## Read-Only Context

The `org-admin` profile resolved to the Organizations management account.
The evidence uses only AWS STS, Budgets, and Cost Explorer read APIs. Subscriber
identities were intentionally not retrieved or recorded.

## Current Budget Posture

The management account currently returns one cost budget:

| Field | Observed value |
|---|---|
| Budget name | `My Monthly Cost Budget` |
| Budget type and period | `COST`, monthly |
| Limit | `$10.00` |
| Actual spend | `$12.927` |
| Forecasted spend | `$23.402` |
| Cost filters | None |

The current budget is therefore already above its monthly limit and forecasts
more than twice that limit. This is current control evidence, not a justified
standard for the management, lakehouse, security, or sandbox accounts.

## Notifications

The budget has these verified notifications:

| Type | Threshold | Subscriber count |
|---|---:|---:|
| Actual spend | 85% | 2 |
| Actual spend | 100% | 2 |
| Forecasted spend | 100% | 2 |

The subscriber identities, addresses, and any private notification ownership
record are outside this public repository.

## Cost Explorer Observation

Cost Explorer returned estimated unblended cost for 2026-07-01 through
2026-07-12, grouped by linked account. Rounded values are shown to avoid
unnecessary billing precision.

| Account role | Estimated cost (USD) |
|---|---:|
| Lakehouse workload | 11.49 |
| Security Log Archive | 1.13 |
| Security Tooling | 0.20 |
| Management | 0.08 |
| Sandbox | 0.02 |
| Closed legacy account | 0.00 |

The observation is not service-level attribution and does not establish a
steady-state run rate. The closed-account entry is retained as Cost Explorer
reporting context, not evidence that the account remains active.

## Cost Allocation Tags

The following selected user-defined cost allocation tags remain `Active`:
`Workload`, `DataClassification`, `Purpose`, `ManagedBy`, `CostCenter`,
`Project`, and `Environment`.

## Decision Input

This fresh baseline shows that the existing all-services `$10` management
budget is breached and insufficiently scoped to be copied as an account-level
standard. The related decision is recorded in
`docs/planning/domain-1-budget-threshold-notification-ownership-decision-20260712.md`.
