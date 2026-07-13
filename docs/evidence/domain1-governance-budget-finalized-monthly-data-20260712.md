# Domain 1 Evidence - Finalized Monthly Cost Data - 2026-07-12

## Boundary

This is the public-safe finalized monthly dataset for the next budget-threshold
proposal. It was generated from a read-only Cost Explorer 12-month lookback,
grouped by linked account. No AWS resource or billing configuration changed.

## Finalized Data Availability

Cost Explorer returned only these finalized periods in the 2025-07 through
2026-06 lookback. Earlier months were returned as estimated zero-value periods
and are excluded from this finalized dataset.

| Month | Management | Lakehouse workload | Security Log Archive | Sandbox | Closed legacy | Total |
|---|---:|---:|---:|---:|---:|---:|
| 2026-05 | 0.00 | — | — | — | — | 0.00 |
| 2026-06 | 0.20 | 13.65 | 0.88 | 0.21 | 0.00 | 14.93 |

Amounts are unblended USD, rounded to two decimal places. A dash means Cost
Explorer returned no group for that account in the finalized period; it is not
a claim that an account or service configuration did not exist.

## Dataset Quality

The dataset has only one meaningful non-zero month. It cannot establish
variance, a normal operating range, or a defensible account-level alert
threshold. The accompanying tag query also shows that the active `Workload`
tag does not currently classify most June spend. A 2026-07-13 account-and-
service refresh attributes the June Lakehouse amount without relying on that
tag, but does not add a second finalized month; see
`docs/evidence/domain1-governance-cost-attribution-20260713.md`.

## Use for the Next Proposal

Use this artifact as the initial finalized baseline, then refresh it after each
future month closes. Do not turn it into a numeric threshold until it contains
three meaningful finalized months, tag coverage is adequate for any tag-based
control, and the private primary/backup ownership confirmation is complete.

Related raw-safe interpretation and service/tag limitations are recorded in
`docs/evidence/domain1-governance-budget-cost-history-20260712.md`.
