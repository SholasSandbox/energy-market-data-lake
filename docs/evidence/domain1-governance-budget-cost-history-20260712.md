# Domain 1 Evidence - Budget Cost History - 2026-07-12

## Boundary

This public-safe evidence extends the documentation-only IAM foundation slice.
It records three requested calendar-month Cost Explorer periods through
read-only APIs. No budget, notification, tag, identity, or AWS resource was
changed.

## Period Quality

| Period | Cost Explorer status | Suitable as a threshold baseline? |
|---|---|---|
| 2026-04 | `$0.00`, still returned as estimated | No |
| 2026-05 | Effectively `$0.00` | No |
| 2026-06 | Finalized and non-zero | One observation only; no |

The requested three periods exist, but they do not provide three meaningful,
finalized observations. They are insufficient for evidence-supported numeric
thresholds.

## Linked-Account Observation

Rounded June unblended costs were:

| Account role | June cost (USD) |
|---|---:|
| Lakehouse workload | 13.65 |
| Security Log Archive | 0.88 |
| Sandbox | 0.21 |
| Management | 0.20 |
| Closed legacy account | 0.00 |

The fresh July month-to-date observation is directionally similar for the
lakehouse workload and Security Log Archive, but it is estimated and is not a
complete monthly comparison.

## Service and Tag Limitations

June service-level totals show that `EC2 - Other` (8.71), Tax (2.49), AWS
Config (1.08), and IAM Access Analyzer (1.00) are the largest non-zero items.
That query was not cross-grouped with account, so it must not be used to assign
an exact service cost to an account.

The active `Workload` tag returned almost all June spend as untagged (14.93),
with only 0.00 attributed to `energy-market-data-lake` after rounding. Current
tag coverage is therefore inadequate for workload-tag budget thresholds.

This query is organization-wide. A fresh lakehouse-account resource inventory
confirms 14 core workload resources already carry the tag and found no NAT
gateway, Elastic IP, or VPC endpoint in that account. This initial artifact
therefore did not assign the untagged spend to the Lakehouse. A follow-up
account-and-service query on 2026-07-13 now attributes 13.65 USD of it to the
Lakehouse account and identifies its leading driver; see
`docs/evidence/domain1-governance-cost-attribution-20260713.md`.

## Implication

No numeric per-account threshold is proposed from this evidence. The next
threshold decision needs three meaningful finalized months, better tag
coverage where tag-based controls are intended, and the private ownership
confirmation required by
`docs/planning/domain-1-budget-threshold-notification-ownership-decision-20260712.md`.
