# Domain 1 Evidence - Cost Attribution Refresh - 2026-07-13

## Boundary

This public-safe evidence refreshes the IAM-foundation cost-governance slice
with read-only AWS Organizations, Cost Explorer, and Lakehouse-account EC2
inventory queries. No budget, notification, tag, IAM, or AWS resource was
changed.

## Account-Level Monthly History

Amounts are unblended USD, rounded to two decimal places. July is a
month-to-date estimated observation through 2026-07-12, not a completed month.

| Period | Management | Lakehouse workload | Security Log Archive | Security Tooling | Sandbox | Total | Quality |
|---|---:|---:|---:|---:|---:|---:|---|
| 2026-04 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | Estimated; unsuitable |
| 2026-05 | 0.00 | — | — | — | — | 0.00 | Finalized; effectively zero |
| 2026-06 | 0.20 | 13.65 | 0.88 | — | 0.21 | 14.93 | Finalized; first meaningful month |
| 2026-07 MTD | 0.14 | 11.82 | 1.23 | 0.20 | 0.02 | 13.42 | Estimated; incomplete |

A dash means that Cost Explorer returned no non-zero linked-account group for
the period. The closed legacy account's negligible values are omitted from the
rounded table.

## June Attribution

The prior organization-wide service query could not safely assign untagged
spend to an account. A fresh query grouped by **linked account and service**
resolves that limitation for June:

| Account role | Largest June drivers | Account total (USD) |
|---|---|---:|
| Lakehouse workload | EC2 - Other 8.71; Tax 2.26; IAM Access Analyzer 1.00; AWS Config 0.57; CloudTrail 0.41; Secrets Manager 0.34 | 13.65 |
| Security Log Archive | KMS 0.43; AWS Config 0.18; Tax 0.16; S3 0.11 | 0.88 |
| Management | AWS Config 0.17; Tax 0.03 | 0.20 |
| Sandbox | AWS Config 0.16; Tax 0.04 | 0.21 |

Within the Lakehouse account, the June `EC2 - Other` charge is specifically
the `EUW2-EBS:VolumeUsage.gp2` usage type (8.71 USD). A fresh read-only
inventory found two gp2 EBS volumes: an unattached 80 GiB volume and an
attached 8 GiB volume. Neither carries a `Workload` tag. Their combined
capacity is consistent with the attributed EBS charge; this is a cost-driver
finding, not authorization to modify or remove either volume.

The June account-and-tag query also showed 13.65 USD of the Lakehouse account
as untagged and only 0.0007 USD tagged `Workload=energy-market-data-lake`.
The account boundary now supports an account-level future budget baseline, but
the tag remains unsuitable as the sole budget filter.

## July Directional Observation

The incomplete July Lakehouse total is 11.82 USD through 2026-07-12. Its
largest visible drivers are IAM Access Analyzer (4.40), EC2 - Other (3.87),
Tax (1.96), CloudTrail (0.77), and Route 53 (0.50). This supports monitoring
the Lakehouse account as a unit, but it is not a second finalized baseline.

## Threshold Decision Impact

The evidence gap has narrowed from **unattributed organization-wide spend** to
**account-attributed but insufficient history**. The following gates remain:

1. Collect two additional meaningful finalized monthly observations.
2. Reassess whether the EBS volumes and the growing Access Analyzer charge are
   steady-state, transitional, or exceptional before using them in a baseline.
3. Keep any future budget account-scoped rather than `Workload`-tag-only until
   tag coverage is materially improved.
4. Obtain a separate live-change approval immediately before any budget,
   notification, tagging, or remediation action.

Therefore, no numeric threshold is proposed or authorized by this refresh.

## SAP-C02 Relevance

This supports Domain 1 cost governance by using account and service dimensions
to distinguish workload costs from centralized security and management costs,
while preserving least-privilege and change-approval boundaries.
