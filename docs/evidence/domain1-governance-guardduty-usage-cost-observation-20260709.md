# Domain 1 GuardDuty Usage And Cost Observation - 2026-07-09

## Status

Completed as a read-only observation. No GuardDuty feature, Region, member,
account, SCP, IAM, AWS Config, Security Hub, OAM, or workload setting changed.

## Scope And Method

- Management-account identity was verified through the existing `org-admin`
  IAM Identity Center profile.
- Security Tooling (`668848431187`) was accessed through a short-lived
  `OrganizationAccountAccessRole` session held only in memory.
- The observation read GuardDuty organization administration, detector,
  organization-configuration, member, CloudWatch usage-metric, and Cost
  Explorer data for `eu-west-2`.

## Current GuardDuty State

- Security Tooling remains the enabled GuardDuty delegated administrator.
- Security Tooling detector: `6ccf9e93cbfefca63bcb3c31593649c3`.
- Associated member accounts: lakehouse workload (`464975959576`), Security Log
  Archive (`955659429518`), and Container Sandbox (`974893866311`).
- `AutoEnableOrganizationMembers` is `NONE`.
- S3 data events, EKS audit/runtime monitoring, EBS malware protection, RDS
  login events, Lambda network logs, runtime monitoring, and AI Analyst remain
  set to `NONE`.

## Usage Observation

CloudWatch published both foundational organization-level GuardDuty metrics in
`eu-west-2`:

| Metric | Data source | Daily observation | Unit |
|---|---|---:|---|
| `AnalyzedCount` | `CloudTrailEvents` | 416 on 2026-07-07; 636 on 2026-07-08; 460 on 2026-07-09 | Count |
| `AnalyzedBytes` | `VPCFlowLogDNSLogEvents` | 15,946 on 2026-07-08; 15,944 on 2026-07-09 | Bytes |

The 2026-07-09 buckets were read before the day ended and must not be treated
as final daily totals.

## Cost Observation

Cost Explorer returned an estimated `0 USD` unblended GuardDuty cost for each
available daily bucket:

| Period | Cost | Status |
|---|---:|---|
| 2026-07-07 to 2026-07-08 | $0 | Estimated |
| 2026-07-08 to 2026-07-09 | $0 | Estimated |

This is an early baseline, not a durable monthly cost conclusion. Continue
observing after Cost Explorer finalizes the daily data and before considering a
new Region or optional GuardDuty protection plan.

## Decision

Keep the current `eu-west-2` foundational-only posture unchanged. Do not enable
Security Hub, OAM, optional GuardDuty features, additional Regions, or
organization auto-enrollment from this observation.

## SAP-C02 Relevance

This evidence supports Domain 1 governance through delegated-administrator
validation, multi-account member coverage, cost-aware security-service
sequencing, and explicit separation of observation from configuration change.
