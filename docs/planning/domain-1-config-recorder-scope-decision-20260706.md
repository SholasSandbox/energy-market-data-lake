# Domain 1 AWS Config Recorder Scope Decision - 2026-07-06

<!-- markdownlint-disable MD013 -->

## Status

Decision implemented for `Security Tooling` on 2026-07-07.

Live implementation evidence:

- `docs/evidence/domain1-governance-config-security-tooling-recorder-change-note-20260707.md`

## Context

AWS Config delegated administration and aggregation were migrated from `Security
Log Archive` (`955659429518`) to `Security Tooling` (`668848431187`) in
`eu-west-2` on 2026-07-06.

After the 2026-07-07 Security Tooling recorder onboarding, the migrated
organization CloudTrail Config rule remains deployed only to accounts with
working AWS Config recorders. It now excludes:

- `so-aws-admin` (`054394900225`)

Both `Security Tooling` and `so-aws-admin` were originally excluded because they
lacked AWS Config recorders before the delegated-administrator migration. This
decision has now been implemented only for `Security Tooling`; `so-aws-admin`
remains excluded on the decommission path.

## Decision

| Account | Decision | Rationale |
|---|---|---|
| `Security Tooling` / `668848431187` | In scope for the next bounded AWS Config recorder implementation in `eu-west-2`. | This account is now the AWS Config delegated administrator and owns the organization aggregator, so its own configuration posture should become visible before further security-service migration. |
| `so-aws-admin` / `054394900225` | Keep excluded from the current recorder implementation scope and place on the decommission path. | The account is pre-existing Security OU current state, not the new `Security Tooling` account. Enabling recording would expand cost and governance scope in an account now intended for retirement after dependency checks. |

## Implemented Bounded Change

The approved live implementation was limited to `Security Tooling` recorder
onboarding:

1. Collect fresh read-only prechange evidence for AWS Config state, central
   archive bucket policy, central Config KMS key policy, and the organization
   CloudTrail Config rule.
2. Extend the central AWS Config archive bucket policy and Config KMS key policy
   only as needed for account `668848431187`.
3. Create or verify the AWS Config delivery channel and configuration recorder in
   `eu-west-2` for account `668848431187`.
4. Start the recorder and verify delivery to the existing central Config archive.
5. Remove only `668848431187` from the migrated
   `org-multi-region-cloudtrail-enabled` organization Config rule exclusion
   list after the recorder is working.
6. Keep `054394900225` excluded on the decommission path.

## Explicitly Out Of Scope

- enabling an AWS Config recorder in `so-aws-admin`;
- enabling GuardDuty, Security Hub, OAM, or other security services;
- changing SCPs;
- moving or renaming accounts;
- modifying workload resources;
- broadening Region scope beyond the accepted `eu-west-2` recorder baseline.

## SAP-C02 Relevance

This supports Domain 1 by preserving the separation between archive storage,
delegated security tooling, and account-purpose decisions. It also reinforces
the governance sequencing pattern: recorder coverage first, then organization
Config rule inclusion, then later security-service migration.

The account-purpose decision for `so-aws-admin` is recorded separately in
`docs/planning/domain-1-so-aws-admin-decommission-decision-20260706.md`.
