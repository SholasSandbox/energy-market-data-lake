# Domain 1 Governance Evidence - SecurityAudit Direct-Access Precheck - 2026-07-11

<!-- markdownlint-disable MD013 -->

## Status

Fresh read-only precheck captured from the active management-account SSO
session. No IAM Identity Center users, groups, permission sets, assignments,
IAM roles, policies, or AWS accounts were created, modified, assigned, or
removed.

## Current State

- The management-account SSO caller is valid.
- One IAM Identity Center instance is `ACTIVE`.
- The only current permission sets are `AdministratorAccess` and
  `BreakGlassAdmin`, both with one-hour sessions and the AWS-managed
  `AdministratorAccess` policy.
- The normal Workforce Identity is enabled and remains a member of the two
  existing administrator groups.
- The proposed `security-tooling-auditors` group does not exist.
- Security Tooling has no assignment under either existing permission set.

## Policy Decision

The current AWS-managed `SecurityAudit` policy default is `v90`, updated on
2026-07-09. It includes actions beyond passive inspection, including
`config:Deliver*`, `iam:GenerateCredentialReport`, and
`iam:GenerateServiceLastAccessedDetails`. `config:DeliverConfigSnapshot` can
schedule delivery of an AWS Config snapshot, and IAM report generation starts
report-generation work.

The prepared direct-access change therefore rejects the AWS-managed policy for
this first read-only assignment. It instead uses the custom no-mutation inline
policy in
`docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json`.
IAM Access Analyzer validated that custom policy with no findings.
The policy permits inspection of Security Tooling's AWS Config, GuardDuty,
CloudTrail, CloudWatch Logs, IAM, S3, KMS, CloudWatch, and tag posture without
granting Config snapshot delivery, IAM report generation, data-object reads,
or security-service configuration changes.

## Boundary

The target account is Security Tooling only. Its AWS Config aggregator and
GuardDuty delegated-administrator functions can expose read-only metadata for
member accounts; that visibility is intentional for the security reviewer, but
does not grant a role in those member accounts or permission to change their
resources.

Immediately before any approved execution, repeat this inventory and policy
validation. This precheck was repeated successfully before the approved
assignment; post-change evidence is recorded in
`docs/evidence/domain1-governance-identity-center-security-audit-assignment-change-note-20260711.md`.
