# Domain 1 so-aws-admin Decommission Decision - 2026-07-06

<!-- markdownlint-disable MD013 -->

## Status

Decision recorded. No live AWS change is authorized or performed by this note.

## Decision

Place `so-aws-admin` (`054394900225`) on the decommission path.

Retire the account only after read-only dependency checks are completed and all
identified dependencies are resolved. Until then, keep the account excluded from
new governance-service rollout and do not use it as a durable governance account.

Security Hub, if later adopted, belongs in `Security Tooling` (`668848431187`),
not in `so-aws-admin`.

## Rationale

This avoids keeping a legacy placeholder account as technical debt. The current
accepted governance model already has clearer account ownership:

- `Security Log Archive` (`955659429518`) owns durable CloudTrail and AWS Config
  archive storage, KMS keys, retention controls, and log-storage evidence.
- `Security Tooling` (`668848431187`) owns active delegated security tooling such
  as AWS Config aggregation, GuardDuty delegated administration if adopted,
  Security Hub if later adopted, OAM if useful, and read-only investigation
  tooling.

Keeping `so-aws-admin` without a unique durable purpose would create an unclear
operating owner, extra access surface, avoidable cost risk, and split-brain
security administration.

## Required Read-Only Dependency Checks Before Retirement

Before any retirement or closure action is approved, collect read-only evidence
for:

1. Organizations parent, attached SCPs, tags, and account status.
2. Delegated-administrator registrations and trusted-service relationships.
3. IAM Identity Center account assignments and any direct IAM users, roles,
   policies, or access keys.
4. AWS Config recorders, delivery channels, aggregators, organization rules, and
   Config rule exclusions.
5. GuardDuty, Security Hub, OAM, CloudTrail, CloudWatch, EventBridge, SNS, S3,
   KMS, Route 53, and any other active service dependencies.
6. Billing, Budgets, Cost Explorer, support, alternate contacts, and primary
   contact dependencies.
7. Any data, logs, evidence, domains, email, or recovery paths that must be
   preserved before closure.

## Retirement Gates

Do not retire or close the account until:

- no active workload, security-service, billing, identity, DNS, data-retention,
  or recovery dependency remains;
- any required evidence or logs have been preserved outside the account;
- the closure blast radius and rollback limitations are documented;
- the final closure action receives separate explicit approval.

## Explicitly Out Of Scope

- enabling AWS Config, GuardDuty, Security Hub, OAM, or any other new service in
  `so-aws-admin`;
- moving active security tooling into `so-aws-admin`;
- closing, suspending, or moving the account without a separate approved
  dependency-check and retirement change note;
- changing SCPs, workload resources, or existing delegated administration.

## SAP-C02 And AWS Well-Architected Framework Relevance

This supports SAP-C02 Domain 1 by keeping account purpose, delegated
administration, and separation of duties explicit. It also aligns with the AWS
Well-Architected Framework security, operational excellence, and cost
optimization pillars: reduce unused privileged surfaces, simplify ownership,
avoid unnecessary service spend, and retire resources only after dependency and
evidence checks are clear.

Write out `AWS Well-Architected Framework` in this repository instead of using
`WAF` for that framework, because `AWS WAF` commonly refers to the AWS Web
Application Firewall service.
