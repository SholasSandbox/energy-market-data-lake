# Domain 1 IAM Identity Center SecurityToolingAdmin Design - 2026-07-12

<!-- markdownlint-disable MD013 -->

## Status

Accepted design with migration stage 1 implemented on 2026-07-12.
`SecurityToolingAdmin` and its Security Tooling group assignment are live, and
the read-only entitlement path is proven. The representative write test and
removal of temporary broad `AdministratorAccess` remain open. Implementation
evidence is recorded in
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-staged-assignment-change-note-20260712.md`.

The GuardDuty representative write call subsequently succeeded with unchanged
postconditions, but its immediate CloudTrail Event History evidence is still
missing. The audit portion of the gate and broad-role removal therefore remain
open; see
`docs/evidence/domain1-governance-identity-center-security-tooling-admin-guardduty-write-test-20260712.md`.

## Decision

Use `SecurityToolingAdmin` as the eventual replacement
for the temporary broad `AdministratorAccess` assignment in `Security Tooling`
account `668848431187`.

| Item | Design |
|---|---|
| Permission set | `SecurityToolingAdmin` |
| Session duration | `PT1H` |
| Principal | Existing `security-tooling-admins` group only |
| Target | `Security Tooling` (`668848431187`) only |
| Policy | `docs/policies/iam-identity-center-security-tooling-admin.inline-policy.example.json` only |
| Region | `eu-west-2` for Config, GuardDuty, and CloudTrail operations |
| Assignment strategy | Add and prove before removing broad `AdministratorAccess` |

The mature portal model is:

- choose `SecurityAudit` for inspection and evidence collection;
- choose `SecurityToolingAdmin` for approved Config or GuardDuty changes;
- use the temporary broad `AdministratorAccess` path only until the custom
  path is proven and removed;
- keep `BreakGlassAdmin` separate and management-account-only.

## Allowed Operating Boundary

The proposed policy supports the existing Security Tooling responsibilities:

- read organization account and delegated-service context;
- inspect the Config aggregator, organization Config rule, recorder, delivery
  channel, aggregate resources, and compliance state;
- update the existing Config aggregator, organization rule, recorder, and
  delivery channel in `eu-west-2`;
- pass only the two existing Config roles, and only to
  `config.amazonaws.com`;
- inspect and operate GuardDuty organization configuration, members, member
  detector features, findings, coverage, and usage in `eu-west-2`; and
- inspect local CloudTrail event history for change evidence.

Some AWS Config write APIs can create a resource when it is absent. The live
change procedure must therefore verify the expected named baseline immediately
before use and stop on absence or drift.

## Explicit Exclusions

The policy deliberately excludes or explicitly denies:

- Organizations trusted-access and delegated-administrator registration;
- account, OU, and SCP administration;
- IAM role or policy creation, attachment, update, or deletion;
- GuardDuty detector update or deletion, delegated-admin enablement or
  disablement, and member teardown;
- Config recorder stop and Config resource deletion;
- central `Security Log Archive` S3 bucket or KMS key administration;
- Security Hub and OAM;
- access to the management, lakehouse, sandbox, or log archive accounts; and
- any Region other than `eu-west-2` for regional service operations.

These exclusions preserve the split between management-account control-plane
work, storage-only log archive administration, and routine delegated-security
operations. A rare bootstrap or teardown task needs its own approval and a
separate temporary path; it must not silently widen this permission set.

## AWS Authorization Caveat

AWS lists `organizations:EnableAWSServiceAccess`,
`organizations:ListDelegatedAdministrators`, `iam:CreateServiceLinkedRole`,
and `iam:PassRole` as dependent actions for some Config organization APIs. This
design grants only organization read access and tightly scoped `iam:PassRole`.
It denies trusted-access and service-linked-role mutation because those are
management/bootstrap responsibilities and the required live prerequisites
already exist.

The future validation must stop if an approved Config update requires either
excluded action. Do not broaden this routine role merely to make a single
bootstrap operation succeed.

## Validation Cases

| Case | Expected result |
|---|---|
| Assume `SecurityToolingAdmin` through the workforce portal | Allowed in Security Tooling only |
| List Config aggregator, recorder, delivery channel, organization rule, and detailed status | Allowed in `eu-west-2` |
| Query aggregate Config resources and compliance | Allowed in `eu-west-2` |
| Pass `aws-config-organization-aggregator-role` to Config | Allowed |
| Pass any other role or pass either role to another service | Denied |
| List GuardDuty detector, members, findings, coverage, and usage | Allowed in `eu-west-2` |
| Update approved GuardDuty organization/member feature settings | Allowed in `eu-west-2` |
| Delete or update the GuardDuty detector | Denied |
| Stop/delete Config recorder or delete aggregator/rule/channel | Denied |
| Register/deregister a delegated administrator or change trusted access | Denied |
| Create or modify IAM roles or policies | Denied |
| Change archive S3/KMS policies, Security Hub, OAM, SCPs, or another account | Denied |
| Perform Config, GuardDuty, or CloudTrail operations outside `eu-west-2` | Denied |

Before any live assignment, validate the JSON with IAM Access Analyzer and
simulate representative allowed and denied actions. A live proof should then
use fresh read-only state and one separately approved, reversible Config or
GuardDuty operation. Read-only checks alone prove entitlement, not the write
path.

## Policy Validation

Documentation-time validation completed on 2026-07-12 without creating or
changing AWS resources:

- `jq` accepted the policy as valid JSON;
- IAM Access Analyzer `ValidatePolicy` returned zero findings;
- simulation allowed Config and GuardDuty read operations, Config aggregator
  and organization-rule updates, GuardDuty organization-configuration update,
  organization read context, and passing the named aggregator role only to
  Config;
- simulation explicitly denied GuardDuty detector deletion, Config recorder
  stop, delegated-administrator registration, IAM role creation, and Security
  Hub enablement;
- simulation implicitly denied Config access outside `eu-west-2`, passing an
  unapproved role, and passing the approved role to GuardDuty.

Simulation evaluates the policy document, not live service prerequisites or
request payloads. The dependent-action caveat and approved live proof therefore
remain mandatory before replacement.

## Migration And Rollback

Use two separately reviewable stages:

1. Create `SecurityToolingAdmin` with `PT1H` and only the prepared inline
   policy, assign the existing `security-tooling-admins` group to Security
   Tooling, and leave broad `AdministratorAccess` unchanged.
2. After portal, simulation, CloudTrail, Config, and GuardDuty validation,
   remove only the Security Tooling `AdministratorAccess` group assignment.

Stage 1 is complete. Portal assumption and read-only Config, GuardDuty, IAM,
CloudTrail, and Organizations checks passed. The write proof and stage 2 broad
assignment removal remain separately gated. The selected idempotent GuardDuty
write succeeded and preserved all observed postconditions, but the audit-event
evidence must be collected before considering stage 2.

Rollback before broad-role removal is deletion of only the new
`SecurityToolingAdmin` account assignment. Rollback after broad-role removal is
temporary restoration of the previous account assignment under explicit
approval, followed by correction of the custom policy. Do not change the
auditor or break-glass paths.

## Cost And Audit Impact

This design has no AWS cost or runtime effect. A future permission-set change
creates CloudTrail administrative events. Config snapshot delivery, recorder
scope changes, GuardDuty member/feature changes, and other live operations may
affect cost or evidence and require their own approved change boundary.

## References

- [AWS IAM Identity Center permission sets](https://docs.aws.amazon.com/singlesignon/latest/userguide/permissionsetsconcept.html)
- [AWS Config authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_awsconfig.html)
- [Amazon GuardDuty authorization reference](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonguardduty.html)
- [IAM security best practices](https://docs.aws.amazon.com/IAM/latest/UserGuide/best-practices.html)

## SAP-C02 Relevance

This supports Domain 1 by converting an initial broad federated administrator
path into a task-specific permission-set design. It demonstrates workforce
federation, temporary credentials, group assignment, least privilege,
delegated administration, separation of duties, Region scoping, explicit
negative permissions, staged migration, validation, and rollback.
