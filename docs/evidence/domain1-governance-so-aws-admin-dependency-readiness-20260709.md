# Domain 1 so-aws-admin Dependency Readiness Evidence - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Initial management-visible dependency evidence collected; follow-on direct
read-only inventory captured under separate explicit approval.

No AWS resource was created, updated, deleted, moved, enabled, disabled, or
closed. No Terraform plan or apply was run.

This evidence does **not** approve account retirement. The initial
management-visible pass found direct account inventory blocked. A follow-on
temporary Identity Center path captured direct inventory and verified assignment
removal. The remaining blockers are residual role/root posture, budget metadata,
and CloudTrail Lake event-data-store evidence.

## Scope

Target account:

- `so-aws-admin` / `054394900225`

Read-only checks covered:

- Organizations account status, parent placement, SCPs, and tags;
- Organizations service access and delegated-administrator state;
- IAM Identity Center provisioned permission sets and account assignments;
- alternate-contact and primary-contact presence without saving contact values;
- AWS Config organization rule and aggregator status from `Security Tooling`;
- GuardDuty delegated-admin organization configuration and member list from
  `Security Tooling`;
- Security Hub and OAM organization/tooling posture;
- management-account organization CloudTrail state;
- direct assume-role readiness into `so-aws-admin`;
- budgets metadata access readiness.

## Findings

### Cleared By Management-Visible Evidence

- `so-aws-admin` is `ACTIVE`, was `CREATED` in the organization, and remains in
  `Security OU` (`ou-gbyf-mug20ym0`).
- The account has no direct account tags in the Organizations view.
- The account has only the AWS-managed `FullAWSAccess` SCP attached directly;
  `Security OU` also shows only `FullAWSAccess` in this read-only snapshot.
- `so-aws-admin` is not a registered delegated administrator. The registered
  delegated administrator is `Security Tooling` (`668848431187`) for AWS Config
  and GuardDuty.
- No IAM Identity Center permission sets are provisioned to `so-aws-admin`, and
  no account assignments were returned for it.
- `so-aws-admin` is excluded from the migrated organization Config rule
  `org-multi-region-cloudtrail-enabled`.
- The AWS Config aggregator in `Security Tooling` still reports
  `so-aws-admin` as an organization source with `SUCCEEDED` source status.
- GuardDuty in `Security Tooling` has members only for the approved active
  accounts: lakehouse workload, Security Log Archive, and container sandbox.
  `so-aws-admin` is not a GuardDuty member.
- GuardDuty organization auto-enable remains `NONE`, and optional protection
  plans remain disabled.
- No Security Hub delegated administrator is configured, and `Security Tooling`
  is not subscribed to Security Hub.
- OAM sink lists in the management and Security Tooling accounts are empty.
- The organization CloudTrail `organization-management-events` trail is an
  organization trail, is multi-Region, uses log file validation, and is logging.
- No alternate `BILLING`, `OPERATIONS`, or `SECURITY` contacts are configured
  for `so-aws-admin`.
- Primary contact presence was checked without saving values; the public
  evidence records only booleans.

### Initially Not Cleared

- Direct account access was blocked during the initial management-visible pass:
  `sts:AssumeRole` into
  `arn:aws:iam::054394900225:role/OrganizationAccountAccessRole` returned
  `AccessDenied`.
- `aws budgets describe-budgets --account-id 054394900225` from the management
  SSO session returned `AccessDeniedException`; budget and cost metadata still
  require direct account access or a separate private billing review path.

### Still Not Cleared

- `BreakGlassAdminRole`, service-linked-role posture, root/break-glass posture,
  and any final closure treatment still need an explicit decision.
- CloudTrail Lake event-data-store absence is not proven because
  `cloudtrail:ListEventDataStores` was not included in the approved temporary
  read-only policy set.
- Budget metadata is not proven absent because `budgets:ViewBudget` was not
  included in the approved temporary read-only policy set.
- The unassigned `SoAwsAdminReadOnlyInventory` permission set needs a retain or
  delete decision; it is not assigned or provisioned to `054394900225`.

## Evidence Files

Management and Organizations:

- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-management-sts-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-account-status-sanitized-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-parent-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-ou-accounts-sanitized-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-account-attached-scps-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-ou-scps-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-account-tags-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-org-service-access-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-delegated-admins-all-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-target-delegated-services-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-target-delegated-services-20260709.err`

Identity and account contacts:

- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-identity-center-instances-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-identity-center-permission-sets-provisioned-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-identity-center-account-assignments-20260709.jsonl`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-primary-contact-presence-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-alternate-contact-billing-presence-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-alternate-contact-operations-presence-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-alternate-contact-security-presence-20260709.status`

Delegated security services:

- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-delegated-admins-config-amazonaws-com-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-delegated-admins-config-multiaccountsetup-amazonaws-com-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-delegated-admins-guardduty-amazonaws-com-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-delegated-admins-securityhub-amazonaws-com-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-config-aggregators-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-config-aggregator-source-status-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-org-config-rule-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-org-config-rule-so-aws-admin-detailed-status-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-guardduty-org-config-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-guardduty-members-sanitized-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-management-securityhub-admin-accounts-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-securityhub-hub-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-securityhub-hub-20260709.err`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-management-oam-sinks-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-security-tooling-oam-sinks-20260709.json`

Logging, cost, and access blockers:

- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-management-cloudtrail-org-trail-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-management-cloudtrail-org-trail-status-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-assume-role-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-assume-role-20260709.err`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-budgets-metadata-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-budgets-metadata-20260709.err`

## Retirement Readiness Decision

Not ready for retirement.

The management-visible evidence supports keeping `so-aws-admin` excluded from
new Config, GuardDuty, Security Hub, and OAM rollout. It does not yet prove that
the account has no local IAM, data, logs, DNS, cost, recovery, or service
dependencies.

## Required Next Step

Resolve the remaining role, root/break-glass, budget, and CloudTrail Lake
evidence items before any closure package is drafted. The next slice should not
close the account; it should only prove or explicitly waive the remaining
dependencies.

## Follow-On Direct Access Slice

The existing-profile access check is recorded in
`docs/evidence/domain1-governance-so-aws-admin-direct-access-profile-check-20260709.md`.
No configured local AWS profile directly targets `054394900225`, and all usable
configured profiles failed the target `OrganizationAccountAccessRole`
assume-role check.

The planned temporary read-only inventory path is recorded in
`docs/planning/domain-1-so-aws-admin-direct-inventory-access-plan-20260709.md`.
That plan is repo-only documentation; it does not authorize IAM Identity Center,
IAM, SCP, service, or account-retirement changes.

Under separate explicit approval on 2026-07-09, the temporary Identity Center
path was created, direct in-account read-only inventory was captured, and the
target-account assignment was removed and verified. The live evidence is
recorded in
`docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-readiness-20260709.md`.
Retirement is still not ready: `BreakGlassAdminRole`, service-linked-role/root
posture, budget metadata, and CloudTrail Lake event-data-store evidence still
need closure decisions or additional public-safe evidence.

## SAP-C02 Relevance

This supports Domain 1 by turning a decommission-path account into an explicit
evidence-backed governance decision rather than an assumption. It also supports
Domain 3 by identifying the residual operational-control gap before retirement:
no account closure should proceed until dependency evidence is complete and
preservation requirements are known.
