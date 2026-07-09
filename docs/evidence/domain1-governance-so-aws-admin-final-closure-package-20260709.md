# Domain 1 so-aws-admin Final Closure Package - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Final closure package prepared for `so-aws-admin` (`054394900225`).

No account closure, account removal, IAM deletion, SCP change, Identity Center
change, service enablement or disablement, Terraform apply, resource deletion,
or billing change was performed.

This package supports a separate go/no-go decision. It does **not** approve or
perform account closure.

## Approval Boundary Used For This Package

Approved scope:

- target account: `so-aws-admin` / `054394900225`;
- management account context: `349687196588`;
- read-only AWS CLI/API checks;
- public-safe evidence capture;
- redacted documentation updates;
- git commit.

`BreakGlassAdminRole` was used only as the existing read-only inspection path
into the target account. Temporary credentials were used in-process and were not
persisted.

## Evidence Files

- `docs/evidence/domain1-governance-so-aws-admin-final-closure-package-20260709-aws-summary.json`
- `docs/evidence/domain1-governance-so-aws-admin-dependency-readiness-20260709.md`
- `docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-readiness-20260709.md`
- `docs/planning/domain-1-so-aws-admin-breakglass-role-decision-20260709.md`
- `docs/evidence/domain1-governance-so-aws-admin-closure-blockers-20260709.md`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-20260709.md`

The final summary file is public-safe: it excludes raw contact values, account
email, credentials, budget details, S3 object data, and private DNS names.

## Live Recheck Summary

Management and Organizations:

- management caller verified as account `349687196588`;
- target account `054394900225` remains `ACTIVE`;
- target account name remains `so-aws-admin`;
- joined method is `CREATED`;
- account remains under the existing `Security OU` parent;
- direct account SCP evidence shows only `FullAWSAccess`;
- account tag count is `0`;
- `so-aws-admin` is not a delegated administrator;
- delegated administrators remain `Security Tooling` (`668848431187`) for AWS
  Config, AWS Config multi-account setup, and GuardDuty;
- no Security Hub delegated administrator is registered.

IAM Identity Center:

- the approved Identity Center instance is active;
- current permission-set names are `AdministratorAccess` and `BreakGlassAdmin`;
- deleted temporary permission set `SoAwsAdminReadOnlyInventory` is absent;
- provisioned permission sets for `054394900225`: `0`;
- account assignments for `054394900225`: `0`.

Target-account identity and IAM:

- read-only target session resolved to `054394900225`;
- root access keys present: `0`;
- root signing certificates present: `0`;
- root MFA enabled: `1`;
- account password present: `1`;
- IAM users: `0`;
- IAM groups: `0`;
- customer-managed IAM policies: `0`;
- instance profiles: `0`;
- server certificates: `0`;
- total IAM roles: `8`.

Remaining roles:

- service-linked roles:
  - `AWSServiceRoleForCloudTrail`
  - `AWSServiceRoleForConfigMultiAccountSetup`
  - `AWSServiceRoleForOrganizations`
  - `AWSServiceRoleForResourceExplorer`
  - `AWSServiceRoleForSSO`
  - `AWSServiceRoleForSupport`
  - `AWSServiceRoleForTrustedAdvisor`
- non-service-linked role:
  - `BreakGlassAdminRole`

`BreakGlassAdminRole` still trusts the management account root principal
(`arn:aws:iam::349687196588:root`), has the AWS managed
`AdministratorAccess` policy attached, and has no inline policies. The recorded
decision remains: retain it only as temporary recovery/closure-path access while
the account is on the decommission path. If the account is retained instead of
closed, remove or redesign the role under separate explicit approval.

Account contact and billing metadata:

- primary contact presence was checked without saving values;
- billing alternate contact: absent;
- operations alternate contact: absent;
- security alternate contact: absent;
- target-account `DescribeBudgets` returned successful empty output, normalized
  as budget count `0`.

Global and regional dependency checks:

- enabled/default regions checked: `17`;
- S3 buckets: `0`;
- Route 53 hosted zones: `0`;
- CloudTrail Lake event-data-store presence: `false`;
- target-owned CloudTrail trails: `0`;
- AWS Config recorders: `0`;
- AWS Config delivery channels: `0`;
- AWS Config rules: `0`;
- GuardDuty detectors: `0`;
- Security Hub enabled regions: `0`;
- OAM sinks: `0`;
- CloudWatch log-group presence: `false`;
- CloudWatch alarm presence: `false`;
- EventBridge default-bus rule presence: `false`;
- SNS topics: `0`;
- AWS Backup vault presence: `false`;
- KMS key presence: `false`;
- regional API errors: none recorded for the approved service set.

The organization CloudTrail `organization-management-events` trail remains
management-owned in `eu-west-2`; no target-owned trail was found.

## Closure Readiness Decision

The account is ready for a separate explicit account-closure approval from the
repository-evidence perspective.

The final package found no active workload, security-service, billing, identity,
DNS, data-retention, or local evidence-preservation dependency in the approved
read-only service set.

Closure must still stop if a fresh pre-close re-run finds any new dependency,
assignment, budget, CloudTrail Lake event data store, target-owned trail, S3
bucket, hosted zone, customer-managed key, log group, alarm, EventBridge rule,
SNS topic, Backup vault, Config recorder/rule, GuardDuty detector, Security Hub
hub, OAM sink, or unexpected IAM principal.

## Blast Radius

Expected impact of closing only `so-aws-admin` (`054394900225`):

- the target account enters the AWS account-closure lifecycle and will no
  longer be usable as an active governance account;
- `BreakGlassAdminRole` and all service-linked roles in the target account
  become inaccessible as part of account closure;
- no IAM Identity Center account assignment is expected to be lost, because no
  assignment is provisioned to this account;
- no local S3 bucket, Route 53 hosted zone, AWS Budget, CloudTrail Lake event
  data store, Config recorder/rule, GuardDuty detector, Security Hub hub, OAM
  sink, CloudWatch log group/alarm, EventBridge rule, SNS topic, KMS key, or
  Backup vault is expected to be lost, because none was found;
- management account `349687196588`, `Security Tooling` (`668848431187`),
  `Security Log Archive` (`955659429518`), the lakehouse workload account
  (`464975959576`), and the container sandbox account (`974893866311`) are not
  in closure scope;
- organization CloudTrail logging is management-owned and not a local target
  account resource.

## Rollback And Reopen Limits

AWS Organizations `CloseAccount` is asynchronous: the close request can return
success while closure is still in progress, and account status should be
checked afterward with `DescribeAccount`.

AWS documents that a closed account can be reopened only during the 90-day
post-closure period by contacting AWS Support. During that period, account
resources are unavailable unless the account is reopened. After 90 days, the
account cannot be reopened and remaining resources are deleted, except for the
special CloudTrail trail behavior documented by AWS. AWS also notes that bills
can still be received after closure for prior usage or remaining commitments.

Sources:

- AWS Organizations `CloseAccount` API:
  <https://docs.aws.amazon.com/organizations/latest/APIReference/API_CloseAccount.html>
- AWS CLI `organizations close-account`:
  <https://docs.aws.amazon.com/cli/latest/reference/organizations/close-account.html>
- AWS Account Management account-closure guide:
  <https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-closing.html>
- AWS CloudTrail account-closure behavior:
  <https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-account-closure.html>

## Evidence Preservation

Preserved outside `so-aws-admin`:

- all repository evidence files listed above;
- prior management-account and target-account sanitized evidence;
- Security Tooling and Security Log Archive governance evidence recorded in the
  existing Domain 1 evidence chain;
- management-owned organization CloudTrail evidence.

Not preserved in this public repository:

- raw primary-contact values;
- account email;
- credential material;
- raw billing-console details;
- private billing/payment instrument details.

Optional private human check before closure:

- open AWS Billing and Account pages for `so-aws-admin` and confirm there are
  no private payment, Marketplace, Reserved Instance, Savings Plan, tax,
  invoice, or support-case concerns that should be handled outside the public
  repository before closure.

## Exact Closure Action For Separate Approval

Do not run this command unless separate explicit account-closure approval is
given for the current task:

```bash
aws organizations close-account \
  --account-id 054394900225 \
  --profile org-admin \
  --region us-east-1
```

Post-close status check:

```bash
aws organizations describe-account \
  --account-id 054394900225 \
  --profile org-admin \
  --region us-east-1
```

Expected status flow:

- immediately after request: `PENDING_CLOSURE` may appear while the asynchronous
  close request is still processing;
- after closure completes: `SUSPENDED`.

Console equivalent:

1. Sign in to the AWS Organizations management account (`349687196588`).
2. Open AWS Organizations.
3. Select AWS accounts.
4. Select `so-aws-admin` (`054394900225`).
5. Choose the close-account action.
6. Enter the account ID requested by AWS and confirm closure.

## Required Pre-Close Stop Conditions

Immediately before any closure command or console action, re-run the final
pre-close checks. Stop and do not close the account if any of the following
appears:

- target account is not `ACTIVE`;
- target account ID or name does not match `054394900225` / `so-aws-admin`;
- any IAM Identity Center assignment or provisioned permission set exists;
- any unexpected IAM user, group, local policy, instance profile, server
  certificate, or non-service-linked role exists;
- any AWS Budget exists;
- any S3 bucket or Route 53 hosted zone exists;
- any target-owned CloudTrail trail or CloudTrail Lake event data store exists;
- any AWS Config recorder, delivery channel, or rule exists;
- any GuardDuty detector, Security Hub hub, or OAM sink exists;
- any CloudWatch log group or alarm exists;
- any EventBridge default-bus rule exists;
- any SNS topic exists;
- any KMS key exists;
- any AWS Backup vault exists;
- any regional read-only check fails unexpectedly;
- any private billing/contact/support concern is found by the human pre-close
  console check.

## SAP-C02 Relevance

This supports Domain 1 by turning account retirement into an evidence-backed
Organizations governance decision with explicit blast-radius and rollback
boundaries. It supports Domain 3 by reducing operational ambiguity before a
destructive cleanup action.
