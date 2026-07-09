# Domain 1 so-aws-admin Account Closure Evidence - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

`so-aws-admin` (`054394900225`) was closed through AWS Organizations under
explicit approval on 2026-07-09.

Only this account was closed. No AWS Organization removal, management-account
closure, SCP change, IAM change other than the account-closure action itself,
service enablement or disablement, Terraform apply, billing-policy expansion,
or resource deletion outside account closure was performed.

## Approval Boundary

Approved closure action:

- close only AWS account `so-aws-admin` / `054394900225`;
- use Organizations management account `349687196588`;
- run AWS Organizations `CloseAccount` for account `054394900225`;
- immediately re-run the final pre-close checks from
  `docs/evidence/domain1-governance-so-aws-admin-final-closure-package-20260709.md`;
- stop before closure if any new dependency, active resource, unexpected
  assignment, budget, CloudTrail Lake event data store, or preservation issue
  appeared;
- save public-safe evidence and commit the result.

## Evidence Files

- Pre-close check:
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-precheck-20260709.json`
- CloseAccount request result:
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-close-account-result-20260709.json`
- Post-close account status:
  `docs/evidence/domain1-governance-so-aws-admin-account-closure-postclose-status-20260709.json`
- Final closure package reviewed before approval:
  `docs/evidence/domain1-governance-so-aws-admin-final-closure-package-20260709.md`

All saved evidence is public-safe. It excludes raw contact values, account
email, credentials, billing details, S3 object data, and private DNS names.

## Fresh Pre-Close Result

The fresh pre-close check returned:

- blocker count: `0`;
- warning count: `0`;
- regions checked: `17`;
- IAM Identity Center provisioned permission sets: `0`;
- IAM Identity Center account assignments: `0`;
- `SoAwsAdminReadOnlyInventory` permission set exists: `false`;
- AWS Budgets: `0`;
- S3 buckets: `0`;
- Route 53 hosted zones: `0`;
- CloudTrail Lake event data stores: none found;
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
- regional API error lists: empty for the approved service set.

Expected residuals before closure:

- service-linked roles remained as account/service scaffolding;
- `BreakGlassAdminRole` remained as the approved temporary recovery/closure-path
  role.

No stop condition was present.

## Closure Request

Command executed:

```bash
aws organizations close-account \
  --account-id 054394900225 \
  --profile org-admin \
  --region us-east-1
```

Result:

- exit status: `0`;
- success: `true`;
- stdout: empty, matching the AWS Organizations API contract for a successful
  `CloseAccount` call;
- stderr: empty.

## Post-Close Status

`DescribeAccount` polling returned the closed account state:

- account ID: `054394900225`;
- account name: `so-aws-admin`;
- final observed status: `SUSPENDED`;
- final observed state: `CLOSED`;
- observation count: `12`.

AWS documents `CloseAccount` as asynchronous. The final observed status/state
above is the repository evidence that the account has entered the closed account
lifecycle.

## Closure Impact

Expected impact now realized:

- `so-aws-admin` is no longer usable as an active governance account;
- the target account's `BreakGlassAdminRole` and service-linked roles are no
  longer usable as active operating paths;
- no IAM Identity Center assignment was removed, because none was provisioned
  to the account at closure time;
- no local S3 bucket, Route 53 hosted zone, AWS Budget, CloudTrail Lake event
  data store, Config recorder/rule, GuardDuty detector, Security Hub hub, OAM
  sink, CloudWatch log group/alarm, EventBridge rule, SNS topic, KMS key, or
  Backup vault was expected to be lost because none was found;
- management account `349687196588`, `Security Tooling` (`668848431187`),
  `Security Log Archive` (`955659429518`), the lakehouse workload account
  (`464975959576`), and the container sandbox account (`974893866311`) were not
  in scope and were not closed.

## Reopen And Retention Notes

AWS documents that during the 90-day post-closure period, resources in the
account are unavailable unless AWS Support reopens the account. After 90 days,
the account cannot be reopened and remaining resources are deleted, except for
the special CloudTrail trail behavior documented by AWS. AWS also documents
that bills can still arrive after closure.

Sources:

- AWS Organizations `CloseAccount` API:
  <https://docs.aws.amazon.com/organizations/latest/APIReference/API_CloseAccount.html>
- AWS CLI `organizations close-account`:
  <https://docs.aws.amazon.com/cli/latest/reference/organizations/close-account.html>
- AWS Account Management account-closure guide:
  <https://docs.aws.amazon.com/accounts/latest/reference/manage-acct-closing.html>
- AWS CloudTrail account-closure behavior:
  <https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-account-closure.html>

## Follow-Up

- Keep this evidence outside the closed account.
- Do not attempt to use `so-aws-admin` for future governance services.
- If AWS Support reopening is ever required during the 90-day window, record a
  separate explicit approval and support-case evidence.
- Continue Domain 1 governance work in the surviving management, Security
  Tooling, Security Log Archive, lakehouse workload, and sandbox accounts.

## SAP-C02 Relevance

This supports Domain 1 by completing an evidence-backed account-decommission
decision inside AWS Organizations. It supports Domain 3 by reducing operational
ambiguity and removing an unused privileged account surface after dependency
checks passed.
