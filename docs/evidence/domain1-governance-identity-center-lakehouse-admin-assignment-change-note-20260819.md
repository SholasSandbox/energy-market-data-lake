# Domain 1 Governance Change Note - Identity Center Lakehouse Administrator

## Status

Completed. The approved group account assignment succeeded, the permission set
was provisioned to the workload account, portal entitlement and fresh temporary
role credentials were verified, and the legacy IAM user remained unchanged.

## Objective

Give the existing normal IAM Identity Center workforce principal the same
effective permissions in workload account `464975959576` as legacy
`IAMUser1`, while preserving the IAM user, console password, access key, and all
other long-lived credentials unchanged.

## Why Console Navigation Failed

IAM Identity Center separates identity from authorization:

1. the user exists in the Identity Center identity store;
2. a permission set defines the role permissions and session duration; and
3. an account assignment connects a user or group, permission set, and AWS
   account.

The normal workforce principal existed and could sign in, but the workload
account had no provisioned permission set and no account assignment. Therefore,
the AWS access portal had no workload-account role to display or launch. This
was an entitlement gap, not a password, MFA, browser, SCP, or IAM-user problem.

## Effective-Permission Comparison

`IAMUser1` has no group or inline policies. It has AWS-managed
`AdministratorAccess` plus five narrower managed policies for DynamoDB, S3,
Elastic Beanstalk, Lambda, and Amplify. Those five policies add no effective
permission because `AdministratorAccess` already allows all actions and
resources within the account's organization guardrails.

The existing Identity Center `AdministratorAccess` permission set has:

- a one-hour session duration;
- only the AWS-managed `AdministratorAccess` policy;
- no inline policy;
- no customer-managed policy reference; and
- no permissions boundary.

It therefore matches the effective identity-based access of `IAMUser1` without
copying redundant policies or long-lived credentials.

## Principal and Assignment Design

The target is the existing normal workforce principal, represented through the
`cloud-lab-aws-admins` group. Fresh inventory confirmed that the group contains
exactly one user and does not contain the dedicated break-glass principal.

The approved assignment tuple is:

- account: `464975959576` / `Olusola_AWS`;
- principal type: group;
- group: `cloud-lab-aws-admins`;
- permission set: `AdministratorAccess`; and
- session duration: one hour.

The direct emergency `BreakGlassAdmin` path remains unchanged and is not used
for routine navigation.

## SCP Evaluation

`DenyRootUserActions-LakehouseWorkloads` applies only when
`aws:PrincipalArn` matches an account root principal. An Identity Center session
uses an AWS-reserved role principal, so the root-only condition does not block
this assignment. SCPs still define the account's maximum permissions and do not
grant access by themselves.

## Rollback

Delete only the matching account assignment connecting
`cloud-lab-aws-admins`, `AdministratorAccess`, and workload account
`464975959576`. Do not delete the permission set, group, user, IAM user,
password, access key, or account.

## Implemented Change

The existing `AdministratorAccess` permission set was assigned to the existing
`cloud-lab-aws-admins` group for workload account `464975959576`. IAM Identity
Center returned `SUCCEEDED` for assignment request
`3eb971e5-2714-4ef9-865c-142bee9ce5d3`.

No new user, group, permission set, policy, IAM user, access key, password, MFA
device, SCP, or AWS account was created. No existing legacy credential or
emergency assignment was modified.

## Validation Results

1. Assignment creation reached `SUCCEEDED`.
2. Workload account `464975959576` lists exactly one provisioned permission
   set: `AdministratorAccess`.
3. The account assignment appears exactly once for
   `cloud-lab-aws-admins`.
4. The signed-in Identity Center access token lists `AdministratorAccess` for
   the workload account.
5. A fresh one-hour role session identified itself as the
   `AWSReservedSSO_AdministratorAccess` role in account `464975959576`.
6. Representative read-only EC2 and S3 navigation succeeded with the temporary
   role credentials.
7. Postchange IAM reads confirmed that `IAMUser1`, all six attached managed
   policies, its active access key, and its console login profile remain
   present and unchanged.
8. CloudTrail recorded `CreateAccountAssignment` as event
   `2402fa12-971c-469b-941c-f87795057402`.

The structured prechange and postchange records are:

- `docs/evidence/domain1-governance-identity-center-lakehouse-admin-assignment-precheck-20260819.json`
- `docs/evidence/domain1-governance-identity-center-lakehouse-admin-assignment-postchange-20260819.json`

## Console Navigation Routine

1. Open the organization's AWS access portal, not the account-specific IAM-user
   sign-in page.
2. Sign in with the normal Identity Center workforce user and complete MFA if
   prompted.
3. Open **Accounts** and select `Olusola_AWS` / `464975959576`.
4. Select the `AdministratorAccess` role.
5. Confirm the console header shows the workload account and an
   `AWSReservedSSO_AdministratorAccess` role session.
6. Select the required Region, such as `eu-west-2`, before navigating services.
7. When the one-hour role session expires, return to the access portal and open
   the role again; do not create or paste long-lived access keys.

For CLI practice, configure a separate named SSO profile with the access portal
start URL, SSO Region `eu-west-2`, workload account `464975959576`, and role
`AdministratorAccess`. Authenticate with `aws sso login` and verify the account
before every change with `aws sts get-caller-identity`.

## SAP-C02 Relevance

This is Domain 1 governance evidence for the modern workforce-access pattern:
central identity, group-based account assignment, permission-set-driven IAM
roles, short-lived credentials, separate emergency access, and preserved SCP
guardrails.
