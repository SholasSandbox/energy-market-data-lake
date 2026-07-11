# Domain 1 IAM Identity Center SecurityAudit Direct-Access Change Note - 2026-07-11

<!-- markdownlint-disable MD013 -->

## Status

Prepared for a single bounded IAM Identity Center change. This document does
not approve or perform the change. Its fresh read-only precheck is recorded in
`docs/evidence/domain1-governance-identity-center-security-audit-precheck-20260711.md`.

Repeat the precheck immediately before execution. This evidence does not
substitute for the execution-session baseline.

## Purpose

Provide the existing normal Workforce Identity with direct, individual sign-in
to the Security Tooling account through the AWS access portal. The resulting
access must be read-only, limited to one account, and separate from both the
normal management-administrator path and the emergency break-glass path.

This implements the first routine-access candidate accepted in
`docs/planning/domain-1-identity-center-assignment-decision-20260710.md`.

## Exact Proposed Change

| Item | Proposed value |
|---|---|
| IAM Identity Center group | New `security-tooling-auditors` group |
| Initial member | Existing normal Workforce Identity, `org-admin-principal` only |
| Permission set | New `SecurityAudit` |
| Session duration | `PT1H` |
| Policy attachment | Custom no-mutation inline policy: `docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json` |
| AWS-managed, customer-managed, and permissions-boundary policy | None |
| Target account | `Security Tooling` / `668848431187` in `Security OU` |
| Assignment | `security-tooling-auditors` group plus `SecurityAudit` in Security Tooling only |
| Expected portal result | The normal Workforce Identity sees a Security Tooling `SecurityAudit` role in the AWS access portal |

Do not add the emergency Workforce Identity, reuse `cloud-lab-aws-admins` or
`sandbox-cloud-admins`, assign another account, attach another policy, alter
`BreakGlassAdmin`, or make an SCP, Organizations, AWS Config, GuardDuty,
Security Hub, OAM, root, or budget change in this change unit.

The assignment provisions an IAM Identity Center-managed
`AWSReservedSSO_SecurityAudit_*` role in Security Tooling. The direct role is
limited to that account, but its AWS Config aggregator and GuardDuty delegated
administrator can expose read-only member-account metadata by design.

## Rationale And Policy Boundary

The dedicated group prevents the current management-administrator groups from
becoming an implicit Security Tooling access mechanism. The target account is
the active AWS Config and GuardDuty delegated-administrator account, so
read-only security posture and evidence review is the smallest useful initial
access path.

The AWS-managed `SecurityAudit` policy is not suitable for this first strict
read-only role. The 2026-07-11 precheck found version `v90`, updated on
2026-07-09, includes `config:Deliver*`, IAM report generation, and other
actions beyond passive inspection. A Config snapshot delivery can cause a
snapshot to be delivered to the configured archive and IAM report generation
starts report-generation work.

The proposed custom inline policy contains only the current Security Tooling
review actions and excludes those side effects, object data reads, and all
service configuration actions. AWS-managed-policy version changes therefore do
not broaden the access path. Immediately before execution, rerun the managed
policy comparison and validate the custom policy:

```bash
aws iam get-policy \
  --policy-arn arn:aws:iam::aws:policy/SecurityAudit \
  --profile org-admin \
  --output json

aws accessanalyzer validate-policy \
  --policy-type IDENTITY_POLICY \
  --policy-document file://docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json \
  --profile org-admin \
  --output json
```

The managed-policy reference is documented by AWS at
<https://docs.aws.amazon.com/aws-managed-policy/latest/reference/SecurityAudit.html>.

## Latest Recorded Baseline And Execution Preconditions

The fresh 2026-07-11 precheck confirms only `AdministratorAccess` and
`BreakGlassAdmin` exist, neither is assigned in Security Tooling, the normal
Workforce Identity is enabled, and `security-tooling-auditors` does not exist.
The normal identity remains in the two existing administrator groups; neither
may be reused for this access path.

Before approval or execution, refresh the SSO token and save redacted,
read-only evidence for all of the following:

1. management caller identity and active IAM Identity Center instance;
2. permission-set names, descriptions, session durations, AWS-managed policy
   attachments, inline policies, customer-managed policy references, and
   permissions boundaries;
3. existence check for `security-tooling-auditors` and the current membership
   of the named normal Workforce Identity;
4. current `AdministratorAccess` and `BreakGlassAdmin` assignment counts in
   Security Tooling; and
5. the current AWS-managed `SecurityAudit` policy default version and document;
   and
6. an Access Analyzer validation result with no `ERROR` or `SECURITY_WARNING`
   for the custom inline policy.

Stop if `SecurityAudit` or `security-tooling-auditors` already exists, if the
Security Tooling account already has any unexpected assignment, or if policy
validation reports an error or security warning. Reconcile the difference in a
new decision note rather than overwriting or extending the existing state.

## Execution Sequence After Separate Approval

Use one permission set in one account and pause after each operation returns
success:

1. Create `security-tooling-auditors` with no pre-existing members.
2. Add only `org-admin-principal` to that group.
3. Create `SecurityAudit` with `PT1H` and apply only the approved custom
   inline policy.
4. Create the group assignment in Security Tooling.
5. Capture post-change evidence before signing in through the access portal.

The exact API calls, returned identifiers, timestamps, and raw outputs belong
in private working evidence or redacted artifacts. Do not commit email
addresses, user IDs, access keys, SSO tokens, or raw credential output.

## Execution Commands After Separate Approval

Run only after the precheck and approval gate pass. Resolve the current
identifiers rather than reusing values from an earlier session:

```bash
INSTANCE_ARN="$(aws sso-admin list-instances --profile org-admin \
  --query 'Instances[0].InstanceArn' --output text)"
IDENTITY_STORE_ID="$(aws sso-admin list-instances --profile org-admin \
  --query 'Instances[0].IdentityStoreId' --output text)"
TARGET_ACCOUNT_ID="668848431187"
USER_ID="$(aws identitystore list-users \
  --identity-store-id "$IDENTITY_STORE_ID" \
  --filters AttributePath=UserName,AttributeValue=shola-cloud-lab-admin \
  --profile org-admin --query 'Users[0].UserId' --output text)"

GROUP_ID="$(aws identitystore create-group \
  --identity-store-id "$IDENTITY_STORE_ID" \
  --display-name security-tooling-auditors \
  --description 'Read-only Security Tooling review access' \
  --profile org-admin --query GroupId --output text)"

aws identitystore create-group-membership \
  --identity-store-id "$IDENTITY_STORE_ID" \
  --group-id "$GROUP_ID" \
  --member-id "UserId=$USER_ID" \
  --profile org-admin

PERMISSION_SET_ARN="$(aws sso-admin create-permission-set \
  --instance-arn "$INSTANCE_ARN" \
  --name SecurityAudit \
  --description 'Read-only Security Tooling security posture review' \
  --session-duration PT1H \
  --profile org-admin --query 'PermissionSet.PermissionSetArn' --output text)"

aws sso-admin put-inline-policy-to-permission-set \
  --instance-arn "$INSTANCE_ARN" \
  --permission-set-arn "$PERMISSION_SET_ARN" \
  --inline-policy file://docs/policies/iam-identity-center-security-audit-security-tooling.inline-policy.example.json \
  --profile org-admin

REQUEST_ID="$(aws sso-admin create-account-assignment \
  --instance-arn "$INSTANCE_ARN" \
  --target-id "$TARGET_ACCOUNT_ID" \
  --target-type AWS_ACCOUNT \
  --permission-set-arn "$PERMISSION_SET_ARN" \
  --principal-type GROUP \
  --principal-id "$GROUP_ID" \
  --profile org-admin \
  --query 'AccountAssignmentCreationStatus.RequestId' --output text)"

aws sso-admin describe-account-assignment-creation-status \
  --instance-arn "$INSTANCE_ARN" \
  --account-assignment-creation-request-id "$REQUEST_ID" \
  --profile org-admin --output json
```

If any command before `create-account-assignment` fails, stop without retrying
an alternate scope. If the assignment request fails, retain the failure output
as private evidence and investigate before attempting any rollback or retry.

## Validation

After the assignment is provisioned, verify:

1. `SecurityAudit` has the approved session duration and only the approved
   inline policy, with no AWS-managed, customer-managed, or boundary policy.
2. Security Tooling has exactly one new `SecurityAudit` group assignment and
   no changed `AdministratorAccess` or `BreakGlassAdmin` assignment.
3. The normal Workforce Identity can sign out and back in to the AWS access
   portal, select Security Tooling, and open only the `SecurityAudit` role.
4. Read-only GuardDuty detector listing and AWS Config recorder/aggregator
   descriptions succeed in `eu-west-2`.
5. A prohibited mutation attempt is not made. Permission inspection and
   CloudTrail evidence are the validation boundary for the first slice.

## Rollback

The access rollback is to delete only the Security Tooling
`SecurityAudit` group assignment, then verify the account no longer appears
for that role after the Workforce Identity signs out and back in. This leaves
the existing management and break-glass paths untouched.

Do not delete the new group or permission set as part of the access rollback.
They grant no Security Tooling access without the assignment and any cleanup is
a separate, reviewable change. Record the rollback result and reason before
considering a retry.

After capturing the post-change failure or validation evidence, use the
assignment deletion request and wait for its terminal status:

```bash
DELETE_REQUEST_ID="$(aws sso-admin delete-account-assignment \
  --instance-arn "$INSTANCE_ARN" \
  --target-id "$TARGET_ACCOUNT_ID" \
  --target-type AWS_ACCOUNT \
  --permission-set-arn "$PERMISSION_SET_ARN" \
  --principal-type GROUP \
  --principal-id "$GROUP_ID" \
  --profile org-admin \
  --query 'AccountAssignmentDeletionStatus.RequestId' --output text)"

aws sso-admin describe-account-assignment-deletion-status \
  --instance-arn "$INSTANCE_ARN" \
  --account-assignment-deletion-request-id "$DELETE_REQUEST_ID" \
  --profile org-admin --output json
```

## Cost, Audit, And Notification Impact

No new data-plane service, GuardDuty protection plan, or security-service
delegation is proposed. The custom policy excludes the AWS-managed policy's
Config snapshot-delivery and IAM report-generation actions, so no direct
workload cost or delivery side effect is expected from its permitted actions.
Confirm current IAM Identity Center pricing before execution.

The change creates IAM Identity Center administrative events that must be
preserved by the existing organization CloudTrail. No new user is created, so
no invitation email is expected. The normal Workforce Identity may need to
sign out and in again before the new account role is visible.

## Approval Required

Execution requires a separate explicit approval that names this exact scope:

> Create `security-tooling-auditors`, add only `org-admin-principal`, create
> `SecurityAudit` with `PT1H` and only the approved custom no-mutation inline
> policy, and assign that group to Security Tooling `668848431187` only, after
> fresh read-only prechecks and with the rollback and validation steps in this
> note.

## SAP-C02 Relevance

This provides auditable, least-privilege human access to a delegated security
administrator while keeping routine, emergency, and management-account access
separate. It supports Domain 1 account governance and Domain 3 operational
evidence collection.
