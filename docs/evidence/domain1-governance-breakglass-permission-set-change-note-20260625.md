# Domain 1 Governance Change Note - Create And Assign BreakGlassAdmin Permission Set - 2026-06-25

<!-- markdownlint-disable MD013 -->

## Status

Executed successfully as the next bounded IAM Identity Center live change for
the Domain 1 governance slice.

## Target Scope

- IAM Identity Center instance:
  `arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7`
- management account target:
  `349687196588` / `management-account-alias`
- emergency principal:
  `breakglass-principal` / `[redacted-email]`

## Current State Before Change

Same-day baseline evidence showed:

- the IAM Identity Center instance was `ACTIVE`;
- the dedicated break-glass user already existed and had one enrolled MFA
  device in same-day console evidence;
- only one permission set was yet evidenced live:
  `AdministratorAccess`;
- no dedicated `BreakGlassAdmin` permission set or management-account
  assignment was yet recorded for the emergency principal.

## Executed Change

Created one new permission set:

- name: `BreakGlassAdmin`
- session duration: `PT1H`
- description:
  `Emergency recovery access for management-account Organizations and SCP administration`

Attached one AWS-managed policy to that permission set:

- `arn:aws:iam::aws:policy/AdministratorAccess`

Created one direct account assignment:

- target account: `349687196588`
- principal type: `USER`
- principal:
  `663212d4-7091-70ab-ecc7-f4c876ab9bf0` / `breakglass-principal`

This is intentionally the smallest live emergency-access implementation:

- management account only;
- one-hour session duration;
- one emergency user only;
- no workload-account, sandbox-account, or security-account expansion;
- no group-based expansion yet.

## Expected Blast Radius

- Adds one new emergency IAM Identity Center access path in the management
  account only.
- Does not change OU structure, SCP attachments, CloudTrail, AWS Config,
  workload-account IAM, or sandbox-account access.
- Uses broad AWS-managed administrator authority inside that one permission set,
  so the principal must remain dormant outside real recovery scenarios.

## Rollback Path

If this staged permission-set path must be removed:

1. Delete the management-account assignment for `breakglass-principal`.
2. Detach the AWS-managed policy from `BreakGlassAdmin`.
3. Delete the `BreakGlassAdmin` permission set if no longer needed.

Read-only validation after rollback:

- `list-account-assignments` for management account `349687196588` and the
  `BreakGlassAdmin` permission-set ARN returns no assignments;
- `list-managed-policies-in-permission-set` returns no attached policy if the
  set is retained;
- `list-permission-sets` no longer includes the `BreakGlassAdmin` ARN if the
  set is deleted.

## Validation

Executed commands:

```bash
aws sso-admin create-permission-set \
  --profile org-admin \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --name BreakGlassAdmin \
  --description "Emergency recovery access for management-account Organizations and SCP administration" \
  --session-duration PT1H \
  --output json

aws sso-admin attach-managed-policy-to-permission-set \
  --profile org-admin \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --permission-set-arn arn:aws:sso:::permissionSet/ssoins-7535b4aeae7b57d7/ps-7535514a7384530d \
  --managed-policy-arn arn:aws:iam::aws:policy/AdministratorAccess

aws sso-admin create-account-assignment \
  --profile org-admin \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --target-id 349687196588 \
  --target-type AWS_ACCOUNT \
  --permission-set-arn arn:aws:sso:::permissionSet/ssoins-7535b4aeae7b57d7/ps-7535514a7384530d \
  --principal-type USER \
  --principal-id 663212d4-7091-70ab-ecc7-f4c876ab9bf0 \
  --output json

aws sso-admin describe-account-assignment-creation-status \
  --profile org-admin \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --account-assignment-creation-request-id c8eb8d7d-dc30-45c9-8946-7e61c5cc527a \
  --output json

aws sso-admin list-account-assignments \
  --profile org-admin \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --account-id 349687196588 \
  --permission-set-arn arn:aws:sso:::permissionSet/ssoins-7535b4aeae7b57d7/ps-7535514a7384530d \
  --output json
```

Successful postchange evidence now shows:

- `BreakGlassAdmin` exists with `PT1H` session duration;
- `AdministratorAccess` is attached to the new permission set;
- the account-assignment request status is `SUCCEEDED`;
- management account `349687196588` now shows one direct assignment for
  `breakglass-principal`.

## Evidence

- `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`
- `docs/evidence/domain1-governance-breakglass-permission-set-create-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-status-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-managed-policies-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-create-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-status-20260625.json`
- `docs/evidence/domain1-governance-breakglass-permission-set-assignment-management-20260625.json`

## Approval

- approval source: direct user instruction
- approval text: `Suggestion works for me, Proceed`
- execution precondition later satisfied by:
  `I have refreshed org-admin SSO token`
- execution date: 2026-06-25

## Result

The dedicated `BreakGlassAdmin` permission set is now live in the smallest
approved form for this repository:

- dedicated emergency principal;
- management account only;
- one-hour session duration;
- first staged AWS-managed administrator policy attachment.

This closes the emergency permission-set creation and assignment gap for the
current governance slice. The root-user emergency-only SCP is still blocked,
but the blocker is now narrower: preserve root-user and management-path MFA
evidence, generate/store out-of-band backup material, validate the active
notification path, and record a light procedural recovery validation before the
SCP is attached.
