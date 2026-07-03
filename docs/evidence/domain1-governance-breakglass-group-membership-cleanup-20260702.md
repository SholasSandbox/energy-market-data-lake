# Domain 1 Governance Change Note - Break-Glass Group Membership Cleanup - 2026-07-02

## Status

Completed successfully.

This was a bounded IAM Identity Center live change to remove an unintended
routine-administrator inheritance path from the dedicated break-glass user.

## Approval

Explicit user approval was granted in the working session to remove
`breakglass-principal` from `cloud-lab-aws-admins`.

## Scope

- IAM Identity Center instance:
  `arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7`
- Identity store: `d-9c674fdf75`
- Management account: `349687196588` / `management-account-alias`
- Emergency user:
  `663212d4-7091-70ab-ecc7-f4c876ab9bf0` /
  `breakglass-principal`
- Removed group:
  `96828204-a021-7096-12d7-85afa6c75ffa` /
  `cloud-lab-aws-admins`
- Removed membership:
  `66b29284-a001-70a3-4639-f04f3173c94d`

## Prechange Finding

Read-only CLI inventory showed that `breakglass-principal` had two management
account access paths:

- direct `BreakGlassAdmin` assignment to management account `349687196588`;
- inherited `AdministratorAccess` through membership in
  `cloud-lab-aws-admins`.

That inherited group path was not aligned to the documented break-glass model,
because the dedicated emergency user should not also carry routine
administrator group membership.

## Change Executed

```bash
aws identitystore delete-group-membership \
  --identity-store-id d-9c674fdf75 \
  --membership-id 66b29284-a001-70a3-4639-f04f3173c94d \
  --profile org-admin
```

The command completed without error.

## Postchange Verification

### Break-glass user group memberships

```bash
aws identitystore list-group-memberships-for-member \
  --identity-store-id d-9c674fdf75 \
  --member-id UserId=663212d4-7091-70ab-ecc7-f4c876ab9bf0 \
  --profile org-admin \
  --output json
```

Result:

```json
{
    "GroupMemberships": []
}
```

### `cloud-lab-aws-admins` remaining members

```bash
aws identitystore list-group-memberships \
  --identity-store-id d-9c674fdf75 \
  --group-id 96828204-a021-7096-12d7-85afa6c75ffa \
  --profile org-admin \
  --output json
```

Result: the group still contains `org-admin-principal`
(`b6b262e4-0041-70a7-df00-66e62c9af94a`) and no longer contains
`breakglass-principal`.

### `BreakGlassAdmin` direct assignment remains

```bash
aws sso-admin list-account-assignments \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --account-id 349687196588 \
  --permission-set-arn arn:aws:sso:::permissionSet/ssoins-7535b4aeae7b57d7/ps-7535514a7384530d \
  --profile org-admin \
  --output json
```

Result: `BreakGlassAdmin` remains assigned directly to user
`663212d4-7091-70ab-ecc7-f4c876ab9bf0` / `breakglass-principal`.

### `AdministratorAccess` management-account assignments remain for normal admin paths

```bash
aws sso-admin list-account-assignments \
  --instance-arn arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7 \
  --account-id 349687196588 \
  --permission-set-arn arn:aws:sso:::permissionSet/ssoins-7535b4aeae7b57d7/ps-753598a7b4667850 \
  --profile org-admin \
  --output json
```

Result: `AdministratorAccess` remains assigned to normal management-account
admin paths, including `cloud-lab-aws-admins` and `org-admin-principal`, but
`breakglass-principal` no longer inherits that path because it is no longer a
member of `cloud-lab-aws-admins`.

## Impact

- The dedicated break-glass user now has no group memberships.
- The dedicated break-glass user retains the intended direct
  `BreakGlassAdmin` management-account assignment.
- The normal administrator path for `org-admin-principal` remains intact.
- No workload account, SCP, CloudTrail, AWS Config, or root-user settings were
  changed.

Existing browser or CLI sessions that were already issued before this cleanup
may remain usable until sign-out or session expiry. The practical validation is
to sign out of the AWS access portal and sign back in as `breakglass-principal`;
the expected portal view is management account `349687196588` with
`BreakGlassAdmin` only.

## Rollback

If emergency rollback is required, re-add `breakglass-principal` to
`cloud-lab-aws-admins`:

```bash
aws identitystore create-group-membership \
  --identity-store-id d-9c674fdf75 \
  --group-id 96828204-a021-7096-12d7-85afa6c75ffa \
  --member-id UserId=663212d4-7091-70ab-ecc7-f4c876ab9bf0 \
  --profile org-admin
```

Rollback should be treated as a temporary emergency exception and removed again
after the incident.

## SAP-C02 Relevance

This supports Domain 1 by separating routine administration from emergency
access. The clean target model is:

- normal administration through the named admin user and admin group;
- emergency access through a dedicated IAM Identity Center user;
- short-session `BreakGlassAdmin` permission set;
- MFA, evidence, and post-use review before restrictive SCP rollout.
