# Domain 1 Governance Organization Inventory Summary - 2026-06-21

<!-- markdownlint-disable MD013 -->

## Purpose

Record the first Organizations inventory evidence slice for the Domain 1
governance live-readiness path using read-only AWS CLI commands only.

This check follows
`docs/runbooks/domain-1-governance-live-readiness-runbook.md`, which puts
Organizations inventory evidence first in the execution order.

## Command Set

```bash
aws sts get-caller-identity --output json
aws organizations describe-organization --output json
aws organizations list-roots --output json
aws organizations list-accounts --output json
aws organizations list-aws-service-access-for-organization --output json
```

## First Attempt From The Workload-Account Shell

The first attempt in the Codex shell used account `464975959576` and could only
partially complete the inventory.

### Observed identity

`aws sts get-caller-identity` returned:

```json
{
  "UserId": "AIDAWYQV2CYMFKHW4IY24",
  "Account": "464975959576",
  "Arn": "arn:aws:iam::464975959576:user/IAMUser1"
}
```

### Observed organization summary

`aws organizations describe-organization` returned:

```json
{
  "Organization": {
    "Id": "o-hmvgqmav88",
    "Arn": "arn:aws:organizations::349687196588:organization/o-hmvgqmav88",
    "FeatureSet": "ALL",
    "MasterAccountArn": "arn:aws:organizations::349687196588:account/o-hmvgqmav88/349687196588",
    "MasterAccountId": "349687196588",
    "MasterAccountEmail": "[redacted-email]",
    "AvailablePolicyTypes": [
      {
        "Type": "SERVICE_CONTROL_POLICY",
        "Status": "ENABLED"
      }
    ]
  }
}
```

### Initial access blocker

The org-wide inventory commands all failed from the current credentials:

- `aws organizations list-roots --output json`
- `aws organizations list-accounts --output json`
- `aws organizations list-aws-service-access-for-organization --output json`

Each returned `AccessDeniedException`.

Representative error:

```text
aws: [ERROR]: An error occurred (AccessDeniedException) when calling the ListAccounts operation: You don't have permissions to access this resource.
```

### Interpretation of the first attempt

The workload-account credentials were sufficient to confirm the organization
exists and that SCPs are enabled, but they were not sufficient to complete the
org-wide inventory evidence package from that shell.

This is a **structural and permission-scope blocker**, not a calendar blocker:

- the active credentials are for account `464975959576`;
- the organization management account is `349687196588`;
- the runbook's first evidence slice needs org-wide inventory commands that are
  not allowed from the current scope.

## Management-Scope Resolution

The shell blocker was resolved by selecting the management-account SSO profile
instead of relying on the default workload-account credentials. In this
workspace, the working profile is `org-admin`.

Working command pattern:

```bash
aws sts get-caller-identity --profile org-admin --output json
aws organizations describe-organization --profile org-admin --output json
aws organizations list-roots --profile org-admin --output json
aws organizations list-accounts --profile org-admin --output json
aws organizations list-aws-service-access-for-organization --profile org-admin --output json
```

Observed learning:

- the shell default credentials still point to workload account `464975959576`;
- management-account organization inventory succeeds when the shell uses a
  management-account SSO profile such as `--profile org-admin`;
- this is therefore a shell-context and profile-selection issue, not an
  Organizations service limitation.

For longer sessions, the equivalent environment-based fix is:

```bash
export AWS_PROFILE=org-admin
aws sts get-caller-identity --output json
```

If an SSO-backed management profile exists but fails with an expired-session
error, refresh that intended profile with:

```bash
aws sso login --profile <profile-name>
```

The raw outputs are now recorded in:

- `docs/evidence/domain1-governance-sts-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-description-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-roots-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-accounts-prechange-20260621.json`
- `docs/evidence/domain1-governance-org-service-access-prechange-20260621.json`

## Observed Organization Inventory

The resolved inventory shows:

- one root: `r-gbyf`
- three active accounts in the organization
- AWS service access enabled for `sso.amazonaws.com`
- one current OU under the root: `Container Sandbox`

### Root inventory

- Root ID: `r-gbyf`
- Root ARN:
  `arn:aws:organizations::349687196588:root/o-hmvgqmav88/r-gbyf`
- Root name: `Root`

### Account inventory

- `349687196588` / `management-account-alias`
  Management account; joined by invitation on 2026-05-08.
- `464975959576` / `lakehouse-workload-account`
  Energy Data Lakehouse workload member account; joined by invitation on
  2026-06-05.
- `974893866311` / `containers-lab.com`
  Separate sandbox/container-lab member account; created in the organization on
  2026-06-05.

### Organization service access

- `sso.amazonaws.com` enabled on 2026-05-08

### Prechange parent mapping

- `management-account-alias` (`349687196588`) was attached directly to root `r-gbyf`
- `lakehouse-workload-account` (`464975959576`) was attached directly to root `r-gbyf`
- `containers-lab.com` (`974893866311`) was attached to OU
  `ou-gbyf-zs0f26b5`
- the root OU list at that point contained one OU:
  `Container Sandbox` (`ou-gbyf-zs0f26b5`)

Important interpretation:

- this confirms the organization inventory needed for the first live-readiness
  slice;
- it supports the accepted governance design that separates management,
  workload, and sandbox account roles;
- it now also proves current parent placement for the management, lakehouse,
  and container accounts;
- it shows a useful live-state nuance: the sandbox/container account already has
  a dedicated OU, while the lakehouse workload account is still attached to the
  root;
- the root output alone should not be used to infer SCP attachment state.

Later live OU creation is recorded separately in
`docs/evidence/domain1-governance-lakehouse-workloads-ou-change-note-20260621.md`.
The later live lakehouse account move is recorded separately in
`docs/evidence/domain1-governance-lakehouse-account-move-change-note-20260622.md`.

## Tracker Impact

- Step 4 can continue beyond the earlier access blocker.
- The first Organizations inventory evidence slice is now recorded.
- The current-to-target OU/account-placement decision can now be grounded in
  live parent-mapping evidence, not only account inventory.

## Next Required State

After the inventory capture, the next useful states are:

1. Record the repo-only current-to-target OU/account-placement decision using
   the live parent-mapping evidence.
2. Decide whether the existing `Container Sandbox` OU is accepted as the
   sandbox target name or should be normalized later.
3. Keep live OU creation or account movement behind explicit approval,
   rollback, and validation boundaries.
