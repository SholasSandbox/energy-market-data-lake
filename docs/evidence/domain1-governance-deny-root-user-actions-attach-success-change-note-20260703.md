# Domain 1 Governance Change Note - Attach Root-User Emergency-Only SCP - 2026-07-03

<!-- markdownlint-disable MD013 -->

## Status

Completed successfully.

This was a bounded live AWS Organizations change. The customer-managed SCP was
created from the repository policy example and attached only to
`Lakehouse Workloads OU`.

## Approval

Explicit user approval was granted in the working session.

Approved scope:

- collect fresh read-only prechange Organizations evidence first;
- create one customer-managed SCP from
  `docs/policies/scp/deny-root-user-actions.example.json`;
- attach it only to `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`;
- verify the policy is attached only to that OU;
- record postchange evidence and rollback commands;
- make no other OU, account, SCP, IAM, Identity Center, CloudTrail, or Config
  changes.

## Scope

- Management account: `349687196588` / `management-account-alias`
- Target OU: `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`
- Current in-scope member account: `464975959576` / `lakehouse-workload-account`
- Customer-managed SCP name: `DenyRootUserActions-LakehouseWorkloads`
- Customer-managed SCP ID: `p-dv2ss5us`
- Customer-managed SCP ARN:
  `arn:aws:organizations::349687196588:policy/o-hmvgqmav88/service_control_policy/p-dv2ss5us`
- Policy source file:
  `docs/policies/scp/deny-root-user-actions.example.json`

## Prechange Evidence

Fresh read-only evidence was collected on 2026-07-03 before the live change:

- caller identity:
  `docs/evidence/domain1-governance-deny-root-user-actions-sts-prechange-20260703.json`
- root policy-type status:
  `docs/evidence/domain1-governance-deny-root-user-actions-roots-prechange-20260703.json`
- existing Lakehouse Workloads OU SCP attachments:
  `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-policies-prechange-20260703.json`
- Lakehouse Workloads OU account inventory:
  `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-accounts-prechange-20260703.json`
- precreate customer-managed SCP list:
  `docs/evidence/domain1-governance-deny-root-user-actions-policy-list-precreate-20260703.json`

Prechange summary:

- caller was the management account through `org-admin`;
- root `r-gbyf` had `SERVICE_CONTROL_POLICY` enabled;
- `Lakehouse Workloads OU` had `FullAWSAccess` and
  `DenyLeavingOrganization-LakehouseWorkloads` attached;
- `lakehouse-workload-account` was the only account in `Lakehouse Workloads OU`;
- no existing customer-managed SCP named
  `DenyRootUserActions-LakehouseWorkloads` was returned by the duplicate-name
  check.

## Change Executed

Create the customer-managed SCP:

```bash
aws organizations create-policy \
  --profile org-admin \
  --content file://docs/policies/scp/deny-root-user-actions.example.json \
  --name DenyRootUserActions-LakehouseWorkloads \
  --description "Restrict root-user actions for member accounts in Lakehouse Workloads OU except emergency recovery actions" \
  --type SERVICE_CONTROL_POLICY \
  --output json
```

Create-policy evidence:

- `docs/evidence/domain1-governance-deny-root-user-actions-create-policy-20260703.json`

Attach the customer-managed SCP only to `Lakehouse Workloads OU`:

```bash
aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

Attach-policy evidence:

- `docs/evidence/domain1-governance-deny-root-user-actions-attach-policy-20260703.json`

## Postchange Verification

Postchange evidence:

- policy target verification:
  `docs/evidence/domain1-governance-deny-root-user-actions-targets-postchange-20260703.json`
- Lakehouse Workloads OU SCP attachments after the change:
  `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-policies-postchange-20260703.json`
- Lakehouse Workloads OU account inventory after the change:
  `docs/evidence/domain1-governance-deny-root-user-actions-lakehouse-ou-accounts-postchange-20260703.json`
- policy summary and content after the change:
  `docs/evidence/domain1-governance-deny-root-user-actions-policy-postchange-20260703.json`

Verification summary:

- `DenyRootUserActions-LakehouseWorkloads` is attached to
  `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`;
- no other policy target was returned for policy `p-dv2ss5us`;
- `Lakehouse Workloads OU` now has:
  - `FullAWSAccess`;
  - `DenyLeavingOrganization-LakehouseWorkloads`;
  - `DenyRootUserActions-LakehouseWorkloads`;
- `lakehouse-workload-account` remains the only account in `Lakehouse Workloads OU`.

## Rollback

If this guardrail must be removed, detach the policy from the OU:

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

Validate rollback:

```bash
aws organizations list-targets-for-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --output json

aws organizations list-policies-for-target \
  --profile org-admin \
  --target-id ou-gbyf-m6ppfmpq \
  --filter SERVICE_CONTROL_POLICY \
  --output json
```

If the detached policy is no longer needed, delete it:

```bash
aws organizations delete-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us
```

## Impact

- The change affects root principals in member accounts under
  `Lakehouse Workloads OU`.
- The management account is not affected by this SCP.
- No account placement, IAM Identity Center assignment, CloudTrail, AWS Config,
  bucket, KMS key, or workload resource was changed.
- The documented break-glass, root-MFA, notification, recovery-code, and
  procedural-validation evidence should be kept current before broadening this
  pattern to any additional OU.

## SAP-C02 Relevance

This supports Domain 1 by adding a preventive organization guardrail after the
recovery path was documented and validated. The exam-relevant pattern is to use
SCPs as maximum-permission boundaries, stage them narrowly at the OU level, and
pair them with break-glass, rollback, and audit evidence.
