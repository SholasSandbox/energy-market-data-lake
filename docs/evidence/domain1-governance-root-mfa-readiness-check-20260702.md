# Domain 1 Governance Evidence - Root MFA Readiness Check - 2026-07-02

## Status

Read-only verification completed.

This evidence records the live MFA posture for the workload account root user
before any future root-user emergency-only SCP attachment.

No AWS resources were changed by this check.

## Scope

- Account checked: `464975959576` / `lakehouse-workload-account`
- Purpose: confirm whether the authenticator entry labelled
  `emergency@464975959576` corresponds to workload-account root MFA.
- CLI profile used for read-only IAM inventory: `default`

## Caller Identity

```bash
aws sts get-caller-identity --profile default --output json
```

Result summary:

- account: `464975959576`
- caller ARN: `arn:aws:iam::464975959576:user/IAMUser1`

## IAM User Inventory

```bash
aws iam list-users --profile default --output json
```

Result summary:

- the only IAM user returned was `IAMUser1`;
- no IAM user named `emergency` was present.

## Virtual MFA Inventory

```bash
aws iam list-virtual-mfa-devices \
  --profile default \
  --assignment-status Any \
  --output json
```

Result summary:

- virtual MFA serial:
  `arn:aws:iam::464975959576:mfa/Pixel-6-Pro-emergency`;
- assigned user ARN:
  `arn:aws:iam::464975959576:root`;
- enable date: `2026-06-25T13:33:42+00:00`.

## Interpretation

The authenticator entry labelled `emergency@464975959576` corresponds to the
workload account root MFA path, not to a standalone IAM user named
`emergency`.

This satisfies the root-MFA identification portion of the root-user
emergency-only SCP readiness work for account `464975959576`.

It does not, by itself, complete the broader break-glass readiness blocker. The
remaining readiness work is still to:

- store the out-of-band backup material and recovery note outside Git;
- confirm notification-path reachability;
- record a light procedural validation of evidence capture and access
  reduction.

## SAP-C02 Relevance

This supports Domain 1 by preserving a documented root-user recovery path before
attaching restrictive SCPs. The exam-relevant pattern is that root access should
remain a last resort, protected by MFA, and backed by a verified recovery path
before organization guardrails are tightened.
