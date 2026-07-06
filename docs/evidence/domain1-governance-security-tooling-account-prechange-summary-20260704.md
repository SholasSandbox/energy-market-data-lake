# Domain 1 Governance Evidence - Security Tooling Account Prechange - 2026-07-04

<!-- markdownlint-disable MD013 -->

## Status

Fresh read-only Organizations/account evidence was collected successfully.

No AWS account was created, no account was moved, no SCP was attached or
detached, no trusted service access was changed, and no delegated-administrator
setting was changed.

## Scope

- Management account: `349687196588` / `management-account-alias`
- Intended future account boundary: `Security Tooling`
- Target parent for any later approved account creation: `ou-gbyf-mug20ym0` /
  `Security OU`
- Existing storage-only security account: `955659429518` /
  `Security Log Archive`

## Evidence Files

Raw evidence captured on 2026-07-04:

- caller identity:
  `docs/evidence/domain1-governance-security-tooling-sts-prechange-20260704.json`
- organization description:
  `docs/evidence/domain1-governance-security-tooling-org-description-prechange-20260704.json`
- root and policy-type status:
  `docs/evidence/domain1-governance-security-tooling-roots-prechange-20260704.json`
- root OU inventory:
  `docs/evidence/domain1-governance-security-tooling-root-ous-prechange-20260704.json`
- all-account inventory:
  `docs/evidence/domain1-governance-security-tooling-accounts-prechange-20260704.json`
- current `Security OU` account inventory:
  `docs/evidence/domain1-governance-security-tooling-security-ou-accounts-prechange-20260704.json`
- current `Security OU` SCP attachments:
  `docs/evidence/domain1-governance-security-tooling-security-ou-policies-prechange-20260704.json`
- trusted service access:
  `docs/evidence/domain1-governance-security-tooling-service-access-prechange-20260704.json`
- delegated administrator inventory:
  `docs/evidence/domain1-governance-security-tooling-delegated-admins-prechange-20260704.json`
- delegated services for `Security Log Archive`:
  `docs/evidence/domain1-governance-security-tooling-security-log-archive-delegated-services-prechange-20260704.json`
- current parent for `Security Log Archive`:
  `docs/evidence/domain1-governance-security-tooling-security-log-archive-parent-prechange-20260704.json`

## Prechange Findings

- The `org-admin` session resolved to the Organizations management account
  `349687196588`.
- Root `r-gbyf` still has `SERVICE_CONTROL_POLICY` enabled.
- Root `r-gbyf` currently contains these OUs:
  - `Lakehouse Workloads OU` / `ou-gbyf-m6ppfmpq`
  - `Security OU` / `ou-gbyf-mug20ym0`
  - `Container Sandbox` / `ou-gbyf-zs0f26b5`
- No account named `Security Tooling` appears in the all-account inventory.
- `Security OU` currently contains:
  - `Security Log Archive` / `955659429518`
  - `so-aws-admin` / `054394900225`
- `Security OU` currently has only the AWS-managed `FullAWSAccess` SCP attached.
- Trusted service access is currently enabled for:
  - `account.amazonaws.com`
  - `cloudtrail.amazonaws.com`
  - `config-multiaccountsetup.amazonaws.com`
  - `config.amazonaws.com`
  - `sso.amazonaws.com`
- `Security Log Archive` / `955659429518` is the only delegated administrator
  returned by the organization-level delegated-admin inventory.
- Delegated services currently pointing at `Security Log Archive` are:
  - `config-multiaccountsetup.amazonaws.com`
  - `config.amazonaws.com`
- `Security Log Archive` currently sits under `Security OU`.

## Interpretation

The evidence supports the next approval conversation for the Security Tooling
account slice, but it does not itself authorize a live account-creation or
account-move command.

Before account creation is approved, the current `Security OU` occupancy should
be acknowledged explicitly because `so-aws-admin` is already present alongside
`Security Log Archive`. The evidence still shows that the accepted target
account name, `Security Tooling`, does not currently exist.

## SAP-C02 Relevance

This supports Domain 1 by preserving the prechange organization state for a
future delegated security-administration boundary. It reinforces separation of
duties between the management account, the write-mostly `Security Log Archive`
account, and the proposed active `Security Tooling` account.
