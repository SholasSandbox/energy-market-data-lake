# Domain 1 so-aws-admin Permission Set Cleanup Evidence - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Approved live IAM Identity Center cleanup completed.

The temporary `SoAwsAdminReadOnlyInventory` permission set was deleted after the
direct read-only inventory run. No account closure, SCP change, service
enablement or disablement, IAM user creation, long-lived credential creation,
Terraform apply, administrator access expansion, or change to another AWS
account was performed.

## Scope

Deleted permission set:

- `SoAwsAdminReadOnlyInventory`

Identity Center instance:

- `arn:aws:sso:::instance/ssoins-7535b4aeae7b57d7`

Target account checked before deletion:

- `so-aws-admin` / `054394900225`

## Pre-Delete Verification

Before deletion:

- target account assignment count: `0`;
- target account provisioned permission-set count for
  `SoAwsAdminReadOnlyInventory`: `0`;
- all-account provisioned account count for `SoAwsAdminReadOnlyInventory`: `0`;
- attached managed policies were only `SecurityAudit` and `ViewOnlyAccess`;
- pre-delete state was safe to delete.

## Delete Verification

Deletion completed successfully:

- `delete-permission-set` status: `0`;
- post-delete list by permission-set name found no
  `SoAwsAdminReadOnlyInventory`;
- describing the deleted permission-set ARN returned `ResourceNotFoundException`
  as expected.

## Remaining so-aws-admin Retirement Blockers

This cleanup resolves only the temporary permission-set lifecycle item. The
account is still not ready for retirement until these residual items are
resolved:

- `BreakGlassAdminRole`;
- service-linked-role and root/break-glass posture;
- budget/cost metadata evidence or private review;
- CloudTrail Lake event-data-store evidence or explicit waiver;
- separate explicit account-closure approval, if closure is ever pursued.

## Evidence Files

- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-pre-delete-verification-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-permission-set-before-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-managed-policies-before-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-target-account-assignments-before-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-target-provisioned-permission-sets-before-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-all-provisioned-accounts-before-delete-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-delete-permission-set-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-delete-summary-20260709.json`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-permission-sets-after-20260709.jsonl`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-describe-after-delete-20260709.status`
- `docs/evidence/domain1-governance-so-aws-admin-permission-set-cleanup-describe-after-delete-20260709.err`

## SAP-C02 Relevance

This supports Domain 1 by closing the temporary access lifecycle after an
approved account-inventory task. It supports Domain 3 by reducing residual
access surface and keeping account retirement gated on explicit dependency
evidence rather than lingering administrative convenience.
