# Domain 1 so-aws-admin Direct Inventory Access Plan - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Executed under separate explicit approval on 2026-07-09.

No further live AWS change is authorized by this document.

This plan exists because management-visible dependency evidence has been
collected, but direct in-account inventory for `so-aws-admin` (`054394900225`)
was blocked before the approved temporary access path was created.

Execution evidence is recorded in
`docs/evidence/domain1-governance-so-aws-admin-direct-inventory-live-readiness-20260709.md`.

## Problem

`so-aws-admin` is on the decommission path, but it is not ready for retirement.
The initial dependency evidence proves useful organization-level facts, yet it
does not prove that the account has no local IAM, data, DNS, service, cost, or
recovery dependencies.

The pre-execution access blocker was:

- no configured local AWS profile directly targets `054394900225`;
- management-account profiles cannot assume
  `arn:aws:iam::054394900225:role/OrganizationAccountAccessRole`;
- IAM Identity Center had no permission sets provisioned to
  `054394900225`.

## Objective

Create a temporary, auditable, read-only direct inventory path into
`054394900225`; run sanitized dependency checks; then remove the temporary
access path after evidence is captured.

This objective does not include account closure.

## Recommended Live-Change Path

Use IAM Identity Center rather than a long-lived IAM user.

Under separate explicit approval, either reuse an existing suitable read-only
permission set or create a temporary permission set named
`SoAwsAdminReadOnlyInventory` with:

- session duration limited to the evidence window, preferably 1 hour;
- AWS managed `SecurityAudit` for security and IAM inventory;
- AWS managed `ViewOnlyAccess` for broad service discovery;
- billing or budget read-only access only if the approver intentionally includes
  billing review in the same change window;
- no administrator access;
- no write permissions beyond the minimum Identity Center assignment lifecycle
  needed to provision and remove the temporary access.

Assign the permission set only to the approved human user or audit group and
only for `054394900225`. Do not expand the assignment to the management,
lakehouse workload, `Security Log Archive`, `Security Tooling`, or sandbox
accounts as part of this slice.

After evidence capture, remove the account assignment and verify that
`list-account-assignments` for `054394900225` is empty again, unless the user
separately approves keeping a durable read-only assignment.

## Explicit Approval Boundary

The next live step must be approved separately before any of the following
actions are performed:

- create, update, provision, assign, unassign, or delete an IAM Identity Center
  permission set;
- create, update, or delete an IAM role, IAM policy, IAM user, access key, SCP,
  or account assignment;
- enable or disable AWS Config, GuardDuty, Security Hub, OAM, CloudTrail, or
  any other service in `054394900225`;
- move, suspend, close, or otherwise retire the account.

If the approver chooses the smallest live change, the request should name the
target account, principal, permission set, session duration, evidence window,
and rollback/removal step.

## Fallback Path

If IAM Identity Center cannot be used, use a separately approved IAM role repair
or temporary IAM role path. That fallback should still be read-only, time-bound,
and removable. It should not create a permanent administrator path or rely on
root-user activity unless the user explicitly approves that break-glass path.

## Inventory Checklist After Access Exists

Capture sanitized evidence for:

- STS caller identity in `054394900225`;
- IAM account summary, password policy presence, users, roles, local policies,
  groups, instance profiles, and access-key metadata;
- Config recorders, delivery channels, rules, conformance packs, and aggregator
  visibility;
- GuardDuty detector state, Security Hub hub state, and OAM sinks;
- CloudTrail trails, event data stores, and logging status;
- CloudWatch alarms and log-group inventory;
- EventBridge rules and event buses;
- SNS topics and subscription counts with endpoints redacted or summarized;
- S3 bucket names, locations, encryption, versioning, lifecycle, tags, and
  policies, without reading object data unless separately approved;
- KMS keys, aliases, rotation status, and key policy ownership;
- Route 53 hosted-zone counts and sanitized dependency notes;
- budgets, cost, support, and contact dependencies through a public-safe summary
  or private billing review path;
- any data, log, domain, email, backup, or recovery dependency that must be
  preserved outside the account before closure.

## Retirement Gate

Do not draft an account-closure package until the direct inventory checklist is
complete, dependencies are either cleared or migrated, required evidence is
preserved outside `054394900225`, and a separate explicit closure approval is
recorded.

## SAP-C02 Relevance

This supports Domain 1 by turning account retirement into an explicit identity,
governance, and organizational-complexity decision. It supports Domain 3 by
protecting operational continuity: dependency discovery and evidence
preservation must happen before any irreversible account action.
