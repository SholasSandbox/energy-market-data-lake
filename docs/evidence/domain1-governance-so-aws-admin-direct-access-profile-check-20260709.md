# Domain 1 so-aws-admin Direct Access Profile Check - 2026-07-09

<!-- markdownlint-disable MD013 -->

## Status

Read-only access-path check complete.

No AWS resource was created, updated, deleted, moved, enabled, disabled, or
closed. No role credentials were saved.

This evidence does **not** approve account retirement. It only confirms that the
currently configured local AWS profiles do not provide a direct inventory path
into `so-aws-admin` (`054394900225`).

## Scope

Target account:

- `so-aws-admin` / `054394900225`

Checks performed:

- enumerated configured AWS CLI profile names;
- ran `sts get-caller-identity` for each profile and stored only account ID,
  caller class, and whether the profile directly targets `054394900225`;
- attempted `sts:AssumeRole` into
  `arn:aws:iam::054394900225:role/OrganizationAccountAccessRole` for each
  profile;
- stored assume-role failure categories only, not raw caller ARNs or usernames.

## Findings

- No configured profile returned an STS caller identity for `054394900225`.
- The usable configured profiles resolve to either the management account
  (`349687196588`) or the lakehouse workload account (`464975959576`), not the
  target account.
- The usable configured profiles all failed to assume
  `OrganizationAccountAccessRole` in `054394900225` with `AccessDenied`.
- One SSO-named profile has no current local SSO token, and one profile has no
  usable credentials. Refreshing either would not by itself prove target access;
  the current successful management-account profiles already fail the target
  assume-role check.
- The direct inventory blocker is therefore broader than a single `org-admin`
  profile choice. The repository still needs an approved temporary read-only
  target-account access path before in-account dependency checks can be
  completed.

## Evidence Files

- `docs/evidence/domain1-governance-so-aws-admin-direct-access-profile-check-sts-20260709.jsonl`
- `docs/evidence/domain1-governance-so-aws-admin-direct-access-profile-check-assume-role-20260709.jsonl`

## SAP-C02 Relevance

This supports Domain 1 by confirming that the account-retirement decision still
needs explicit identity and access design, not an assumption that the management
session can inspect every member account. It also supports Domain 3 by keeping
the retirement workflow reversible and evidence-led before any destructive
closure action is considered.
