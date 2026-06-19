# Break-Glass Access Procedure

<!-- markdownlint-disable MD013 -->

## Scope

This procedure defines the target emergency-access model for the SAP-C02
governance track.

It is documentation only. It does not create users, permission sets, roles,
alarms, or AWS account changes.

Related design:

- `docs/adr/0005-aws-organizations-governance-design.md`
- `docs/planning/identity-center-permission-set-matrix-20260619.md`

## Purpose

Break-glass access exists only for emergency recovery when ordinary
administrative paths are unavailable, broken, or too slow for the incident.

It is not for routine administration, convenience, deployments, study labs, or
normal lakehouse operations.

## Accepted Emergency Triggers

Use break-glass only when at least one condition is true:

- IAM Identity Center access is unavailable or misconfigured.
- The management account cannot administer Organizations, SCPs, or account
  placement through the normal path.
- A restrictive SCP or IAM change blocks required recovery.
- The lakehouse workload account requires urgent recovery and normal operator
  access cannot perform the fix.
- Security/audit evidence is at risk and normal security access is unavailable.

## Access Paths

Preferred order:

1. normal IAM Identity Center permission set;
2. dedicated emergency `BreakGlassAdmin` permission set;
3. root-user recovery only when the first two paths are unavailable.

Root-user access must remain the last resort and must use MFA.

## Preconditions Before Live Implementation

Before this process can be considered live-ready:

- identify the emergency owner;
- confirm MFA on any emergency principal;
- document credential storage and recovery location outside this repository;
- define who receives emergency-use notifications;
- define the evidence location for emergency-use logs;
- decide whether the emergency path exists in the management account, workload
  account, or both;
- define how access is disabled or reduced after use.

## Activation Checklist

Record the following before use whenever possible:

- date and time;
- account;
- reason for emergency access;
- normal access path that failed;
- intended change;
- expected blast radius;
- rollback path;
- approver or self-approval reason if no approver is available.

If immediate recovery is required, record the missing details as soon as the
account is stable.

## During Use

- Make only the minimum change needed to restore safe access or stop the
  incident.
- Avoid unrelated cleanup or improvement work.
- Capture console screenshots or AWS CLI command output where safe.
- Do not leave new broad IAM users, access keys, or unmanaged administrator
  roles behind.
- Do not weaken SCPs, logging, or public-access controls permanently without a
  separate reviewed change.

## Closure Checklist

After emergency use:

- confirm the account is stable;
- remove temporary access, roles, or credentials;
- restore any loosened SCP, IAM, logging, or public-access control;
- rotate credentials if exposed or newly created;
- confirm CloudTrail or equivalent audit evidence exists;
- document the exact actions taken;
- record what failed in the normal access path;
- create a follow-up task to prevent recurrence.

## Evidence Template

```text
Date:
Account:
Emergency trigger:
Normal path attempted:
Break-glass path used:
Actions taken:
AWS evidence captured:
Rollback completed:
Temporary access removed:
Credentials rotated:
Follow-up task:
Reviewer:
```

## Rejection Rules

Do not use break-glass for:

- routine lakehouse operations;
- ordinary Terraform applies;
- convenience access;
- bypassing least-privilege design;
- parked container or AI orchestration work;
- unapproved live governance changes.

## SAP-C02 Relevance

This procedure supports Domain 1 by documenting emergency access, separation of
duties, management-account recovery, and auditability. It also reinforces that
governance controls need a recovery path before restrictive SCPs or account
guardrails are attached.
