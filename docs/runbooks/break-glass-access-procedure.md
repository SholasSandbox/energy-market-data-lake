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

## Current Live-Readiness Values

The following values are now recorded for the current governance slice:

- documented target emergency owner identity:
  `[redacted-email]`
- current active emergency contact mailbox:
  `[redacted-email]`
- retired contact path: `[redacted-email]` is no longer part of the active
  break-glass design and should not be used for future notification testing
- recorded emergency-use notifications currently include email to
  `[redacted-email]` plus SMS to `[redacted-phone]`; any secondary
  notification recipient must be re-recorded explicitly before restrictive SCP
  rollout
- credential and recovery storage: Google Password Manager as the primary
  cross-platform store
- out-of-band backup requirement: keep Google backup codes and a short AWS root
  recovery note outside this repository and outside the primary machine path
- evidence location for emergency-use logs:
  `docs/evidence/domain1-governance-break-glass-usage-YYYYMMDD.md`, with saved
  CLI output and screenshots where safe
- current scope: workload-account emergency recovery is the active design
  priority for OU-targeted root-user guardrails; the management account remains
  out of scope as an SCP target for the current OU attachment, but it remains
  in scope for Organizations control-plane rollback and recovery until
  delegated Organizations policy management is explicitly implemented
- post-use access reduction path: remove temporary access, confirm MFA remains
  enabled, review CloudTrail or equivalent audit evidence, rotate any exposed
  or newly created credentials, and document the follow-up action

Follow-on clarification from 2026-06-25 same-day IAM Identity Center evidence:

- the current live management-account admin principal used for Organizations
  recovery is
  `org-admin-principal` /
  `[redacted-email]`;
- a dedicated IAM Identity Center user now exists for the documented target
  emergency owner identity:
  `breakglass-principal` / `[redacted-email]`;
- that dedicated break-glass user shows one enrolled MFA device in same-day
  console evidence;
- a dedicated `BreakGlassAdmin` permission set now exists with `PT1H` session
  duration and the AWS-managed `AdministratorAccess` policy attached as the
  first staged implementation;
- that permission set is currently assigned only to the management account for
  user `breakglass-principal`;
- the current live principal inventory is recorded in
  `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`.

Follow-on cleanup from 2026-07-02 live IAM Identity Center evidence:

- the dedicated break-glass user previously inherited management-account
  `AdministratorAccess` through `cloud-lab-aws-admins`;
- under explicit approval, `breakglass-principal` was removed from
  `cloud-lab-aws-admins`;
- postchange read-only verification shows `breakglass-principal` now has no
  group memberships and retains the direct management-account
  `BreakGlassAdmin` assignment;
- the cleanup evidence is recorded in
  `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`.

Follow-on root-MFA clarification from 2026-07-02 read-only IAM evidence:

- the authenticator entry labelled `emergency@464975959576` maps to virtual
  MFA device `arn:aws:iam::464975959576:mfa/Pixel-6-Pro-emergency`;
- that virtual MFA device is assigned to `arn:aws:iam::464975959576:root`;
- no IAM user named `emergency` exists in workload account `464975959576`;
- the read-only evidence is recorded in
  `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`.

Follow-on break-glass MFA clarification from 2026-07-03 console evidence:

- `breakglass-principal` now has two registered IAM Identity Center
  authenticator-app MFA devices;
- the second MFA device was registered on a separate device;
- the secret-free evidence is recorded in
  `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`.

Follow-on notification-path clarification from 2026-07-03 receipt evidence:

- the active emergency SMS notification path was tested with a harmless
  break-glass readability message;
- the user confirmed receipt at 2026-07-03 13:33 BST;
- the secret-free evidence is recorded in
  `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`.

Follow-on recovery-code clarification from 2026-07-03 user-confirmed evidence:

- Google backup codes for the emergency mailbox are readable in both private
  recorded storage locations;
- the Microsoft/Outlook recovery code for the management-account root mailbox
  is readable in both private recorded storage locations;
- both recovery-code sets are stored in electronic and paper formats;
- the secret-free evidence is recorded in
  `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`.

Follow-on procedural validation from 2026-07-03 tabletop evidence:

- the break-glass notification, evidence-capture, and post-use reduction path
  was rehearsed without using emergency access or changing AWS resources;
- the validation closes the light procedural-validation prerequisite for the
  root-user emergency-only SCP candidate;
- any live SCP attachment still requires separate explicit approval and fresh
  prechange Organizations evidence;
- the secret-free evidence is recorded in
  `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`.

Follow-on root-user SCP attachment from 2026-07-03 live Organizations evidence:

- `DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us` is now attached to
  `Lakehouse Workloads OU`;
- the policy was created from
  `docs/policies/scp/deny-root-user-actions.example.json`;
- the management account is not affected by the OU-targeted SCP;
- rollback is documented in the change note;
- the secret-free evidence is recorded in
  `docs/evidence/domain1-governance-deny-root-user-actions-attach-success-change-note-20260703.md`.

For this repository, "out-of-band backup" means recovery material that is not
available only through the same day-to-day Google sign-in path or the same
primary machine. Acceptable examples include:

- printed backup codes stored securely;
- an encrypted offline file on separate removable media; or
- a secondary secure vault that does not depend solely on the same active
  Google session.

## Root-User SCP Live Status

Before the root-user emergency-only SCP was attached live, the following
evidence was preserved:

- confirm MFA on the relevant root user and preserve the MFA evidence for the
  management-account recovery path (workload account `464975959576` root MFA is
  now recorded in
  `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`);
- preserve evidence that the dedicated Identity Center break-glass user has a
  second MFA device (recorded in
  `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`);
- preserve evidence that the active emergency SMS notification path is
  reachable (recorded in
  `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`);
- preserve evidence that the out-of-band recovery-code material is readable in
  both private recorded storage locations (recorded in
  `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`);
- preserve a light procedural validation of the evidence and reduction path
  (recorded in
  `docs/evidence/domain1-governance-breakglass-procedural-validation-20260703.md`).

The live SCP is now attached to `Lakehouse Workloads OU` as
`DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us`. Any change to detach,
replace, broaden, or reuse the policy for another OU still requires separate
explicit approval, fresh prechange evidence, and postchange verification.

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
