# Domain 1 Governance Evidence - Break-Glass Backup Material Check - 2026-06-27

<!-- markdownlint-disable MD013 -->

## Status

Prepared as a repo-safe evidence template for the remaining break-glass
live-readiness step.

Do not store backup codes, passwords, recovery answers, or private storage
locations in this file.

Do not mark this evidence as complete until the private out-of-band work has
actually been performed.

## Scope

This note is intended to capture secret-free completion evidence for:

- generation of backup codes for the emergency identity path;
- creation and storage of a private recovery note outside the repository;
- confirmation that the active recovery path is recorded privately;
- confirmation that no secrets were copied into this repository.

Related documents:

- `docs/runbooks/break-glass-access-procedure.md`
- `docs/evidence/domain1-governance-breakglass-permission-set-change-note-20260625.md`
- `docs/evidence/domain1-governance-deny-root-user-actions-change-note-20260622.md`

## Secret-Free Completion Checklist

- [ ] Google backup codes generated for `[redacted-email]`
- [ ] Backup codes stored in at least two out-of-band locations recorded
  privately
- [ ] Private recovery note created and stored outside the repository
- [ ] Management-account recovery path documented privately
- [ ] Root-user recovery phone/email path documented privately
- [ ] Repository checked to confirm that no backup codes or other secrets were
  stored here

## Secret-Free Evidence Fields

- emergency user: `breakglass-principal`
- emergency mailbox: `[redacted-email]`
- emergency permission set: `BreakGlassAdmin`
- management account: `349687196588` / `management-account-alias`
- active SMS notification path documented privately: [ ] yes / [ ] no
- out-of-band location 1 recorded privately: [ ] yes / [ ] no
- out-of-band location 2 recorded privately: [ ] yes / [ ] no
- private recovery note created: [ ] yes / [ ] no
- repo remains secret-free: [ ] yes / [ ] no

## Completion Record

- completion confirmed by: [fill in]
- completion date: [fill in]
- next review date: [fill in]

## Notes

- This file is intentionally secret-free.
- Private recovery material should live outside the repository and outside the
  primary machine path.
- When this template is fully completed, it can be cited as evidence for the
  remaining tracker item before any live root-user emergency-only SCP
  attachment.
