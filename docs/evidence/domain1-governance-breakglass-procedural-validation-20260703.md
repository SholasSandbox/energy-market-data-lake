# Domain 1 Governance Evidence - Break-Glass Procedural Validation - 2026-07-03

## Status

Completed as a light tabletop procedural validation.

No emergency access was used, no root-user recovery was invoked, no SCP was
attached, and no AWS resources were changed by Codex for this evidence item.

## Scope

- Emergency user: `breakglass-principal`
- Emergency permission set: `BreakGlassAdmin`
- Management account: `349687196588` / `management-account-alias`
- Current root-user SCP candidate:
  `docs/policies/scp/deny-root-user-actions.example.json`
- Target OU for any future attachment:
  `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`

## Inputs Reviewed

- Break-glass runbook:
  `docs/runbooks/break-glass-access-procedure.md`
- Identity Center current-state evidence:
  `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`
- Break-glass group-membership cleanup evidence:
  `docs/evidence/domain1-governance-breakglass-group-membership-cleanup-20260702.md`
- Workload-account root MFA evidence:
  `docs/evidence/domain1-governance-root-mfa-readiness-check-20260702.md`
- Second Identity Center MFA evidence:
  `docs/evidence/domain1-governance-breakglass-mfa2-readiness-check-20260703.md`
- Emergency SMS notification reachability evidence:
  `docs/evidence/domain1-governance-notification-reachability-check-20260703.md`
- Recovery-code readability evidence:
  `docs/evidence/domain1-governance-recovery-code-readability-check-20260703.md`

## Procedure Rehearsed

The following dry-run path was validated against the documented evidence:

1. Use normal IAM Identity Center administration first where available.
2. Use `breakglass-principal` with `BreakGlassAdmin` only when normal access is
   unavailable, broken, or too slow for the incident.
3. Treat account root recovery as the last-resort path after Identity Center
   routes fail.
4. Send or preserve emergency-use notification evidence before or immediately
   after emergency access, depending on incident urgency.
5. Capture secret-free evidence in
   `docs/evidence/domain1-governance-break-glass-usage-YYYYMMDD.md` or a
   dedicated evidence note.
6. Make only the minimum corrective change required to restore safe access.
7. Remove temporary access, restore loosened controls, rotate exposed
   credentials where applicable, and confirm CloudTrail or equivalent audit
   evidence.
8. Record the post-use review and follow-up action.

## Validation Result

The dry-run validation passed for the current readiness boundary:

- the dedicated emergency user exists and is separate from routine admin group
  membership;
- the emergency permission-set path is direct and management-account scoped;
- the workload account root MFA path is identified;
- the emergency Identity Center user has a second MFA device;
- the active emergency SMS notification path has been tested;
- recovery-code materials are confirmed readable in both private recorded
  locations and stored in paper plus electronic formats;
- the runbook defines activation, use, closure, evidence, and rejection rules.

## Boundary

This validation closes the light procedural-validation prerequisite for the
root-user emergency-only SCP candidate.

It does not attach the SCP, create a policy, detach any existing policy, or
approve a live Organizations change. A live attachment still requires:

- explicit approval for that one change;
- fresh read-only Organizations prechange evidence;
- confirmation of the exact policy ID after creation;
- rollback commands captured with the real policy ID;
- postchange verification that the policy is attached only to
  `Lakehouse Workloads OU`.

## SAP-C02 Relevance

This supports Domain 1 by validating that restrictive organization guardrails
have a documented recovery path, notification path, evidence path, and
post-use reduction path before implementation. The exam-relevant pattern is to
combine preventive controls such as SCPs with operational recovery and audit
readiness.
