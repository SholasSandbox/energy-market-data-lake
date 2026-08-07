# SAP-C02 Study Note: Governance Structure Mental Model

**Last revised:** 2026-08-07

## Purpose

This note captures the current governance mental model for the Energy
Lakehouse AWS organization. It is designed to be understood, not memorised by
rote.

The key idea is that AWS governance is a layered control plane:

```text
AWS Organization
-> OUs and accounts
-> IAM Identity Center users and permission sets
-> temporary IAM roles in target accounts
-> IAM policies plus SCP guardrails
-> workload, security, audit, and recovery actions
```

When these layers are kept separate, the governance design becomes much easier
to reason about.

## The Five Layers

| Layer | What it is | In this setup |
|---|---|---|
| AWS Organization | The top-level container for all AWS accounts | The organization containing management, workload, sandbox, and security accounts |
| Management account | The control-plane account | `Management account` / `<management-account-id>` |
| Organizational Units | Groupings used to apply guardrails to accounts | `Lakehouse Workloads OU`, `Security OU`, `Container Sandbox` |
| Accounts | Hard blast-radius, cost, and security boundaries | Lakehouse workload, sandbox, security log archive, future security tooling |
| Identities and permission sets | Human entry paths into accounts | IAM Identity Center users plus permission sets such as `BreakGlassAdmin` |

The management account should stay as the control plane. Workloads should run in
member accounts, not in the management account.

## Current Account Roles

| Account | Role in the design | What belongs there |
|---|---|---|
| `Management account` / `<management-account-id>` | Management account | Organizations, IAM Identity Center, SCP administration, billing, account placement |
| `Lakehouse workload account` | Lakehouse workload account | Energy Data Lakehouse workload resources |
| `Container sandbox` / `<sandbox-account-id>` | Sandbox/container account | Containers, microservices, lab work, sandbox AWS Config recorder |
| `Security Log Archive` | Storage-only security account | CloudTrail archive bucket, AWS Config archive bucket, KMS keys, retention controls |
| Future `Security Tooling` | Active security operations account | AWS Config aggregator, GuardDuty delegated administration, OAM, possible Security Hub |

The important split is:

```text
Log Archive = durable evidence storage
Security Tooling = active security operations
```

This separation keeps the log archive account quiet, narrower, and easier to
protect.

## Current Human Access Model

| Identity or permission set | What it is | Intended use |
|---|---|---|
| `platform-admin` | Example normal IAM Identity Center admin user | Routine management-account administration |
| `emergency-admin` | Example dedicated IAM Identity Center emergency user | Emergency access only |
| `BreakGlassAdmin` | IAM Identity Center permission set | Emergency management-account recovery path |
| `AdministratorAccess` | Broad permission set or AWS-managed policy attachment, depending on context | Useful during bootstrap, but should not become the long-term routine model |

`emergency-admin` is **not** a classic IAM user.

It is an IAM Identity Center user. When it signs in, Identity Center grants
temporary access through a permission set that provisions an IAM role in the
target AWS account.

## The Access Chain

The clean way to think about a login is:

```text
Human signs in
-> IAM Identity Center user authenticates
-> user is assigned a permission set
-> permission set targets an AWS account
-> Identity Center provisions/uses an IAM role in that account
-> user receives temporary credentials for that role
-> effective permissions are limited by IAM policy and SCP guardrails
```

For the current break-glass path:

```text
emergency-admin
-> BreakGlassAdmin
-> Management account / <management-account-id>
-> temporary emergency admin role
-> Organizations, SCP, and account recovery actions if required
```

The AWS access portal `Access keys` link provides temporary role credentials for
the selected Identity Center access path. It is not the same thing as creating
or storing permanent IAM user access keys.

## SCPs Versus Permission Sets

This distinction is exam-critical.

| Concept | Grants access? | Limits access? | Where it applies |
|---|---:|---:|---|
| IAM Identity Center permission set | Yes | Yes, by the policies it contains | Assigned user/group to target account |
| IAM role policy | Yes | Yes, by allowed actions/resources | Inside an AWS account |
| SCP | No | Yes | Organization root, OU, or account |
| Root user | Inherently powerful | Can be constrained in member accounts by SCPs, but should be last resort | Per AWS account |

SCPs are guardrails. They do not grant permission. They define the maximum
permissions available inside affected accounts.

That means:

```text
Permission set says "you may"
SCP says "but only up to this boundary"
```

If either layer blocks an action, the action is blocked.

## Root User Versus Break Glass

Root user and break-glass access are related recovery concepts, but they are not
the same thing.

| Path | What it is | When to use |
|---|---|---|
| Normal IAM Identity Center admin path | Day-to-day management access | Routine approved administration |
| `BreakGlassAdmin` | Emergency Identity Center path with MFA and short session | When normal admin access fails or urgent recovery is needed |
| Root user | Account owner identity | Last resort only, with MFA and recovery evidence |

Preferred order:

```text
1. Normal IAM Identity Center access
2. BreakGlassAdmin emergency path
3. Root-user recovery only if the first two fail
```

Root should not be treated as routine administration. It is the parachute, not
the cockpit.

## How To Read An Access Portal View

Seeing this in the AWS access portal:

```text
AWS accounts (1)
Management account / <management-account-id>
- AdministratorAccess
- BreakGlassAdmin
```

means the signed-in Identity Center user can reach the management account using
two visible access paths.

The good signal:

- `BreakGlassAdmin` is visible for the management account.
- The emergency path works far enough to reach the access portal.

The hygiene question:

- Why does `AdministratorAccess` also appear for the same login?

For a clean emergency design, the dedicated break-glass user should ideally have
only the emergency permission set it needs. If `AdministratorAccess` is attached
directly or indirectly to `emergency-admin`, remove that extra assignment
after confirming the assignment source.

## Common Confusions To Watch

| Confusion | Correct interpretation |
|---|---|
| "Is `emergency-admin` an IAM user?" | No. It is an IAM Identity Center user. |
| "Is `BreakGlassAdmin` a user?" | No. It is a permission set. |
| "Are access portal `Access keys` permanent keys?" | No. They are temporary role credentials. |
| "Does an SCP grant permissions?" | No. It only sets a maximum boundary. |
| "Can the workload account detach its own SCP?" | No. SCP administration is controlled from the management account unless a supported delegated path exists. |
| "Should the management account be in an OU?" | No. The management account lives under the organization root and is treated specially. |
| "Should workloads run in the management account?" | No. Workloads belong in member accounts. |

## Lakehouse Case-Study Boundary

Apply this model to a Lakehouse organization by separating the management,
workload, sandbox, log-archive, and security-tooling responsibilities. Treat
the names above as role-based examples, not claims about deployed accounts or
current assignments.

The readiness tracker and dedicated evidence artifacts—not this revision
note—control any current-state claim. Before implementing a break-glass or SCP
change, confirm the actual assignment source, preserve a tested recovery path,
and keep out-of-band recovery material outside Git.

## SAP-C02 Exam Anchors

Use these shortcuts when answering scenario questions:

| Scenario wording | Likely answer pattern |
|---|---|
| Many AWS accounts and human access | IAM Identity Center with permission sets |
| Central account governance | AWS Organizations plus management account |
| Account grouping and shared guardrails | OUs plus SCPs |
| Need to limit what account admins can ever do | SCP |
| Need to grant a human access to an account | IAM Identity Center permission set |
| Need tamper-resistant API audit evidence | Organization CloudTrail into Log Archive account |
| Need central configuration/compliance view | AWS Config aggregator in Security Tooling |
| Need cross-account operational telemetry | OAM / CloudWatch cross-account observability |
| Emergency admin access | Dedicated break-glass path with MFA, short session, logging, and review |

The exam usually rewards the design that separates duties:

```text
Management account = control plane
Workload accounts = applications and data
Log Archive = durable evidence
Security Tooling = active security operations
Identity Center = human access
SCPs = maximum permission boundary
Root = last-resort recovery
```

## Local Evidence Anchors

Use these Lakehouse repo documents for the current project-specific truth:

- `docs/planning/sap-c02-readiness-tracker.md`
- `docs/runbooks/break-glass-access-procedure.md`
- `docs/evidence/domain1-governance-identity-center-current-state-20260625.md`
- `docs/planning/identity-center-permission-set-matrix-20260619.md`
- `docs/adr/0005-aws-organizations-governance-design.md`

Use these governance study notes for exam framing:

- `SAP-C02_Organizations_IdentityCenter_IAM.md`
- `SAP-C02_Security_Observability_Comparison.md`
- `SAP-C02_Mental_Model_Reference_Diagrams.md`
