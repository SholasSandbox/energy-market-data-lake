# Domain 1 Governance Change Note - Workload Billing Root Recovery - 2026-07-13

## Status

Completed and audit-reconciled. The account owner confirmed that the past-due
payment was made, the temporary SCP recovery exception was removed immediately
afterwards, and the delayed CloudTrail management events were later confirmed.

## Trigger

- AWS sent a past-due-payment notice for workload account `464975959576`.
- The account's root user was blocked from Billing by the intentional
  `DenyRootUserActions-LakehouseWorkloads` SCP.
- The account is active and has been an invited member of the organization
  since 2026-06-05; the organization management account is `349687196588`.

## Approval and Scope

The user explicitly requested investigation and resolution to avoid account
suspension. The authorized emergency scope was limited to restoring the
workload root user's Billing access long enough to complete the payment.

No IAM, payment-method, account-placement, workload-resource, or permanent
SCP-policy-content change was made by this recovery action.

## Fresh Prechange Evidence

At 2026-07-13 16:34 BST, the management-account `org-admin` session confirmed:

- workload account `464975959576` is active and remains in
  `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`;
- `DenyRootUserActions-LakehouseWorkloads` / `p-dv2ss5us` was attached only to
  that OU;
- the OU also had `FullAWSAccess` and
  `DenyLeavingOrganization-LakehouseWorkloads` / `p-4stxl0u2` attached;
- the active SCP denies all root-user actions except narrowly defined account
  recovery and MFA actions, which explains the Billing-console denial.

## Recovery Change

At 2026-07-13 16:34 BST, the management-account `org-admin` session detached
only `p-dv2ss5us` from `ou-gbyf-m6ppfmpq`:

```bash
aws organizations detach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

Immediate postchange verification returned no targets for `p-dv2ss5us` and
showed that `DenyLeavingOrganization-LakehouseWorkloads` and `FullAWSAccess`
remain attached to the OU.

## Restoration and Postchange Verification

The account owner confirmed payment completion at 2026-07-13 16:51 BST. The
management-account `org-admin` session then restored the guardrail:

```bash
aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

Immediate postchange verification confirmed:

- `p-dv2ss5us` has exactly one target:
  `ou-gbyf-m6ppfmpq` / `Lakehouse Workloads OU`;
- the OU again has exactly `FullAWSAccess`,
  `DenyLeavingOrganization-LakehouseWorkloads`, and
  `DenyRootUserActions-LakehouseWorkloads` attached;
- workload account `464975959576` remains `ACTIVE` and in the organization.

The management-account CloudTrail Event History did not yet return either the
detach or reattach event immediately after the change. Payment-card data and
payment amount are intentionally not stored in repository evidence.

## Delayed Audit-Trail Reconciliation

On 2026-07-14, a read-only CloudTrail Event History check in `us-east-1`
confirmed the delayed AWS Organizations management events:

- `DetachPolicy` at `2026-07-13T15:34:29Z`, for
  `DenyRootUserActions-LakehouseWorkloads` on `Lakehouse Workloads OU`;
- `AttachPolicy` at `2026-07-13T15:50:55Z`, restoring the same policy to the
  same OU.

This confirms a temporary exception of approximately 16 minutes and 26 seconds.
The publication record intentionally excludes operator identity, source IP,
temporary credentials, payment details, and raw CloudTrail event payloads.

## Rollback / Restoration Command

```bash
aws organizations attach-policy \
  --profile org-admin \
  --policy-id p-dv2ss5us \
  --target-id ou-gbyf-m6ppfmpq
```

## Risk

The temporary exception is no longer active. The workload root user is again
subject to `DenyRootUserActions-LakehouseWorkloads`; the account also remained
in the organization and protected by the no-leaving-organization SCP throughout
the incident.

## SAP-C02 Relevance

This is Domain 1 recovery evidence: a preventive SCP is temporarily relaxed
only for a bounded billing incident, with a documented restoration path and
postchange verification requirement.
