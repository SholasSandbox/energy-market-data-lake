# SCP Policy Examples

<!-- markdownlint-disable MD013 -->

These Service Control Policy examples support the Domain 1 governance design in
`docs/adr/0005-aws-organizations-governance-design.md`.

They are **examples only** until a separate change note and explicit approval
promote one into a customer-managed SCP. This repository does not authorize live
AWS Organizations changes by itself.

Current promoted examples:

- `deny-leaving-organization.example.json` was promoted to
  `DenyLeavingOrganization-LakehouseWorkloads` and attached to
  `Lakehouse Workloads OU`.
- `deny-root-user-actions.example.json` was promoted to
  `DenyRootUserActions-LakehouseWorkloads` and attached to
  `Lakehouse Workloads OU`.

## Use Boundary

Before any SCP is attached, create a separate implementation note with:

1. target OU or account;
2. current read-only Organizations inventory;
3. policy simulation or equivalent review;
4. service exceptions;
5. rollback command;
6. expected blast radius;
7. explicit approval for that one change.

SCPs define maximum allowed permissions. They do not grant access. IAM policies,
resource policies, and IAM Identity Center permission sets still grant access
inside the allowed boundary.

## Example Catalogue

| File | Purpose | Attach only after |
| --- | --- | --- |
| `deny-disable-cloudtrail.example.json` | Protect audit trail operation. | Organization trail design and break-glass path exist. |
| `deny-delete-log-archive-buckets.example.json` | Protect central log buckets from deletion or policy weakening. | Log archive account, bucket names, KMS posture, and retention are finalized. |
| `deny-public-s3-exposure.example.json` | Reduce accidental public S3 exposure. | Approved public dashboard exception model is tested. |
| `deny-unapproved-regions.example.json` | Limit cost and data sprawl outside approved Regions. | Global-service exceptions and required Regions are confirmed. |
| `deny-root-user-actions.example.json` | Reduce root-user blast radius. | Live for `Lakehouse Workloads OU` after emergency root-use process was documented/tested and the live attach was separately approved. |
| `require-encryption-where-supported.example.json` | Encourage encryption baseline for services with stable condition keys. | Service support and exception list are reviewed. |
| `deny-leaving-organization.example.json` | Prevent member-account governance bypass. | Management-account recovery model is documented. |

## SAP-C02 Notes

- SCPs are evaluated alongside IAM and resource policies.
- An explicit deny in an SCP overrides an allow in IAM.
- SCPs do not affect the management account.
- SCPs should be staged from observation/design to narrow OU attachment, then
  broader rollout only after validation.
