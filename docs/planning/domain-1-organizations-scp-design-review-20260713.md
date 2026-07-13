# Domain 1 Organizations/SCP Design Review - 2026-07-13

## Status and Boundary

This is the documentation-only Organizations/SCP design slice scheduled for
the 2026-07-20 tracker week and started early under direct user instruction.
It uses fresh read-only Organizations, CloudTrail, S3 inventory, and IAM Access
Analyzer evidence. It does not create, update, attach, detach, or delete an
SCP, OU, account, trail, bucket, or IAM assignment.

## Fresh State

The organization root has `SERVICE_CONTROL_POLICY` enabled. The active member
account boundaries remain:

| Boundary | Current SCP posture |
|---|---|
| Lakehouse Workloads OU | `FullAWSAccess`, `DenyLeavingOrganization-LakehouseWorkloads`, and `DenyRootUserActions-LakehouseWorkloads` |
| Security OU | `FullAWSAccess` only |
| Container Sandbox | Not assessed for rollout in this slice |

The Lakehouse account currently sees both a legacy member-account management
trail and the central organization trail. The organization trail is
multi-Region and has log-file validation enabled. This makes CloudTrail
protection valuable, but it also means the legacy member-trail ownership and
retention need to be understood before adding a new deny guardrail.

## Design Decision

Keep the two existing Lakehouse-OU SCPs unchanged. Do not broaden either to
Security OU, Container Sandbox, or the root.

The next potential implementation candidate is a narrowly targeted
`DenyDisablingOrDeletingCloudTrail` SCP for **Lakehouse Workloads OU only**.
It protects the member-account trail control plane without attempting a
root-level or multi-OU rollout. SCPs cap member-account permissions and do not
affect management-account identities or service-linked roles; existing IAM and
resource policies still determine granted access.

This is a design selection, not an implementation approval. Before a separate
live-change request can be considered, record the member-trail retention/owner
decision, review CloudTrail API usage and recovery needs, simulate or
equivalently review the exact policy, capture a fresh target inventory, and
prepare a detach rollback command plus post-attach validation.

## Remaining Catalogue Order

| Candidate | Decision | Reason |
|---|---|---|
| Deny disabling CloudTrail | Next design candidate; not approved to attach | Narrow Lakehouse-OU scope and current dual-trail posture make the control easy to reason about after legacy-trail review. |
| Deny delete/change of log archive buckets | Hold for Security OU design | It requires confirmed archive bucket names, lifecycle/retention operations, KMS and service-delivery effects, and a separate Security OU blast-radius review. |
| Deny public S3 exposure | Corrected example; hold | The prior example had invalid public-access-block condition keys. The corrected version protects public ACLs and prevents any public-access-block change, but cannot evaluate bucket-policy content; it needs a bucket-by-bucket exception model. |
| Require encryption where supported | Hold | It prevents only future creates and needs service-specific exception and workload deployment review. It does not remediate existing resources. |
| Deny unapproved Regions | Hold | The global-service exception list and active workload Regions need a usage review before a deny is safe. |

## Policy Quality Review

All SCP examples parse as JSON. IAM Access Analyzer found no findings for the
CloudTrail, log archive, leaving-organization, Region, or encryption examples.
The root-user example had a non-blocking recommendation to use an ARN operator;
its local example now uses `ArnLike` for `aws:PrincipalArn`.

The public-S3 example had four invalid S3 condition keys. It now:

- denies public ACL values for `PutBucketAcl` and `PutObjectAcl`; and
- denies all account- and bucket-level public-access-block changes once
  attached.

It intentionally does **not** claim to inspect or distinguish public
`PutBucketPolicy` content. That requires a separately reviewed control pattern
and must not be inferred from an ACL condition.

## Future Change Unit

If later explicitly approved, the CloudTrail SCP change unit must be limited to
Lakehouse Workloads OU and include:

1. immediate read-only inventory of OU membership, attached policies, trails,
   relevant IAM/Identity Center paths, and recent CloudTrail API usage;
2. policy review of `docs/policies/scp/deny-disable-cloudtrail.example.json`;
3. one customer-managed policy creation and one OU attachment only;
4. a rollback that detaches the policy without changing OU placement or other
   policies; and
5. validation of expected attachment, unchanged organization-trail delivery,
   unchanged workload operation, and public-safe evidence redaction.

## SAP-C02 Relevance

This supports Domain 1 by separating OU-scoped permission guardrails from IAM
grants, preserving management-account and delegated-service exceptions, and
sequencing SCP rollout through limited blast radius, test evidence, and
rollback.
