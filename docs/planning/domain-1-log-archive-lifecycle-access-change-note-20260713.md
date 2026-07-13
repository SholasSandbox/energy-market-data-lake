# Domain 1 Change Note - Log Archive Lifecycle Operator Access - 2026-07-13

## Status

Completed on 2026-07-13 under direct user instruction. The immediate precheck
and postchange validation succeeded.

## Purpose and Scope

Create `LogArchiveLifecycleOperator` with a one-hour session duration and a
direct assignment to the existing normal administrator in **Security Log
Archive only**. Its inline policy is
`docs/policies/iam-log-archive-lifecycle-operator.inline-policy.example.json`.

The permission set can read lifecycle, versioning, encryption, and
public-access-block posture and update lifecycle configurations for exactly the
CloudTrail and AWS Config archive buckets. It does not allow object reads,
deletes, bucket-policy changes, encryption changes, IAM administration, KMS
actions, Organizations actions, or access to any other account.

## Fresh Precheck

The 2026-07-13 IAM Identity Center inventory confirms that Security Log Archive
has no current permission-set assignment. A management-role attempt to read the
two bucket lifecycle configurations was denied, and the current SSO session had
no role assignment in Security Log Archive. The permission gap is therefore
real and scoped to lifecycle administration.

## Blast Radius, Rollback, and Validation

Blast radius is one normal administrator, one Security Log Archive account, and
two named buckets. No workload or management-account permission changes occur.

Rollback is to delete the direct account assignment, then delete the permission
set after confirming it has no other assignments. This does not alter existing
archive data or lifecycle rules.

Validation requires that a fresh SSO session can obtain the new role, that the
role can read both target lifecycle configurations, and that an attempted read
of a non-target bucket remains denied or unavailable.

## Execution Result

The permission set was created, its inline policy was validated with IAM Access
Analyzer, and its direct Security Log Archive assignment provisioned
successfully. A fresh SSO role session read both target lifecycle configurations
and the required immutable bucket posture checks. The initial narrow policy was
expanded only with the three required read-only posture actions before the S3
write; it remains scoped to the same two buckets.

## Cost and Evidence Boundary

The permission set itself has no direct service charge. Public evidence records
only the role name, account role, bucket purpose, and validation outcome; user
and contact identifiers remain outside the repository.
