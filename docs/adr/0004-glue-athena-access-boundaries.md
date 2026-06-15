# ADR 0004: Glue And Athena Access Boundaries

- Status: Accepted; deployed and live verified
- Date: 2026-06-15
- Decision owners: Energy Data Lakehouse repository owner
- Related tracker milestone: June-July lakehouse readiness closure

## Context

The lakehouse uses one data bucket with `raw/`, `curated/`, `scripts/`, and
`athena-results/` prefixes. The existing Glue role had S3 read, write, and
delete access across the whole bucket. Athena had a workgroup and query-result
location but no dedicated role for human or automated query access.

Those permissions did not express the data-flow boundaries required by the
tracker. Glue needs source, script, and curated access to crawl and transform
data. Athena query users need curated data, catalog metadata, the named
workgroup, and a bounded result location; they do not need raw-data access,
catalog mutation, or workgroup administration.

## Decision

### Glue Role

Keep the dedicated Glue service role and replace whole-bucket object access
with the following S3 boundary:

| Prefix | Read | Write | Delete | Purpose |
|---|---:|---:|---:|---|
| `raw/` | Yes | No | No | Raw crawler and ETL source |
| `curated/` | Yes | Yes | Yes | Curated crawler and ETL output replacement |
| `scripts/` | Yes | No | No | Glue ETL script loading |
| `athena-results/` | No | No | No | Not required by Glue |

Bucket listing is constrained with the `s3:prefix` condition to `raw/`,
`curated/`, and `scripts/`. Version-aware reads and listing are included
because bucket versioning is enabled. Multipart permissions apply only to
curated output objects.

The AWS-managed `AWSGlueServiceRole` policy remains attached for Glue service
operations and logging. Data-bucket object access is supplied by the custom,
prefix-scoped policy.

### Athena Query Role

Create `energy-market-athena-query-role` as the dedicated query boundary. Its
inline policy permits:

- query execution and result retrieval through the configured Athena
  workgroup only;
- read-only Glue Data Catalog access to the lakehouse database and its tables;
- S3 reads from `curated/` only; and
- S3 reads and writes under `athena-results/` only.

The role cannot read `raw/`, mutate the Glue catalog, administer Athena
workgroups, delete query-result objects, or access all objects in the bucket.
The workgroup continues to enforce its S3 output location and SSE-S3 result
encryption.

The trust policy delegates role assumption within the current AWS account.
That trust does not grant access by itself: an approved user, permission set,
or automation role must also receive an identity policy allowing
`sts:AssumeRole` on this role ARN. Cross-account trust and console-wide list
permissions remain outside this closure step.

## Deployment Boundary

The Terraform configuration and local policy checks are complete, but no AWS
IAM resource has been created or changed by this ADR. Before deployment:

1. review a saved Terraform plan for the Glue policy update and new Athena
   role;
2. identify the approved principal or Identity Center permission set that may
   assume the Athena role;
3. obtain explicit approval for the IAM change;
4. apply only the reviewed IAM delta; and
5. run representative Glue crawler/job and Athena query tests, then capture
   the effective role and result-location evidence.

Rollback restores the previous Glue inline policy and removes the new Athena
role only after confirming that no approved principal or automation depends on
it. Rollback must not broaden another role to replace the dedicated boundary.

## SAP-C02 Relevance

This decision demonstrates least-privilege workload design, separation of
service and analyst duties, resource and prefix scoping, role-assumption
boundaries, and the interaction between Athena, Glue Data Catalog, and S3
permissions. It primarily supports SAP-C02 Domain 2 and improves the existing
solution under Domain 3.

## Consequences

- Glue can transform raw data without write or delete access to the raw zone.
- Athena query users receive a reusable role without access to raw source data
  or administrative APIs.
- New datasets outside the lakehouse database or prefixes require an explicit
  policy review rather than inheriting access automatically.
- Live verification is complete for the approved Glue/Athena IAM boundary.

## Implementation Artifacts

- `infra/terraform/lakehouse/iam.tf`
- `infra/terraform/lakehouse/variables.tf`
- `infra/terraform/lakehouse/outputs.tf`
- `scripts/check_lakehouse_iam_policies.py`
- `docs/glue-athena-iam-deployment-runbook.md`

## References

- [Athena IAM actions and resource types](https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazonathena.html)
- [Fine-grained Glue Data Catalog access](https://docs.aws.amazon.com/athena/latest/ug/fine-grained-access-to-glue-resources.html)
- [Athena workgroup IAM policies](https://docs.aws.amazon.com/athena/latest/ug/workgroups-iam-policy.html)
- [Athena access to S3](https://docs.aws.amazon.com/athena/latest/ug/s3-permissions.html)
- [Amazon S3 policy condition keys](https://docs.aws.amazon.com/AmazonS3/latest/userguide/amazon-s3-policy-keys.html)
