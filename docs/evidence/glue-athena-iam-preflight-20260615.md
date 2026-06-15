# Glue And Athena IAM Preflight - 2026-06-15

## Scope

This evidence records local validation of the Glue least-privilege policy and
the dedicated Athena query role. It does not record an AWS IAM deployment.

## Policy Boundaries

- Glue lists and reads only `raw/`, `curated/`, and `scripts/` in the shared
  data bucket.
- Glue writes and deletes only under `curated/`.
- Athena query execution is restricted to
  `energy-market-workgroup`.
- Athena receives read-only Glue Catalog access for `energy_market_lake` and
  its tables.
- Athena reads only `curated/` and writes only `athena-results/`.
- Athena has no raw-zone access, catalog mutation, workgroup administration,
  query-result deletion, or whole-bucket object ARN.

## Validation

The local policy contract completed successfully:

```text
python3 scripts/check_lakehouse_iam_policies.py
Glue and Athena IAM policy boundaries are valid.
```

Terraform formatting and validation completed successfully:

```text
terraform -chdir=infra/terraform/lakehouse fmt
terraform -chdir=infra/terraform/lakehouse validate
Success! The configuration is valid.
```

A targeted, refresh-disabled preflight plan produced:

```text
Plan: 2 to add, 1 to change, 0 to destroy.
```

The planned changes are:

1. create `aws_iam_role.athena_query`;
2. create `aws_iam_role_policy.athena_query`; and
3. update `aws_iam_role_policy.glue_s3` in place.

AWS IAM Access Analyzer `ValidatePolicy` returned no findings for either
rendered identity policy.

## Limitations And Deployment Gate

The plan used `-target` to isolate the policy delta and is preflight evidence,
not an approved apply artifact. Before deployment, generate and review the
normal root saved plan with the required live-preservation variables, identify
the principal allowed to assume the Athena role, and obtain explicit approval.

No Terraform apply, IAM mutation, Glue run, or Athena query was performed in
this step.
