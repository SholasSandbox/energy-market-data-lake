#!/usr/bin/env python3
"""Check the Terraform IAM boundaries used by Glue and Athena."""

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
IAM_TF = ROOT / "infra" / "terraform" / "lakehouse" / "iam.tf"


def extract_block(source: str, header: str) -> str:
    start = source.find(header)
    if start == -1:
        raise AssertionError(f"Missing Terraform block: {header}")

    opening_brace = source.find("{", start)
    depth = 0
    for index in range(opening_brace, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return source[start : index + 1]

    raise AssertionError(f"Unclosed Terraform block: {header}")


def require(block: str, values: tuple[str, ...], block_name: str) -> None:
    missing = [value for value in values if value not in block]
    if missing:
        raise AssertionError(f"{block_name} is missing: {', '.join(missing)}")


def forbid(block: str, values: tuple[str, ...], block_name: str) -> None:
    present = [value for value in values if value in block]
    if present:
        raise AssertionError(f"{block_name} contains forbidden values: {', '.join(present)}")


def main() -> None:
    source = IAM_TF.read_text(encoding="utf-8")
    glue = extract_block(source, 'data "aws_iam_policy_document" "glue_s3"')
    athena = extract_block(source, 'data "aws_iam_policy_document" "athena_query"')

    require(
        glue,
        (
            'sid = "ListRequiredPrefixes"',
            '"raw/*"',
            '"curated/*"',
            '"scripts/*"',
            'sid = "WriteCuratedObjectsOnly"',
            '"arn:aws:s3:::${local.data_bucket_name}/curated/*"',
        ),
        "Glue S3 policy",
    )
    forbid(
        glue,
        ('"arn:aws:s3:::${local.data_bucket_name}/*"',),
        "Glue S3 policy",
    )

    require(
        athena,
        (
            'sid = "UseLakehouseWorkgroup"',
            "aws_athena_workgroup.lakehouse.arn",
            'sid = "ReadLakehouseCatalog"',
            '"glue:GetTable"',
            '"glue:GetPartitions"',
            'sid = "ReadCuratedObjects"',
            '"arn:aws:s3:::${local.data_bucket_name}/curated/*"',
            'sid = "ManageBoundedQueryResults"',
            '"arn:aws:s3:::${local.data_bucket_name}/${local.athena_results_prefix}*"',
        ),
        "Athena query policy",
    )
    forbid(
        athena,
        (
            '"arn:aws:s3:::${local.data_bucket_name}/*"',
            "/raw/",
            '"s3:DeleteObject"',
            '"glue:Create',
            '"glue:Delete',
            '"glue:Update',
            '"athena:CreateWorkGroup"',
            '"athena:DeleteWorkGroup"',
            '"athena:UpdateWorkGroup"',
        ),
        "Athena query policy",
    )

    print("Glue and Athena IAM policy boundaries are valid.")


if __name__ == "__main__":
    main()
