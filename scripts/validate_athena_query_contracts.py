#!/usr/bin/env python3
"""Validate the durable Athena query inventory without contacting AWS."""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "athena" / "query-contracts.json"
QUERY_HEADER = re.compile(r"^--\s*(\d+)\)\s*(.+?)\s*$", re.MULTILINE)
TABLE_REFERENCE = re.compile(
    r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_.$]*)",
    re.IGNORECASE,
)
CTE_NAME = re.compile(
    r"(?:\bWITH|,)\s*([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(",
    re.IGNORECASE,
)
MUTATING_KEYWORDS = (
    "ALTER",
    "CREATE",
    "DELETE",
    "DROP",
    "INSERT",
    "MERGE",
    "MSCK",
    "TRUNCATE",
    "UNLOAD",
    "UPDATE",
)


@dataclass(frozen=True)
class QueryBlock:
    """One numbered query extracted from the durable SQL file."""

    query_id: int
    title: str
    sql: str


def load_manifest(path: Path) -> dict[str, Any]:
    """Load the JSON query inventory."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Manifest does not exist: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {path}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"Manifest root must be an object: {path}")
    return data


def parse_query_blocks(source: str) -> list[QueryBlock]:
    """Extract numbered, semicolon-terminated query blocks from Markdown-style comments."""
    matches = list(QUERY_HEADER.finditer(source))
    blocks: list[QueryBlock] = []

    for index, match in enumerate(matches):
        body_start = match.end()
        body_end = matches[index + 1].start() if index + 1 < len(matches) else len(source)
        body = source[body_start:body_end].strip()
        semicolon = body.find(";")
        if semicolon == -1:
            sql = body
        else:
            sql = body[: semicolon + 1].strip()
            trailing = body[semicolon + 1 :].strip()
            if trailing and not all(
                line.lstrip().startswith("--") for line in trailing.splitlines()
            ):
                sql = body

        blocks.append(
            QueryBlock(
                query_id=int(match.group(1)),
                title=match.group(2).strip(),
                sql=sql,
            )
        )

    return blocks


def _without_comments_and_literals(sql: str) -> str:
    without_comments = re.sub(r"--.*?$", " ", sql, flags=re.MULTILINE)
    return re.sub(r"'(?:''|[^'])*'", "''", without_comments)


def referenced_tables(sql: str) -> set[str]:
    """Return physical FROM/JOIN targets, excluding CTE names."""
    normalized = _without_comments_and_literals(sql)
    ctes = {name.lower() for name in CTE_NAME.findall(normalized)}
    tables = {
        reference.split(".")[-1].lower()
        for reference in TABLE_REFERENCE.findall(normalized)
    }
    return tables - ctes


def _validate_manifest_shape(manifest: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    if manifest.get("version") != 1:
        errors.append("Manifest version must be 1")
    if not manifest.get("database"):
        errors.append("Manifest must declare a database")
    if manifest.get("business_criticality_status") != "not-approved":
        errors.append(
            "business_criticality_status must remain 'not-approved' until owner approval"
        )
    if not isinstance(manifest.get("queries"), list) or not manifest.get("queries"):
        errors.append("Manifest queries must be a non-empty list")
    return errors


def validate_contracts(
    manifest: dict[str, Any], blocks: list[QueryBlock]
) -> list[str]:
    """Return all static contract errors for the manifest and SQL blocks."""
    errors = _validate_manifest_shape(manifest)
    entries = manifest.get("queries", [])
    if not isinstance(entries, list):
        return errors

    entry_ids = [entry.get("id") for entry in entries if isinstance(entry, dict)]
    expected_ids = list(range(1, len(entries) + 1))
    if entry_ids != expected_ids:
        errors.append(
            f"Manifest query IDs must be sequential {expected_ids}; found {entry_ids}"
        )

    names = [entry.get("name") for entry in entries if isinstance(entry, dict)]
    if len(names) != len(set(names)):
        errors.append("Manifest query names must be unique")

    block_ids = [block.query_id for block in blocks]
    if block_ids != expected_ids:
        errors.append(
            f"SQL query IDs must be sequential {expected_ids}; found {block_ids}"
        )

    entry_by_id = {
        entry["id"]: entry
        for entry in entries
        if isinstance(entry, dict) and isinstance(entry.get("id"), int)
    }

    for block in blocks:
        entry = entry_by_id.get(block.query_id)
        if entry is None:
            errors.append(f"SQL query {block.query_id} has no manifest entry")
            continue

        label = f"Query {block.query_id} ({entry.get('name', '<unnamed>')})"
        if entry.get("title") != block.title:
            errors.append(
                f"{label}: title mismatch; manifest={entry.get('title')!r}, "
                f"SQL={block.title!r}"
            )

        sql_without_literals = _without_comments_and_literals(block.sql)
        first_keyword = re.search(r"\b([A-Za-z]+)\b", sql_without_literals)
        if first_keyword is None or first_keyword.group(1).upper() not in {"SELECT", "WITH"}:
            errors.append(f"{label}: query must start with SELECT or WITH")

        for keyword in MUTATING_KEYWORDS:
            if re.search(rf"\b{keyword}\b", sql_without_literals, re.IGNORECASE):
                errors.append(f"{label}: mutating keyword is forbidden: {keyword}")

        if not block.sql.rstrip().endswith(";"):
            errors.append(f"{label}: SQL block must end with a semicolon")

        declared_tables = entry.get("tables")
        if not isinstance(declared_tables, list) or not declared_tables:
            errors.append(f"{label}: tables must be a non-empty list")
        else:
            declared = {str(table).lower() for table in declared_tables}
            actual = referenced_tables(block.sql)
            if actual != declared:
                errors.append(
                    f"{label}: table mismatch; manifest={sorted(declared)}, "
                    f"SQL={sorted(actual)}"
                )

        expected_columns = entry.get("expected_output_columns")
        if not isinstance(expected_columns, list) or not expected_columns:
            errors.append(f"{label}: expected_output_columns must be non-empty")
        else:
            missing_columns = [
                column
                for column in expected_columns
                if not re.search(
                    rf"\b{re.escape(str(column))}\b",
                    sql_without_literals,
                    re.IGNORECASE,
                )
            ]
            if missing_columns:
                errors.append(
                    f"{label}: expected output columns absent from SQL: "
                    + ", ".join(str(column) for column in missing_columns)
                )

        if not entry.get("recovery_use"):
            errors.append(f"{label}: recovery_use must be declared")

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the read-only Athena query inventory"
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = load_manifest(args.manifest)
        query_file = ROOT / str(manifest.get("query_file", ""))
        source = query_file.read_text(encoding="utf-8")
    except (OSError, ValueError) as exc:
        print(f"Athena query contract validation failed: {exc}", file=sys.stderr)
        return 1

    blocks = parse_query_blocks(source)
    errors = validate_contracts(manifest, blocks)
    if errors:
        print("Athena query contract validation failed:", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    print(
        f"Validated {len(blocks)} read-only Athena query contracts "
        f"for {manifest['database']}."
    )
    print("Business criticality remains not approved; no RTO/RPO claim is made.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
