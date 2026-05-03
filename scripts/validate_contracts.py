#!/usr/bin/env python3
"""Validate project JSON examples against their JSON Schema contracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
EXAMPLE_DIR = SCHEMA_DIR / "examples"
EVIDENCE_DIR = ROOT / "docs" / "evidence"
CURATED_EVIDENCE_DIR = EVIDENCE_DIR / "curated"
DASHBOARD_PUBLIC_DIR = ROOT / "dashboard-ui" / "public"

# Each tuple is: schema file, matching example file.
CONTRACTS = [
    ("energy_input_v1.schema.json", "energy_input_v1.example.json"),
    ("news_summary_v1.schema.json", "news_summary_v1.example.json"),
    ("ai_insight_v1.schema.json", "ai_insight_v1.example.json"),
    ("dashboard_snapshot_v1.schema.json", "dashboard_snapshot_v1.example.json"),
]

# Generated evidence files are optional because they only exist after local runs.
EVIDENCE_CONTRACTS = [
    ("energy_input_v1.schema.json", EVIDENCE_DIR / "energy_input_v1.sample.json"),
    ("news_summary_v1.schema.json", CURATED_EVIDENCE_DIR / "news_summary_v1.sample.json"),
    ("ai_insight_v1.schema.json", CURATED_EVIDENCE_DIR / "ai_insight_v1.sample.json"),
    ("dashboard_snapshot_v1.schema.json", DASHBOARD_PUBLIC_DIR / "dashboard_snapshot_v1.sample.json"),
]

# Known-bad samples prove the failure path catches malformed payloads.
FAILURE_CONTRACTS = [
    ("energy_input_v1.schema.json", EVIDENCE_DIR / "failed" / "bad_energy_input_v1.sample.json"),
    ("ai_insight_v1.schema.json", EVIDENCE_DIR / "failed" / "bad_ai_insight_v1.sample.json"),
]


def load_json(path: Path) -> dict:
    """Load a JSON file and include the path in any error message."""
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON: {exc}") from exc


def validate_contract(schema_path: Path, example_path: Path) -> list[str]:
    """Return a list of validation errors for one schema/example pair."""
    errors: list[str] = []

    try:
        schema = load_json(schema_path)
        example = load_json(example_path)
    except ValueError as exc:
        return [str(exc)]

    try:
        Draft202012Validator.check_schema(schema)
    except Exception as exc:
        return [f"{schema_path}: invalid JSON Schema: {exc}"]

    # FormatChecker enforces date-time, date, uri, etc.
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    for error in sorted(validator.iter_errors(example), key=lambda err: list(err.path)):
        location = ".".join(str(part) for part in error.path) or "<root>"
        errors.append(f"{example_path}: {location}: {error.message}")

    return errors


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate JSON files against project schemas")
    parser.add_argument(
        "--include-evidence",
        action="store_true",
        help="also validate generated files under docs/evidence when present",
    )
    parser.add_argument(
        "--check-failures",
        action="store_true",
        help="also confirm known-bad samples are rejected by their schemas",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    all_errors: list[str] = []

    for schema_name, example_name in CONTRACTS:
        schema_path = SCHEMA_DIR / schema_name
        example_path = EXAMPLE_DIR / example_name

        print(f"Validating {example_name} against {schema_name}...")
        all_errors.extend(validate_contract(schema_path, example_path))

    if args.include_evidence:
        for schema_name, evidence_path in EVIDENCE_CONTRACTS:
            schema_path = SCHEMA_DIR / schema_name

            if not evidence_path.exists():
                print(f"Skipping missing evidence file {evidence_path}")
                continue

            print(f"Validating {evidence_path.name} against {schema_name}...")
            all_errors.extend(validate_contract(schema_path, evidence_path))

    if args.check_failures:
        for schema_name, failure_path in FAILURE_CONTRACTS:
            schema_path = SCHEMA_DIR / schema_name

            if not failure_path.exists():
                print(f"Skipping missing failure sample {failure_path}")
                continue

            print(f"Confirming {failure_path.name} is rejected by {schema_name}...")
            failure_errors = validate_contract(schema_path, failure_path)
            if failure_errors:
                print(f"Rejected as expected: {failure_errors[0]}")
            else:
                all_errors.append(f"{failure_path}: expected validation failure, but file passed")

    if all_errors:
        print("\nValidation failed:\n")
        for error in all_errors:
            print(f"- {error}")
        return 1

    print("\nAll contracts are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
