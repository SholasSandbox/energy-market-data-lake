#!/usr/bin/env python3
"""Validate project JSON examples against their JSON Schema contracts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
EXAMPLE_DIR = SCHEMA_DIR / "examples"
EVIDENCE_DIR = ROOT / "docs" / "evidence"
CURATED_EVIDENCE_DIR = EVIDENCE_DIR / "curated"
DASHBOARD_PUBLIC_DIR = ROOT / "dashboard-ui" / "public"
CORPUS_DIR = ROOT / "evaluation" / "ai-orchestration" / "p2"

# Each tuple is: schema file, matching example file.
CONTRACTS = [
    ("energy_input_v1.schema.json", "energy_input_v1.example.json"),
    ("news_summary_v1.schema.json", "news_summary_v1.example.json"),
    ("ai_input_bundle_v1.schema.json", "ai_input_bundle_v1.example.json"),
    ("ai_insight_v1.schema.json", "ai_insight_v1.example.json"),
    (
        "ai_structured_evidence_v1.schema.json",
        "ai_structured_evidence_v1.example.json",
    ),
    (
        "ai_document_evidence_v1.schema.json",
        "ai_document_evidence_v1.example.json",
    ),
    ("dashboard_snapshot_v1.schema.json", "dashboard_snapshot_v1.example.json"),
]

# Versioned corpus records are valid contract examples and must always pass.
CORPUS_CONTRACTS = [
    (
        "ai_corpus_manifest_v1.schema.json",
        CORPUS_DIR / "corpus-manifest-v1.json",
    ),
    (
        "ai_corpus_exclusions_v1.schema.json",
        CORPUS_DIR / "exclusions-v1.json",
    ),
    (
        "ai_policy_fixture_v1.schema.json",
        CORPUS_DIR / "policy-fixtures-v1.json",
    ),
    (
        "ai_evaluation_case_v1.schema.json",
        CORPUS_DIR / "evaluation-set-v1.json",
    ),
    (
        "ai_holdout_gold_v1.schema.json",
        CORPUS_DIR / "holdout" / "holdout-gold-v1.json",
    ),
]

# Generated evidence files are optional because they only exist after local runs.
EVIDENCE_CONTRACTS = [
    ("energy_input_v1.schema.json", EVIDENCE_DIR / "energy_input_v1.sample.json"),
    ("news_summary_v1.schema.json", CURATED_EVIDENCE_DIR / "news_summary_v1.sample.json"),
    ("ai_input_bundle_v1.schema.json", EVIDENCE_DIR / "ai" / "ai_input_bundle_v1.sample.json"),
    ("ai_insight_v1.schema.json", CURATED_EVIDENCE_DIR / "ai_insight_v1.sample.json"),
    ("dashboard_snapshot_v1.schema.json", DASHBOARD_PUBLIC_DIR / "dashboard_snapshot_v1.sample.json"),
]

# Known-bad samples prove the failure path catches malformed payloads.
# Each tuple is: schema file, known-bad file, required error fragment or None.
FAILURE_CONTRACTS = [
    (
        "energy_input_v1.schema.json",
        EVIDENCE_DIR / "failed" / "bad_energy_input_v1.sample.json",
        None,
    ),
    (
        "ai_insight_v1.schema.json",
        EVIDENCE_DIR / "failed" / "bad_ai_insight_v1.sample.json",
        None,
    ),
    (
        "ai_structured_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_structured_evidence_v1.arbitrary_sql.invalid.json",
        "'sql' was unexpected",
    ),
    (
        "ai_structured_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_structured_evidence_v1.absent_with_value.invalid.json",
        "'value' was unexpected",
    ),
    (
        "ai_structured_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_structured_evidence_v1.derived_without_operands.invalid.json",
        "'operands' is a required property",
    ),
    (
        "ai_document_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_document_evidence_v1.oversized_passage.invalid.json",
        "is too long",
    ),
    (
        "ai_document_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_document_evidence_v1.public_citation_internal_field.invalid.json",
        "'internal_provenance' was unexpected",
    ),
    (
        "ai_document_evidence_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_document_evidence_v1.metadata_only_with_text.invalid.json",
        "'text' was unexpected",
    ),
]

# Compact mutation fixtures materialize one deliberate invalid payload from a
# valid versioned corpus record. This avoids duplicating large manifests while
# keeping each expected failure isolated and reviewable.
MUTATION_FAILURE_CONTRACTS = [
    (
        "ai_corpus_manifest_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_corpus_manifest_v1.incomplete_active.invalid.json",
    ),
    (
        "ai_corpus_manifest_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_corpus_manifest_v1.revoked_active_entry.invalid.json",
    ),
    (
        "ai_corpus_manifest_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_corpus_manifest_v1.missing_exclusion_hash.invalid.json",
    ),
    (
        "ai_corpus_exclusions_v1.schema.json",
        EXAMPLE_DIR
        / "invalid"
        / "ai_corpus_exclusions_v1.answer_eligible.invalid.json",
    ),
]


class DuplicateKeyError(ValueError):
    """Raised when JSON contains an ambiguous duplicate object member."""


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict:
    """Build a JSON object while rejecting duplicate member names."""
    result: dict = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateKeyError(f"duplicate object member {key!r}")
        result[key] = value
    return result


def load_json(path: Path) -> dict:
    """Load a JSON file and include the path in any error message."""
    try:
        with path.open("r", encoding="utf-8") as file:
            return json.load(file, object_pairs_hook=reject_duplicate_keys)
    except OSError as exc:
        raise ValueError(f"{path}: unable to read JSON: {exc}") from exc
    except (json.JSONDecodeError, DuplicateKeyError) as exc:
        raise ValueError(f"{path}: invalid JSON: {exc}") from exc


def validate_payload(
    schema_path: Path,
    example: dict,
    example_path: Path,
) -> list[str]:
    """Return schema-validation errors for an already loaded payload."""
    errors: list[str] = []

    try:
        schema = load_json(schema_path)
    except ValueError as exc:
        return [str(exc)]

    try:
        Draft202012Validator.check_schema(schema)
    except Exception as exc:
        return [f"{schema_path}: invalid JSON Schema: {exc}"]

    # FormatChecker enforces date-time, date, uri, etc.
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    def append_error(error: ValidationError) -> None:
        location = ".".join(str(part) for part in error.path) or "<root>"
        errors.append(f"{example_path}: {location}: {error.message}")
        for nested_error in error.context:
            append_error(nested_error)

    for error in sorted(validator.iter_errors(example), key=lambda err: list(err.path)):
        append_error(error)

    return errors


def validate_contract(schema_path: Path, example_path: Path) -> list[str]:
    """Return a list of validation errors for one schema/example pair."""
    try:
        example = load_json(example_path)
    except ValueError as exc:
        return [str(exc)]

    return validate_payload(schema_path, example, example_path)


def _resolve_json_pointer_parent(payload: object, pointer: str) -> tuple[object, str]:
    """Resolve a non-root JSON Pointer to its parent and final token."""
    if not pointer.startswith("/"):
        raise ValueError(f"JSON Pointer must start with '/': {pointer!r}")

    tokens = [token.replace("~1", "/").replace("~0", "~") for token in pointer[1:].split("/")]
    current = payload
    for token in tokens[:-1]:
        if isinstance(current, list):
            current = current[int(token)]
        elif isinstance(current, dict):
            current = current[token]
        else:
            raise ValueError(f"JSON Pointer traverses a scalar at {token!r}")

    return current, tokens[-1]


def materialize_invalid_mutation(fixture_path: Path) -> tuple[dict, str]:
    """Apply a closed replace/remove mutation fixture to its valid base record."""
    fixture = load_json(fixture_path)
    required_keys = {
        "fixture_version",
        "base_record",
        "mutations",
        "expected_error_fragment",
    }
    if set(fixture) != required_keys:
        raise ValueError(f"{fixture_path}: mutation fixture fields must equal {required_keys}")
    if fixture["fixture_version"] != "ai_invalid_contract_mutation_v1":
        raise ValueError(f"{fixture_path}: unsupported mutation fixture version")
    if not isinstance(fixture["base_record"], str):
        raise ValueError(f"{fixture_path}: base_record must be a string")
    if not isinstance(fixture["expected_error_fragment"], str):
        raise ValueError(f"{fixture_path}: expected_error_fragment must be a string")
    if not isinstance(fixture["mutations"], list) or not fixture["mutations"]:
        raise ValueError(f"{fixture_path}: mutations must be a non-empty array")

    base_path = (ROOT / fixture["base_record"]).resolve()
    if not base_path.is_relative_to(ROOT):
        raise ValueError(f"{fixture_path}: base_record escapes the repository")
    payload = load_json(base_path)

    try:
        for mutation in fixture["mutations"]:
            if not isinstance(mutation, dict):
                raise ValueError("mutation must be an object")
            operation = mutation.get("op")
            expected_keys = {"op", "path", "value"} if operation == "replace" else {"op", "path"}
            if operation not in {"replace", "remove"} or set(mutation) != expected_keys:
                raise ValueError("mutation must be a closed replace or remove operation")

            parent, token = _resolve_json_pointer_parent(payload, mutation["path"])
            key: str | int = int(token) if isinstance(parent, list) else token
            if operation == "replace":
                parent[key] = mutation["value"]
            else:
                del parent[key]
    except (IndexError, KeyError, TypeError, ValueError) as exc:
        raise ValueError(f"{fixture_path}: invalid mutation: {exc}") from exc

    return payload, fixture["expected_error_fragment"]


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

    for schema_name, corpus_path in CORPUS_CONTRACTS:
        schema_path = SCHEMA_DIR / schema_name
        print(f"Validating {corpus_path.name} against {schema_name}...")
        all_errors.extend(validate_contract(schema_path, corpus_path))

    if args.include_evidence:
        for schema_name, evidence_path in EVIDENCE_CONTRACTS:
            schema_path = SCHEMA_DIR / schema_name

            if not evidence_path.exists():
                print(f"Skipping missing evidence file {evidence_path}")
                continue

            print(f"Validating {evidence_path.name} against {schema_name}...")
            all_errors.extend(validate_contract(schema_path, evidence_path))

    if args.check_failures:
        for schema_name, failure_path, expected_error in FAILURE_CONTRACTS:
            schema_path = SCHEMA_DIR / schema_name

            if not failure_path.exists():
                all_errors.append(f"{failure_path}: required failure sample is missing")
                continue

            print(f"Confirming {failure_path.name} is rejected by {schema_name}...")
            failure_errors = validate_contract(schema_path, failure_path)
            if failure_errors:
                print(f"Rejected as expected: {failure_errors[0]}")
                if expected_error and not any(
                    expected_error in error for error in failure_errors
                ):
                    all_errors.append(
                        f"{failure_path}: rejected, but not for expected reason "
                        f"{expected_error!r}"
                    )
            else:
                all_errors.append(f"{failure_path}: expected validation failure, but file passed")

        for schema_name, fixture_path in MUTATION_FAILURE_CONTRACTS:
            schema_path = SCHEMA_DIR / schema_name
            print(f"Materializing and rejecting {fixture_path.name} with {schema_name}...")
            if not fixture_path.exists():
                all_errors.append(f"{fixture_path}: required mutation fixture is missing")
                continue
            try:
                payload, expected_error = materialize_invalid_mutation(fixture_path)
            except ValueError as exc:
                all_errors.append(str(exc))
                continue

            failure_errors = validate_payload(schema_path, payload, fixture_path)
            if not failure_errors:
                all_errors.append(
                    f"{fixture_path}: expected materialized validation failure, but payload passed"
                )
                continue

            print(f"Rejected as expected: {failure_errors[0]}")
            if not any(expected_error in error for error in failure_errors):
                all_errors.append(
                    f"{fixture_path}: rejected, but not for expected reason "
                    f"{expected_error!r}"
                )

    if all_errors:
        print("\nValidation failed:\n")
        for error in all_errors:
            print(f"- {error}")
        return 1

    print("\nAll contracts are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
