#!/usr/bin/env python3
"""Validate the AI orchestration P2 corpus and evaluation boundary."""

from __future__ import annotations

import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

from validate_contracts import (
    EXAMPLE_DIR,
    SCHEMA_DIR,
    load_json,
    materialize_invalid_mutation,
    validate_contract,
)


ROOT = Path(__file__).resolve().parents[1]
P2_DIR = ROOT / "evaluation" / "ai-orchestration" / "p2"
HOLDOUT_DIR = P2_DIR / "holdout"
INVALID_DIR = P2_DIR / "invalid"
P1_PATH = ROOT / "docs" / "planning" / "ai-orchestration-p1-evaluation-contract-20260901.md"
P1_SHA256 = "85972e3edc6a33deb0d13c79248264ef5f28e3ea2561e8737cd56dedf5bae62c"

MANIFEST_PATH = P2_DIR / "corpus-manifest-v1.json"
EXCLUSIONS_PATH = P2_DIR / "exclusions-v1.json"
EVALUATION_PATH = P2_DIR / "evaluation-set-v1.json"
FIXTURES_PATH = P2_DIR / "policy-fixtures-v1.json"
HOLDOUT_PATH = HOLDOUT_DIR / "holdout-gold-v1.json"
STRUCTURED_EXAMPLE_PATH = EXAMPLE_DIR / "ai_structured_evidence_v1.example.json"
DOCUMENT_EXAMPLE_PATH = EXAMPLE_DIR / "ai_document_evidence_v1.example.json"

PUBLISHER_IDS = {
    "Ofgem": "ofgem",
    "Department for Energy Security and Net Zero": "desnz",
}
FAMILY_PREFIXES = {
    "structured": "ST",
    "document": "DO",
    "combined": "CO",
    "stale": "SA",
    "conflicting": "CF",
    "unsafe_unauthorized": "UN",
    "unanswerable_invalid": "NA",
}
EXPECTED_CASE_IDS = [
    f"{prefix}-{index:02d}"
    for prefix in FAMILY_PREFIXES.values()
    for index in range(1, 5)
]
EXPECTED_SPLITS = {
    1: "calibration",
    2: "development",
    3: "holdout",
    4: "holdout",
}
EXPECTED_BLOCKED_STRUCTURED = {f"SF-{index:02d}" for index in range(1, 8)}
EXPECTED_ACTIVE_DOCUMENTS = {f"DOC-{index:02d}" for index in range(1, 9)}
EXPECTED_ACTIVE_PASSAGES = {f"DP-{index:02d}" for index in range(1, 9)}

SCHEMA_RECORDS = [
    ("ai_structured_evidence_v1.schema.json", STRUCTURED_EXAMPLE_PATH),
    ("ai_document_evidence_v1.schema.json", DOCUMENT_EXAMPLE_PATH),
    ("ai_corpus_manifest_v1.schema.json", MANIFEST_PATH),
    ("ai_corpus_exclusions_v1.schema.json", EXCLUSIONS_PATH),
    ("ai_policy_fixture_v1.schema.json", FIXTURES_PATH),
    ("ai_evaluation_case_v1.schema.json", EVALUATION_PATH),
    ("ai_holdout_gold_v1.schema.json", HOLDOUT_PATH),
]

MARKDOWN_PATHS = [
    P1_PATH,
    ROOT / "docs" / "planning" / "ai-orchestration-p2-corpus-evidence-contract-plan-20260901.md",
    *sorted((ROOT / "docs" / "planning").glob("ai-orchestration-p2-wp*.md")),
    P2_DIR / "coverage-report-v1.md",
]

LOCAL_REFERENCE_FIELDS = {
    "record_locator",
    "selection_record",
    "audit_reference",
    "base_record",
}
REDACTION_PATTERNS = {
    "local user home path": re.compile(r"/Users/[A-Za-z0-9._-]+"),
    "email address": re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    "AWS access key": re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "private key": re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
}


def add_error(errors: list[str], code: str, message: str) -> None:
    errors.append(f"{code}: {message}")


def expect_equal(
    errors: list[str],
    code: str,
    actual: object,
    expected: object,
    label: str,
) -> None:
    if actual != expected:
        add_error(errors, code, f"{label}: expected {expected!r}, found {actual!r}")


def canonical_sha256(payload: dict, omitted_field: str) -> str:
    canonical = {key: value for key, value in payload.items() if key != omitted_field}
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def text_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def parse_instant(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"timestamp lacks an offset: {value!r}")
    return parsed.astimezone(timezone.utc)


def resolve_repo_path(locator: str) -> Path:
    path = (ROOT / locator).resolve()
    if not path.is_relative_to(ROOT):
        raise ValueError(f"repository locator escapes the repository: {locator!r}")
    return path


def resolve_json_pointer(payload: object, pointer: str) -> object:
    if pointer == "":
        return payload
    if not pointer.startswith("/"):
        raise ValueError(f"JSON Pointer must start with '/': {pointer!r}")

    current = payload
    for raw_token in pointer[1:].split("/"):
        token = raw_token.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            current = current[int(token)]
        elif isinstance(current, dict):
            current = current[token]
        else:
            raise ValueError(f"JSON Pointer traverses a scalar at {token!r}")
    return current


def duplicate_values(values: list[str]) -> set[str]:
    return {value for value, count in Counter(values).items() if count > 1}


def iter_named_values(payload: object, field_name: str):
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == field_name:
                yield value
            yield from iter_named_values(value, field_name)
    elif isinstance(payload, list):
        for value in payload:
            yield from iter_named_values(value, field_name)


def load_records() -> dict[Path, dict]:
    paths = [
        MANIFEST_PATH,
        EXCLUSIONS_PATH,
        EVALUATION_PATH,
        FIXTURES_PATH,
        HOLDOUT_PATH,
        STRUCTURED_EXAMPLE_PATH,
        DOCUMENT_EXAMPLE_PATH,
    ]
    return {path: load_json(path) for path in paths}


def check_schema_acceptance(errors: list[str]) -> None:
    for schema_name, record_path in SCHEMA_RECORDS:
        schema_errors = validate_contract(SCHEMA_DIR / schema_name, record_path)
        for schema_error in schema_errors:
            add_error(errors, "SCHEMA_ACCEPTANCE", schema_error)


def check_p1_contract(errors: list[str]) -> None:
    actual = hashlib.sha256(P1_PATH.read_bytes()).hexdigest()
    expect_equal(errors, "P1_CONTRACT_CHANGED", actual, P1_SHA256, "P1 contract SHA-256")


def check_canonical_hashes(records: dict[Path, dict], errors: list[str]) -> None:
    checks = [
        (MANIFEST_PATH, "manifest_hash", "MANIFEST_HASH"),
        (EXCLUSIONS_PATH, "register_hash", "EXCLUSION_REGISTER_HASH"),
        (EVALUATION_PATH, "evaluation_set_hash", "EVALUATION_SET_HASH"),
        (FIXTURES_PATH, "fixture_set_hash", "FIXTURE_SET_HASH"),
        (HOLDOUT_PATH, "gold_set_hash", "HOLDOUT_GOLD_HASH"),
    ]
    for path, hash_field, code in checks:
        payload = records[path]
        expect_equal(
            errors,
            code,
            payload.get(hash_field),
            canonical_sha256(payload, hash_field),
            path.relative_to(ROOT).as_posix(),
        )


def check_manifest_and_pack(records: dict[Path, dict], errors: list[str]) -> dict[str, set[str]]:
    manifest = records[MANIFEST_PATH]
    exclusions = records[EXCLUSIONS_PATH]

    try:
        pack_path = resolve_repo_path(manifest["evidence_pack"]["record_locator"])
        pack = load_json(pack_path)
    except (KeyError, OSError, ValueError) as exc:
        add_error(errors, "MANIFEST_PACK_RESOLUTION", str(exc))
        return {"active": set(), "blocked": set(), "fixtures": set()}

    expect_equal(
        errors,
        "MANIFEST_PACK_IDENTITY",
        (manifest["evidence_pack"]["id"], manifest["evidence_pack"]["version"]),
        (pack.get("evidence_pack_id"), pack.get("pack_version")),
        "manifest evidence-pack identity",
    )
    expect_equal(
        errors,
        "EXCLUSION_PACK_IDENTITY",
        (exclusions["evidence_pack"]["id"], exclusions["evidence_pack"]["version"]),
        (pack.get("evidence_pack_id"), pack.get("pack_version")),
        "exclusion evidence-pack identity",
    )

    structured_pack = pack.get("structured_facts", [])
    document_pack = pack.get("document_passages", [])
    structured_pack_ids = [item.get("evidence_id") for item in structured_pack]
    passage_pack_ids = [item.get("passage_id") for item in document_pack]

    for label, values in (
        ("WP1 structured evidence", structured_pack_ids),
        ("WP1 document passages", passage_pack_ids),
    ):
        duplicates = duplicate_values(values)
        if duplicates:
            add_error(errors, "PACK_ID_UNIQUE", f"{label} duplicates: {sorted(duplicates)}")

    expect_equal(errors, "PACK_SIZE_BOUNDARY", len(structured_pack), 8, "structured count")
    expect_equal(errors, "PACK_SIZE_BOUNDARY", len(document_pack), 8, "document count")

    for item in [*structured_pack, *document_pack]:
        item_id = item.get("evidence_id", item.get("passage_id", "<missing>"))
        if item.get("selection_status") != "selected":
            add_error(errors, "PACK_SELECTION_STATUS", f"{item_id} is not selected")
        coverage = item.get("p1_coverage", [])
        if not coverage or not set(coverage).issubset(EXPECTED_CASE_IDS):
            add_error(errors, "PACK_P1_COVERAGE", f"{item_id} has invalid P1 coverage {coverage!r}")
        if item.get("classification") != "public":
            add_error(errors, "PACK_CLASSIFICATION", f"{item_id} is not public")
        if item.get("access_scope") != "read_only_evaluation":
            add_error(errors, "PACK_ACCESS_SCOPE", f"{item_id} is not read-only evaluation evidence")

    active_structured_ids: list[str] = []
    active_document_ids: list[str] = []
    active_passage_ids: list[str] = []
    active_chunk_ids: list[str] = []
    source_pointers: list[str] = []

    for entry in manifest.get("active_structured_evidence", []):
        entry_id = entry.get("evidence_id", "<missing>")
        active_structured_ids.append(entry_id)
        pointer = entry.get("source_record_pointer", "")
        source_pointers.append(pointer)
        try:
            source = resolve_json_pointer(pack, pointer)
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            add_error(errors, "MANIFEST_POINTER_RESOLUTION", f"{entry_id}: {exc}")
            continue
        if not isinstance(source, dict):
            add_error(errors, "MANIFEST_POINTER_RESOLUTION", f"{entry_id}: pointer did not resolve to an object")
            continue

        comparisons = {
            "evidence_id": source.get("evidence_id"),
            "content_sha256": source.get("source_response_sha256"),
            "classification": source.get("classification"),
            "access_scope": source.get("access_scope"),
            "route case IDs": source.get("p1_coverage"),
        }
        expected = {
            "evidence_id": entry.get("evidence_id"),
            "content_sha256": entry.get("content_sha256"),
            "classification": entry.get("classification"),
            "access_scope": entry.get("access_scope"),
            "route case IDs": entry.get("route_eligibility", {}).get("p1_case_ids"),
        }
        for label, actual in comparisons.items():
            expect_equal(errors, "MANIFEST_STRUCTURED_PIN", actual, expected[label], f"{entry_id} {label}")

        source_template = source.get("precomputed_fact_contract")
        if source_template is None and source.get("query_contract_compatibility_id") is not None:
            source_template = f"query-contract-{source['query_contract_compatibility_id']}"
        expect_equal(
            errors,
            "MANIFEST_STRUCTURED_PIN",
            entry.get("query_template_id"),
            source_template,
            f"{entry_id} query template",
        )

    for entry in manifest.get("active_document_evidence", []):
        document_id = entry.get("document_id", "<missing>")
        passage = entry.get("passage", {})
        passage_id = passage.get("passage_id", "<missing>")
        active_document_ids.append(document_id)
        active_passage_ids.append(passage_id)
        active_chunk_ids.append(passage.get("chunk_id", "<missing>"))
        pointer = entry.get("source_record_pointer", "")
        source_pointers.append(pointer)
        try:
            source = resolve_json_pointer(pack, pointer)
        except (IndexError, KeyError, TypeError, ValueError) as exc:
            add_error(errors, "MANIFEST_POINTER_RESOLUTION", f"{document_id}: {exc}")
            continue
        if not isinstance(source, dict):
            add_error(errors, "MANIFEST_POINTER_RESOLUTION", f"{document_id}: pointer did not resolve to an object")
            continue

        canonical_document = {
            "canonical_url": source.get("canonical_url"),
            "passage": {
                "passage_id": source.get("passage_id"),
                "source_section": source.get("source_section"),
                "text": source.get("passage"),
            },
            "published_date": source.get("published_date"),
            "publisher": source.get("publisher"),
            "title": source.get("title"),
        }
        expected_document_id = f"DOC-{passage_id.removeprefix('DP-')}"
        expected_route = "combined" if source.get("p1_coverage", [""])[0].startswith("CO-") else "document"
        comparisons = [
            ("document ID", document_id, expected_document_id),
            ("document hash", entry.get("document_content_sha256"), text_sha256(json.dumps(canonical_document, ensure_ascii=False, separators=(",", ":"), sort_keys=True))),
            ("passage ID", passage_id, source.get("passage_id")),
            ("passage hash", passage.get("passage_sha256"), source.get("passage_sha256")),
            ("recomputed passage hash", passage.get("passage_sha256"), text_sha256(source.get("passage", ""))),
            ("publisher ID", entry.get("publisher_id"), PUBLISHER_IDS.get(source.get("publisher"))),
            ("classification", entry.get("classification"), source.get("classification")),
            ("access scope", entry.get("access_scope"), source.get("access_scope")),
            ("route", entry.get("route_eligibility", {}).get("route"), expected_route),
            ("route case IDs", entry.get("route_eligibility", {}).get("p1_case_ids"), source.get("p1_coverage")),
        ]
        for label, actual, expected in comparisons:
            expect_equal(errors, "MANIFEST_DOCUMENT_PIN", actual, expected, f"{document_id} {label}")

    unique_groups = [
        ("active structured evidence", active_structured_ids),
        ("active documents", active_document_ids),
        ("active passages", active_passage_ids),
        ("active chunks", active_chunk_ids),
        ("source pointers", source_pointers),
    ]
    for label, values in unique_groups:
        duplicates = duplicate_values(values)
        if duplicates:
            add_error(errors, "MANIFEST_ID_UNIQUE", f"{label} duplicates: {sorted(duplicates)}")

    expect_equal(errors, "MANIFEST_ACTIVE_SET", set(active_structured_ids), {"SF-08"}, "active structured IDs")
    expect_equal(errors, "MANIFEST_ACTIVE_SET", set(active_document_ids), EXPECTED_ACTIVE_DOCUMENTS, "active document IDs")
    expect_equal(errors, "MANIFEST_ACTIVE_SET", set(active_passage_ids), EXPECTED_ACTIVE_PASSAGES, "active passage IDs")
    expect_equal(
        errors,
        "MANIFEST_ACTIVE_SET",
        set(manifest.get("approved_passage_ids", [])),
        EXPECTED_ACTIVE_PASSAGES,
        "approved passage IDs",
    )

    entries = exclusions.get("entries", [])
    exclusion_ids = [entry.get("exclusion_id") for entry in entries]
    excluded_target_ids = [target for entry in entries for target in entry.get("target_ids", [])]
    for label, values in (("exclusion IDs", exclusion_ids), ("excluded targets", excluded_target_ids)):
        duplicates = duplicate_values(values)
        if duplicates:
            add_error(errors, "EXCLUSION_ID_UNIQUE", f"{label} duplicates: {sorted(duplicates)}")
    for entry in entries:
        if entry.get("answer_evidence_eligible") is not False:
            add_error(errors, "EXCLUSION_INELIGIBLE", f"{entry.get('exclusion_id')} is answer eligible")

    blocked_entries = [entry for entry in entries if entry.get("selection_status") == "contract_blocked"]
    blocked_ids = {target for entry in blocked_entries for target in entry.get("target_ids", [])}
    expect_equal(errors, "BLOCKED_STRUCTURED_BOUNDARY", blocked_ids, EXPECTED_BLOCKED_STRUCTURED, "blocked structured IDs")
    if EXPECTED_BLOCKED_STRUCTURED.intersection(active_structured_ids):
        add_error(errors, "BLOCKED_STRUCTURED_LEAK", "contract-blocked structured evidence is active")
    expect_equal(
        errors,
        "BLOCKED_STRUCTURED_BOUNDARY",
        set(structured_pack_ids),
        set(active_structured_ids) | blocked_ids,
        "selected structured accounting",
    )
    expect_equal(
        errors,
        "MANIFEST_DOCUMENT_ACCOUNTING",
        set(passage_pack_ids),
        set(active_passage_ids),
        "selected document-passage accounting",
    )

    manifest_register = manifest.get("exclusion_register", {})
    expect_equal(
        errors,
        "EXCLUSION_REFERENCE",
        (manifest_register.get("id"), manifest_register.get("version"), manifest_register.get("register_hash")),
        (exclusions.get("exclusion_register_id"), exclusions.get("register_version"), exclusions.get("register_hash")),
        "manifest exclusion-register reference",
    )
    try:
        expect_equal(
            errors,
            "EXCLUSION_REFERENCE",
            resolve_repo_path(manifest_register.get("record_locator", "")),
            EXCLUSIONS_PATH.resolve(),
            "manifest exclusion-register locator",
        )
    except ValueError as exc:
        add_error(errors, "EXCLUSION_REFERENCE", str(exc))

    query_templates = manifest.get("selected_query_templates", [])
    query_ids = [item.get("template_id") for item in query_templates]
    duplicates = duplicate_values(query_ids)
    if duplicates:
        add_error(errors, "QUERY_TEMPLATE_ID_UNIQUE", f"query-template duplicates: {sorted(duplicates)}")
    if any(item.get("athena_query_executed") is not False for item in query_templates):
        add_error(errors, "QUERY_EXECUTION_BOUNDARY", "a selected query template claims Athena execution")

    summary = manifest.get("inventory_summary", {})
    expected_counts = {
        "selected_pack_structured_count": len(structured_pack),
        "selected_pack_document_count": len(document_pack),
        "selected_pack_passage_count": len(document_pack),
        "active_structured_count": len(active_structured_ids),
        "active_document_count": len(active_document_ids),
        "active_passage_count": len(active_passage_ids),
        "contract_blocked_selected_count": len(blocked_ids),
        "exclusion_entry_count": len(entries),
        "tombstone_count": len(manifest.get("tombstones", [])),
        "derived_projection_count": manifest.get("derived_projection_boundary", {}).get("actual_count"),
    }
    for field, expected in expected_counts.items():
        expect_equal(errors, "MANIFEST_INVENTORY_COUNT", summary.get(field), expected, field)

    coverage = manifest.get("evaluation_coverage", {})
    expect_equal(errors, "MANIFEST_BLOCKED_COVERAGE", set(coverage.get("blocked_evidence_ids", [])), blocked_ids, "blocked evidence coverage")
    blocked_case_ids = {
        case_id
        for item in structured_pack
        if item.get("evidence_id") in blocked_ids
        for case_id in item.get("p1_coverage", [])
    }
    expect_equal(errors, "MANIFEST_BLOCKED_COVERAGE", set(coverage.get("blocked_required_case_ids", [])), blocked_case_ids, "blocked case coverage")

    try:
        started = parse_instant(manifest["status"]["candidate_started_at"])
        validated = parse_instant(manifest["status"]["validation_completed_at"])
        activated = parse_instant(manifest["status"]["activated_at"])
        if not started <= validated <= activated:
            add_error(errors, "MANIFEST_TIMESTAMP_ORDER", "candidate, validation and activation times are out of order")
    except (KeyError, TypeError, ValueError) as exc:
        add_error(errors, "MANIFEST_TIMESTAMP_ORDER", str(exc))

    fallback = manifest.get("fallback_policy", {})
    prior = manifest.get("prior_complete_manifest", {})
    if prior.get("status") == "none":
        if fallback.get("fallback_available") is not False:
            add_error(errors, "MANIFEST_FALLBACK_BOUNDARY", "fallback is available without a prior complete manifest")
    if fallback.get("revocation_bypass") != "prohibited" or fallback.get("cross_manifest_assembly") != "prohibited":
        add_error(errors, "MANIFEST_FALLBACK_BOUNDARY", "fallback can bypass revocation or assemble across manifests")

    active_ids = set(active_structured_ids) | set(active_document_ids) | set(active_passage_ids)
    if active_ids.intersection(excluded_target_ids):
        add_error(errors, "EXCLUDED_EVIDENCE_ACTIVE", f"excluded targets are active: {sorted(active_ids.intersection(excluded_target_ids))}")

    return {"active": active_ids, "blocked": blocked_ids, "fixtures": set()}


def check_contract_examples(records: dict[Path, dict], errors: list[str]) -> None:
    manifest = records[MANIFEST_PATH]
    structured = records[STRUCTURED_EXAMPLE_PATH]
    document = records[DOCUMENT_EXAMPLE_PATH]
    pack = load_json(resolve_repo_path(manifest["evidence_pack"]["record_locator"]))
    sf08 = next(item for item in pack["structured_facts"] if item["evidence_id"] == "SF-08")
    dp08 = next(item for item in pack["document_passages"] if item["passage_id"] == "DP-08")
    manifest_sf08 = next(item for item in manifest["active_structured_evidence"] if item["evidence_id"] == "SF-08")
    manifest_doc08 = next(item for item in manifest["active_document_evidence"] if item["document_id"] == "DOC-08")

    structured_checks = [
        ("evidence ID", structured.get("evidence_id"), sf08.get("evidence_id")),
        ("pack ID", structured.get("evidence_pack", {}).get("id"), pack.get("evidence_pack_id")),
        ("value", structured.get("observation", {}).get("value"), sf08.get("value")),
        ("metric", structured.get("metric", {}).get("name"), sf08.get("metric")),
        ("unit", structured.get("unit"), sf08.get("unit")),
        ("effective time", structured.get("effective_time", {}).get("effective_at"), sf08.get("effective_at")),
        ("dataset", structured.get("source", {}).get("dataset_id"), sf08.get("source_dataset_id")),
        ("source URL", structured.get("source", {}).get("canonical_url"), sf08.get("source_url")),
        ("source hash", structured.get("source", {}).get("source_content_sha256"), sf08.get("source_response_sha256")),
        ("source count", structured.get("source", {}).get("source_record_count"), sf08.get("source_rows")),
        ("manifest hash", manifest_sf08.get("content_sha256"), sf08.get("source_response_sha256")),
        ("route", structured.get("route_eligibility"), manifest_sf08.get("route_eligibility")),
    ]
    for label, actual, expected in structured_checks:
        expect_equal(errors, "STRUCTURED_EXAMPLE_RECONCILIATION", actual, expected, label)

    source_hash = structured.get("source", {}).get("source_content_sha256")
    expect_equal(errors, "STRUCTURED_PROVENANCE", structured.get("internal_provenance", {}).get("source_response_sha256"), source_hash, "internal source hash")
    expect_equal(errors, "STRUCTURED_PROVENANCE", structured.get("public_citation", {}).get("canonical_url"), structured.get("source", {}).get("canonical_url"), "public source URL")
    expect_equal(errors, "STRUCTURED_PROVENANCE", structured.get("public_citation", {}).get("value_label"), structured.get("observation", {}).get("value"), "public value label")
    expect_equal(
        errors,
        "STRUCTURED_CITATION_LABEL",
        structured.get("public_citation", {}).get("citation_label"),
        "Elexon BMRS INDO dataset metadata, last updated 5 September 2026 at 08:00 UTC",
        "structured citation label",
    )

    try:
        acquired = parse_instant(structured["processing"]["acquired_at"])
        generated = parse_instant(structured["processing"]["generated_at"])
        validated = parse_instant(structured["processing"]["validated_at"])
        controlling = parse_instant(structured["freshness_assessment"]["controlling_time"])
        as_of = parse_instant(structured["freshness_assessment"]["as_of"])
        if not acquired <= generated <= validated:
            add_error(errors, "STRUCTURED_TIMESTAMP_ORDER", "acquisition, generation and validation times are out of order")
        age_seconds = int((as_of - controlling).total_seconds())
        expect_equal(errors, "STRUCTURED_FRESHNESS", structured["freshness_assessment"]["age_seconds"], age_seconds, "structured age_seconds")
        if age_seconds > structured["freshness_assessment"]["maximum_age_hours"] * 3600:
            add_error(errors, "STRUCTURED_FRESHNESS", "structured example is stale at its pinned as_of")
    except (KeyError, TypeError, ValueError) as exc:
        add_error(errors, "STRUCTURED_TIMESTAMP_ORDER", str(exc))

    canonical_document = {
        "canonical_url": dp08["canonical_url"],
        "passage": {
            "passage_id": dp08["passage_id"],
            "source_section": dp08["source_section"],
            "text": dp08["passage"],
        },
        "published_date": dp08["published_date"],
        "publisher": dp08["publisher"],
        "title": dp08["title"],
    }
    document_hash = text_sha256(json.dumps(canonical_document, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    passage_hash = text_sha256(dp08["passage"])
    document_checks = [
        ("document ID", document.get("document_id"), "DOC-08"),
        ("document hash", document.get("document_content_sha256"), document_hash),
        ("manifest document hash", manifest_doc08.get("document_content_sha256"), document_hash),
        ("passage ID", document.get("content", {}).get("passage_id"), dp08.get("passage_id")),
        ("passage text", document.get("content", {}).get("text"), dp08.get("passage")),
        ("passage hash", document.get("content", {}).get("passage_sha256"), passage_hash),
        ("manifest passage hash", manifest_doc08.get("passage", {}).get("passage_sha256"), passage_hash),
        ("publisher", document.get("source", {}).get("publisher_label"), dp08.get("publisher")),
        ("title", document.get("source", {}).get("title"), dp08.get("title")),
        ("URL", document.get("source", {}).get("canonical_url"), dp08.get("canonical_url")),
        ("publication date", document.get("source", {}).get("publication_time", {}).get("value"), dp08.get("published_date")),
        ("source section", document.get("internal_provenance", {}).get("source_section"), dp08.get("source_section")),
        ("route", document.get("route_eligibility"), manifest_doc08.get("route_eligibility")),
    ]
    for label, actual, expected in document_checks:
        expect_equal(errors, "DOCUMENT_EXAMPLE_RECONCILIATION", actual, expected, label)

    expect_equal(errors, "DOCUMENT_PROVENANCE", document.get("internal_provenance", {}).get("document_content_sha256"), document_hash, "internal document hash")
    expect_equal(errors, "DOCUMENT_PROVENANCE", document.get("internal_provenance", {}).get("passage_sha256"), passage_hash, "internal passage hash")
    citation = document.get("public_citation", {})
    citation_checks = [
        ("publisher", citation.get("publisher"), dp08.get("publisher")),
        ("title", citation.get("title"), dp08.get("title")),
        ("publication date", citation.get("publication_date"), dp08.get("published_date")),
        ("section", citation.get("section_locator"), dp08.get("source_section")),
        ("URL", citation.get("canonical_url"), dp08.get("canonical_url")),
    ]
    for label, actual, expected in citation_checks:
        expect_equal(errors, "DOCUMENT_PUBLIC_CITATION", actual, expected, label)
    published_date = datetime.fromisoformat(dp08["published_date"])
    expected_citation_label = (
        f"{dp08['publisher']}, {dp08['title']}, published "
        f"{published_date.day} {published_date.strftime('%B %Y')}"
    )
    expect_equal(
        errors,
        "DOCUMENT_CITATION_LABEL",
        citation.get("citation_label"),
        expected_citation_label,
        "document citation label",
    )
    if any(field in citation for field in ("internal_provenance", "selection_record", "document_content_sha256", "passage_sha256")):
        add_error(errors, "DOCUMENT_PUBLIC_CITATION", "public citation contains an internal field")

    coordinates = document.get("content", {}).get("coordinates", {})
    if coordinates.get("kind") == "section":
        expect_equal(errors, "DOCUMENT_COORDINATES", coordinates.get("section_heading"), dp08.get("source_section"), "section heading")
        if coordinates.get("section_heading_occurrence", 0) < 1 or coordinates.get("passage_ordinal_in_section", 0) < 1:
            add_error(errors, "DOCUMENT_COORDINATES", "section coordinates are not positive")
    else:
        add_error(errors, "DOCUMENT_COORDINATES", "the v1 bounded example must use its selected source section")

    try:
        ingested = parse_instant(document["processing"]["ingested_at"])
        validated = parse_instant(document["processing"]["validated_at"])
        if ingested > validated:
            add_error(errors, "DOCUMENT_TIMESTAMP_ORDER", "ingestion occurs after validation")
    except (KeyError, TypeError, ValueError) as exc:
        add_error(errors, "DOCUMENT_TIMESTAMP_ORDER", str(exc))


def combined_gold_by_case(evaluation: dict, holdout: dict) -> dict[str, dict]:
    result = {
        case["case_id"]: case["gold"]
        for case in evaluation.get("cases", [])
        if case.get("split") != "holdout" and isinstance(case.get("gold"), dict)
    }
    for record in holdout.get("records", []):
        result[record["case_id"]] = record
    return result


def check_evaluation(records: dict[Path, dict], evidence_sets: dict[str, set[str]], errors: list[str]) -> None:
    manifest = records[MANIFEST_PATH]
    evaluation = records[EVALUATION_PATH]
    fixtures_record = records[FIXTURES_PATH]
    holdout = records[HOLDOUT_PATH]
    cases = evaluation.get("cases", [])
    fixtures = fixtures_record.get("fixtures", [])
    gold_records = holdout.get("records", [])

    cross_hash_checks = [
        ("evaluation manifest", evaluation.get("manifest_reference", {}).get("manifest_hash"), manifest.get("manifest_hash")),
        ("evaluation fixtures", evaluation.get("policy_fixture_register", {}).get("fixture_set_hash"), fixtures_record.get("fixture_set_hash")),
        ("evaluation holdout", evaluation.get("holdout_gold_boundary", {}).get("gold_set_hash"), holdout.get("gold_set_hash")),
    ]
    for label, actual, expected in cross_hash_checks:
        expect_equal(errors, "EVALUATION_REFERENCE_HASH", actual, expected, label)

    reference_paths = [
        (evaluation.get("manifest_reference", {}).get("record_locator", ""), MANIFEST_PATH),
        (evaluation.get("policy_fixture_register", {}).get("record_locator", ""), FIXTURES_PATH),
        (evaluation.get("holdout_gold_boundary", {}).get("record_locator", ""), HOLDOUT_PATH),
    ]
    for locator, expected_path in reference_paths:
        try:
            expect_equal(errors, "EVALUATION_REFERENCE_PATH", resolve_repo_path(locator), expected_path.resolve(), locator)
        except ValueError as exc:
            add_error(errors, "EVALUATION_REFERENCE_PATH", str(exc))

    case_ids = [case.get("case_id") for case in cases]
    duplicates = duplicate_values(case_ids)
    if duplicates:
        add_error(errors, "EVAL_CASE_ID_UNIQUE", f"duplicate case IDs: {sorted(duplicates)}")
    expect_equal(errors, "EVAL_CASE_SET", case_ids, EXPECTED_CASE_IDS, "ordered P1 case IDs")
    expect_equal(errors, "EVAL_CASE_COUNT", len(cases), 28, "evaluation case count")

    split_counts = Counter(case.get("split") for case in cases)
    expect_equal(
        errors,
        "EVAL_SPLIT_COUNTS",
        split_counts,
        Counter({"calibration": 7, "development": 7, "holdout": 14}),
        "global split counts",
    )
    family_groups: dict[str, list[dict]] = defaultdict(list)
    for case in cases:
        family_groups[case.get("family")].append(case)
    expect_equal(errors, "EVAL_FAMILY_SET", set(family_groups), set(FAMILY_PREFIXES), "evaluation families")
    for family, prefix in FAMILY_PREFIXES.items():
        family_cases = family_groups.get(family, [])
        expect_equal(errors, "EVAL_FAMILY_COUNT", len(family_cases), 4, f"{family} case count")
        for case in family_cases:
            try:
                ordinal = int(case["case_id"].split("-")[1])
            except (IndexError, TypeError, ValueError):
                add_error(errors, "EVAL_CASE_ID_FORMAT", f"invalid case ID {case.get('case_id')!r}")
                continue
            expect_equal(errors, "EVAL_FAMILY_PREFIX", case["case_id"].split("-")[0], prefix, case["case_id"])
            expect_equal(errors, "EVAL_FAMILY_SPLIT", case.get("split"), EXPECTED_SPLITS.get(ordinal), case["case_id"])

    fixture_ids = [fixture.get("fixture_id") for fixture in fixtures]
    fixture_case_ids = [fixture.get("case_id") for fixture in fixtures]
    for label, values in (("fixture IDs", fixture_ids), ("fixture case IDs", fixture_case_ids)):
        duplicates = duplicate_values(values)
        if duplicates:
            add_error(errors, "FIXTURE_CASE_MAPPING", f"duplicate {label}: {sorted(duplicates)}")
    expected_fixture_cases = {
        case_id for case_id in EXPECTED_CASE_IDS if case_id.startswith(("SA-", "CF-", "UN-", "NA-"))
    }
    expect_equal(errors, "FIXTURE_CASE_MAPPING", set(fixture_case_ids), expected_fixture_cases, "fixture case set")
    expect_equal(errors, "FIXTURE_COUNT", len(fixtures), 16, "policy fixture count")

    fixture_boundary = fixtures_record.get("boundary", {})
    false_fields = [
        "observed_production_evidence",
        "answer_evidence_eligible",
        "ordinary_retrieval_eligible",
        "candidate_tuning_eligible",
    ]
    if fixture_boundary.get("synthetic") is not True:
        add_error(errors, "FIXTURE_INELIGIBLE", "fixture-set boundary is not synthetic")
    for field in false_fields:
        if fixture_boundary.get(field) is not False:
            add_error(errors, "FIXTURE_INELIGIBLE", f"fixture-set boundary {field} is not false")
    for fixture in fixtures:
        fixture_id = fixture.get("fixture_id", "<missing>")
        if fixture.get("synthetic") is not True:
            add_error(errors, "FIXTURE_INELIGIBLE", f"{fixture_id} is not synthetic")
        for field in false_fields[1:]:
            if fixture.get(field) is not False:
                add_error(errors, "FIXTURE_INELIGIBLE", f"{fixture_id} {field} is not false")
        if fixture.get("selection_status") != "adversarial_only" or fixture.get("lifecycle_status") != "adversarial_only":
            add_error(errors, "FIXTURE_INELIGIBLE", f"{fixture_id} is not adversarial-only")
        matching_cases = [case for case in cases if case.get("case_id") == fixture.get("case_id")]
        if len(matching_cases) != 1:
            add_error(errors, "FIXTURE_CASE_MAPPING", f"{fixture_id} resolves to {len(matching_cases)} cases")
        elif fixture.get("as_of") != matching_cases[0].get("as_of"):
            add_error(errors, "FIXTURE_AS_OF", f"{fixture_id} as_of differs from {fixture.get('case_id')}")

    evidence_sets["fixtures"] = set(fixture_ids)
    active_ids = evidence_sets["active"]
    blocked_ids = evidence_sets["blocked"]
    if active_ids.intersection(fixture_ids) or blocked_ids.intersection(fixture_ids):
        add_error(errors, "FIXTURE_CORPUS_LEAK", "policy fixtures overlap corpus evidence IDs")

    holdout_case_ids = [case.get("case_id") for case in cases if case.get("split") == "holdout"]
    non_holdout_cases = [case for case in cases if case.get("split") != "holdout"]
    gold_case_ids = [record.get("case_id") for record in gold_records]
    gold_ids = [record.get("gold_id") for record in gold_records]
    if duplicate_values(gold_case_ids) or duplicate_values(gold_ids):
        add_error(errors, "HOLDOUT_CASE_MAPPING", "holdout gold contains duplicate case or gold IDs")
    expect_equal(errors, "HOLDOUT_CASE_MAPPING", set(gold_case_ids), set(holdout_case_ids), "holdout gold case set")
    expect_equal(errors, "HOLDOUT_COUNT", len(gold_records), 14, "holdout gold count")

    access_boundary = holdout.get("access_boundary", {})
    evaluation_boundary = evaluation.get("holdout_gold_boundary", {})
    for boundary_name, boundary in (("holdout file", access_boundary), ("evaluation reference", evaluation_boundary)):
        for field in ("candidate_access", "prompt_input_eligible", "retrieval_input_eligible", "tuning_input_eligible"):
            if boundary.get(field) is not False:
                add_error(errors, "HOLDOUT_ACCESS_BOUNDARY", f"{boundary_name} {field} is not false")

    holdout_by_case = {record["case_id"]: record for record in gold_records}
    for case in cases:
        case_id = case.get("case_id", "<missing>")
        if case.get("candidate_visible") is not True:
            add_error(errors, "EVAL_CANDIDATE_VISIBILITY", f"{case_id} prompt is not candidate visible")
        if case.get("split") == "holdout":
            if "gold" in case:
                add_error(errors, "HOLDOUT_GOLD_LEAK", f"{case_id} contains inline gold")
            boundary = case.get("gold_boundary", {})
            gold = holdout_by_case.get(case_id)
            if gold is None:
                add_error(errors, "HOLDOUT_CASE_MAPPING", f"{case_id} has no holdout gold")
            else:
                expect_equal(errors, "HOLDOUT_CASE_MAPPING", boundary.get("gold_id"), gold.get("gold_id"), f"{case_id} gold ID")
            if boundary.get("candidate_visible") is not False:
                add_error(errors, "HOLDOUT_GOLD_LEAK", f"{case_id} holdout gold is candidate visible")
        else:
            if "gold_boundary" in case:
                add_error(errors, "NON_HOLDOUT_GOLD_BOUNDARY", f"{case_id} has a holdout boundary")
            if not isinstance(case.get("gold"), dict):
                add_error(errors, "NON_HOLDOUT_GOLD_MISSING", f"{case_id} has no inline gold")

    gold_by_case = combined_gold_by_case(evaluation, holdout)
    scoring_rule = evaluation.get("policy_versions", {}).get("scoring_rule_version")
    for case in cases:
        case_id = case.get("case_id", "<missing>")
        scope = case.get("candidate_input_scope", {})
        gold = gold_by_case.get(case_id)
        if gold is None:
            add_error(errors, "CASE_GOLD_RESOLUTION", f"{case_id} has no gold record")
            continue

        expect_equal(errors, "CASE_GOLD_RESOLUTION", gold.get("case_id"), case_id, f"{case_id} gold case ID")
        expect_equal(errors, "CASE_SCORING_RULE", gold.get("scoring_rule_version"), scoring_rule, case_id)
        if gold.get("primary_outcome_code") not in gold.get("allowed_outcome_codes", []):
            add_error(errors, "CASE_PRIMARY_OUTCOME", f"{case_id} primary outcome is not allowed")

        approved = set(scope.get("approved_evidence_ids", []))
        blocked = set(scope.get("blocked_reference_ids", []))
        required_fixtures = set(scope.get("policy_fixture_ids", []))
        expect_equal(errors, "CASE_GOLD_EVIDENCE_MAPPING", approved, set(gold.get("required_available_evidence_ids", [])), f"{case_id} available evidence")
        expect_equal(errors, "CASE_GOLD_EVIDENCE_MAPPING", blocked, set(gold.get("required_unavailable_evidence_ids", [])), f"{case_id} unavailable evidence")
        expect_equal(errors, "CASE_GOLD_FIXTURE_MAPPING", required_fixtures, set(gold.get("required_fixture_ids", [])), f"{case_id} fixture IDs")

        unresolved_approved = approved - active_ids
        if unresolved_approved:
            add_error(errors, "CASE_APPROVED_EVIDENCE_RESOLUTION", f"{case_id} approved IDs are not active: {sorted(unresolved_approved)}")
        unresolved_blocked = blocked - blocked_ids
        if unresolved_blocked:
            add_error(errors, "CASE_BLOCKED_EVIDENCE_RESOLUTION", f"{case_id} blocked IDs are not contract-blocked: {sorted(unresolved_blocked)}")
        unresolved_fixtures = required_fixtures - set(fixture_ids)
        if unresolved_fixtures:
            add_error(errors, "CASE_FIXTURE_RESOLUTION", f"{case_id} fixtures do not exist: {sorted(unresolved_fixtures)}")

        expected_status = "policy_fixture" if required_fixtures else "contract_blocked" if blocked else "ready"
        expect_equal(errors, "CASE_RESOLUTION_STATUS", case.get("evidence_resolution_status"), expected_status, case_id)
        if required_fixtures and approved:
            add_error(errors, "CASE_ADVERSARIAL_ISOLATION", f"{case_id} mixes active evidence with adversarial fixtures")

        scope_manifest = (
            scope.get("manifest_id"),
            scope.get("manifest_version"),
            scope.get("manifest_hash"),
        )
        expected_manifest = (
            manifest.get("manifest_id"),
            manifest.get("manifest_version"),
            manifest.get("manifest_hash"),
        )
        expect_equal(errors, "CASE_MANIFEST_IDENTITY", scope_manifest, expected_manifest, case_id)

        try:
            as_of = parse_instant(case["as_of"])
        except (KeyError, TypeError, ValueError) as exc:
            add_error(errors, "CASE_AS_OF", f"{case_id}: {exc}")
            continue
        for evidence_id in approved:
            if evidence_id == "SF-08":
                structured = records[STRUCTURED_EXAMPLE_PATH]
                controlling = parse_instant(structured["freshness_assessment"]["controlling_time"])
                age_seconds = (as_of - controlling).total_seconds()
                if age_seconds < 0 or age_seconds > 36 * 3600:
                    add_error(errors, "CASE_FRESHNESS", f"{case_id} uses SF-08 outside its current window")
            elif evidence_id.startswith(("DOC-", "DP-")):
                suffix = evidence_id.split("-")[1]
                passage_id = f"DP-{suffix}"
                pack = load_json(resolve_repo_path(manifest["evidence_pack"]["record_locator"]))
                source = next(item for item in pack["document_passages"] if item["passage_id"] == passage_id)
                published = datetime.fromisoformat(source["published_date"]).replace(tzinfo=timezone.utc)
                if published > as_of:
                    add_error(errors, "CASE_FRESHNESS", f"{case_id} uses {evidence_id} before publication")

    # Recompute the two explicit threshold examples and assert the mixed and
    # incomplete-manifest cases keep their deterministic policy outcomes.
    fixture_by_id = {fixture["fixture_id"]: fixture for fixture in fixtures}
    threshold_cases = {
        "FIX-SA-01": ("structured-current-36h", 36, "stale"),
        "FIX-SA-02": ("document-current-168h", 168, "stale"),
    }
    timestamp_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")
    for fixture_id, (rule_id, threshold_hours, expected_freshness) in threshold_cases.items():
        fixture = fixture_by_id.get(fixture_id, {})
        conditions = " ".join(fixture.get("conditions", []))
        match = timestamp_pattern.search(conditions)
        if match is None:
            add_error(errors, "FIXTURE_FRESHNESS", f"{fixture_id} lacks a controlling timestamp")
            continue
        controlling = parse_instant(match.group(0))
        as_of = parse_instant(fixture["as_of"])
        age_hours = (as_of - controlling).total_seconds() / 3600
        if rule_id not in fixture.get("tested_rule_ids", []):
            add_error(errors, "FIXTURE_FRESHNESS", f"{fixture_id} does not name {rule_id}")
        if age_hours <= threshold_hours:
            add_error(errors, "FIXTURE_FRESHNESS", f"{fixture_id} age {age_hours:g}h does not exceed {threshold_hours}h")
        case_gold = gold_by_case.get(fixture.get("case_id"), {})
        expect_equal(errors, "FIXTURE_FRESHNESS", case_gold.get("assertions", {}).get("freshness"), expected_freshness, fixture_id)

    sa03 = gold_by_case.get("SA-03", {})
    expect_equal(errors, "FIXTURE_FRESHNESS", sa03.get("assertions", {}).get("freshness"), "mixed_freshness", "SA-03")
    sa04 = gold_by_case.get("SA-04", {})
    expect_equal(errors, "FIXTURE_FRESHNESS", sa04.get("assertions", {}).get("freshness"), "manifest_incomplete", "SA-04")
    if manifest.get("fallback_policy", {}).get("cross_manifest_assembly") != "prohibited":
        add_error(errors, "FIXTURE_FRESHNESS", "SA-04 cannot prove fail-safe behaviour while cross-manifest assembly is allowed")


def check_local_references(records: dict[Path, dict], errors: list[str]) -> None:
    for record_path, payload in records.items():
        for field in LOCAL_REFERENCE_FIELDS:
            for value in iter_named_values(payload, field):
                if not isinstance(value, str):
                    add_error(errors, "LOCAL_REFERENCE_TYPE", f"{record_path.name} {field} is not a string")
                    continue
                try:
                    resolved = resolve_repo_path(value)
                except ValueError as exc:
                    add_error(errors, "LOCAL_REFERENCE_PATH", f"{record_path.name}: {exc}")
                    continue
                if not resolved.exists():
                    add_error(errors, "LOCAL_REFERENCE_MISSING", f"{record_path.name}: {value}")

    markdown_link = re.compile(r"(?<!!)\[[^\]]+\]\(([^)]+)\)")
    for markdown_path in dict.fromkeys(MARKDOWN_PATHS):
        if not markdown_path.exists():
            add_error(errors, "MARKDOWN_FILE_MISSING", markdown_path.relative_to(ROOT).as_posix())
            continue
        text = markdown_path.read_text(encoding="utf-8")
        for raw_target in markdown_link.findall(text):
            target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
            parsed = urlparse(target)
            if parsed.scheme or target.startswith("#"):
                continue
            local_target = unquote(target.split("#", 1)[0])
            resolved = (markdown_path.parent / local_target).resolve()
            if not resolved.is_relative_to(ROOT) or not resolved.exists():
                add_error(errors, "MARKDOWN_LINK", f"{markdown_path.relative_to(ROOT)} -> {target}")


def check_redaction(records: dict[Path, dict], errors: list[str]) -> None:
    paths = [*records.keys(), *MARKDOWN_PATHS, *sorted(INVALID_DIR.glob("*.json"))]
    for path in dict.fromkeys(paths):
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        for label, pattern in REDACTION_PATTERNS.items():
            if pattern.search(text):
                add_error(errors, "REDACTION_BOUNDARY", f"{path.relative_to(ROOT)} contains {label}")


def semantic_errors(records: dict[Path, dict], *, include_static_checks: bool = True) -> list[str]:
    errors: list[str] = []
    check_canonical_hashes(records, errors)
    evidence_sets = check_manifest_and_pack(records, errors)
    check_contract_examples(records, errors)
    check_evaluation(records, evidence_sets, errors)
    check_local_references(records, errors)
    check_redaction(records, errors)
    if include_static_checks:
        check_p1_contract(errors)
    return errors


def check_semantic_mutations(base_records: dict[Path, dict], errors: list[str]) -> None:
    mutation_paths = sorted(INVALID_DIR.glob("*.invalid.json"))
    if not mutation_paths:
        add_error(errors, "SEMANTIC_MUTATION_MISSING", "no WP8 semantic mutation fixtures were found")
        return

    supported = {
        path.resolve(): path
        for path in (
            MANIFEST_PATH,
            EXCLUSIONS_PATH,
            EVALUATION_PATH,
            FIXTURES_PATH,
            HOLDOUT_PATH,
            STRUCTURED_EXAMPLE_PATH,
            DOCUMENT_EXAMPLE_PATH,
        )
    }
    for fixture_path in mutation_paths:
        try:
            descriptor = load_json(fixture_path)
            expected_code = descriptor["expected_error_fragment"]
            base_path = resolve_repo_path(descriptor["base_record"])
            target_path = supported.get(base_path)
            if target_path is None:
                raise ValueError(f"unsupported semantic mutation base: {descriptor['base_record']}")
            mutated_payload, materialized_expected = materialize_invalid_mutation(fixture_path)
            if materialized_expected != expected_code:
                raise ValueError("materialized expected code changed")
        except (KeyError, OSError, TypeError, ValueError) as exc:
            add_error(errors, "SEMANTIC_MUTATION_INVALID", f"{fixture_path.name}: {exc}")
            continue

        mutated_records = dict(base_records)
        mutated_records[target_path] = mutated_payload
        found = semantic_errors(mutated_records, include_static_checks=False)
        if not any(error.startswith(f"{expected_code}:") for error in found):
            add_error(
                errors,
                "SEMANTIC_MUTATION_WRONG_REASON",
                f"{fixture_path.name}: expected {expected_code}, found {found[:3]!r}",
            )
        else:
            print(f"Rejected {fixture_path.name} as expected: {expected_code}")


def main() -> int:
    all_errors: list[str] = []
    try:
        records = load_records()
    except (OSError, ValueError) as exc:
        print(f"P2 validation could not load its records: {exc}", file=sys.stderr)
        return 1

    print("Validating P2 schemas...")
    check_schema_acceptance(all_errors)
    print("Validating P2 semantic and cross-file invariants...")
    all_errors.extend(semantic_errors(records))
    print("Validating reason-checked semantic mutations...")
    check_semantic_mutations(records, all_errors)

    if all_errors:
        print("\nP2 validation failed:\n")
        for error in all_errors:
            print(f"- {error}")
        return 1

    print("\nP2 validation passed: schemas, hashes, identities, evidence resolution, "
          "holdout isolation, policy fixtures, local references and redaction are coherent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
