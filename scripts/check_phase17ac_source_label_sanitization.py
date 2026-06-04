#!/usr/bin/env python3
"""Self-check Phase 17AC managed workflow source-label sanitization."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import build_snapshot, load_json, source_label_context  # noqa: E402


PRIVATE_LAKE_REFERENCE = (
    "s3://private-lake-bucket/curated/source=ai_orchestration/"
    "dataset=electricity/date=2026-05-07/"
)


def assert_valid_snapshot(payload: dict) -> None:
    schema = load_json(ROOT / "schemas" / "dashboard_snapshot_v1.schema.json")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(payload), key=lambda error: error.path)
    if errors:
        raise AssertionError(errors[0].message)


def assert_public_label(label: str) -> None:
    if "s3://" in label:
        raise AssertionError(f"private S3 scheme remained in source label: {label}")
    if "private-lake-bucket" in label:
        raise AssertionError(f"private bucket context remained in source label: {label}")
    if "source=ai_orchestration" in label or "dataset=electricity" in label:
        raise AssertionError(f"private dataset path remained in source label: {label}")
    if re.search(r"\b[0-9]{12}\b", label):
        raise AssertionError(f"AWS account id remained in source label: {label}")


def main() -> int:
    energy_input = load_json(ROOT / "docs" / "evidence" / "energy_input_v1.sample.json")
    news_summary = load_json(
        ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json",
    )
    ai_insight = load_json(
        ROOT / "docs" / "evidence" / "phase17p-managed-ai-validated-ai-insight-20260528.json",
    )

    public_context = source_label_context("curated electricity dashboard sample for 2026-05-07")
    if public_context != "curated electricity dashboard sample for 2026-05-07":
        raise AssertionError("public curated source label context was not preserved")

    private_context = source_label_context(PRIVATE_LAKE_REFERENCE)
    if private_context != "curated dashboard evidence for 2026-05-07":
        raise AssertionError(f"private lake source context was not sanitized: {private_context}")

    workflow_ai_insight = json.loads(json.dumps(ai_insight))
    workflow_ai_insight["insights"][0]["energy_references"][0]["reference"] = (
        PRIVATE_LAKE_REFERENCE
    )

    snapshot = build_snapshot(energy_input, news_summary, workflow_ai_insight)
    assert_valid_snapshot(snapshot)
    source = snapshot["insights"][0]["sources"][0]

    if source["url"] != "dashboard-data.json":
        raise AssertionError("private lake source URL did not use dashboard-data.json fallback")
    assert_public_label(source["label"])
    if "curated dashboard evidence for 2026-05-07" not in source["label"]:
        raise AssertionError("sanitized source label did not preserve useful date context")

    print("Phase 17AC source-label sanitization self-check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
