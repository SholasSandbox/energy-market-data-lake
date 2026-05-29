#!/usr/bin/env python3
"""Self-check Phase 17R dashboard source-link hardening."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.news_ai import build_snapshot, is_public_source_url, load_json  # noqa: E402


def assert_valid_snapshot(payload: dict) -> None:
    schema = load_json(ROOT / "schemas" / "dashboard_snapshot_v1.schema.json")
    validator = Draft202012Validator(schema, format_checker=FormatChecker())
    errors = sorted(validator.iter_errors(payload), key=lambda error: error.path)
    if errors:
        raise AssertionError(errors[0].message)


def assert_public_source_urls(payload: dict) -> None:
    sources = payload["insights"][0]["sources"]
    for source in sources:
        url = source["url"]
        if not is_public_source_url(url):
            raise AssertionError(f"unsafe dashboard source URL remained: {url}")
        if url.startswith(("s3://", "local://")):
            raise AssertionError(f"private dashboard source URL remained: {url}")
        if " " in url:
            raise AssertionError(f"plain-text dashboard source URL remained: {url}")


def main() -> int:
    energy_input = load_json(ROOT / "docs" / "evidence" / "energy_input_v1.sample.json")
    news_summary = load_json(ROOT / "docs" / "evidence" / "curated" / "news_summary_v1.sample.json")
    ai_insight = load_json(
        ROOT / "docs" / "evidence" / "phase17p-managed-ai-validated-ai-insight-20260528.json",
    )

    snapshot = build_snapshot(energy_input, news_summary, ai_insight)
    assert_valid_snapshot(snapshot)
    assert_public_source_urls(snapshot)

    sources = snapshot["insights"][0]["sources"]
    energy_source = sources[0]
    if energy_source["url"] != "dashboard-data.json":
        raise AssertionError("managed energy reference did not use dashboard-data.json fallback")
    if "curated electricity dashboard sample for 2026-05-07" not in energy_source["label"]:
        raise AssertionError("managed energy reference context was not preserved in label")

    news_urls = [source["url"] for source in sources[1:]]
    if not news_urls or not all(url.startswith("https://") for url in news_urls):
        raise AssertionError("public news source URLs were not preserved")

    private_ai_insight = json.loads(json.dumps(ai_insight))
    private_ai_insight["insights"][0]["energy_references"][0][
        "reference"
    ] = "s3://private-bucket/curated/source=ai_orchestration/payload.json"
    private_snapshot = build_snapshot(energy_input, news_summary, private_ai_insight)
    assert_valid_snapshot(private_snapshot)
    assert_public_source_urls(private_snapshot)
    if private_snapshot["insights"][0]["sources"][0]["url"] != "dashboard-data.json":
        raise AssertionError("private S3 reference was not neutralized")

    print("Phase 17R dashboard source-link self-check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
