#!/usr/bin/env python3
"""Self-check Phase 8 shared runtime helpers."""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.ai_orchestration import (  # noqa: E402
    artifact_key,
    audit_summary_key,
    build_state_payload,
    dashboard_snapshot_key,
    failed_payload_key,
    generate_run_id,
    validate_payload,
)


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def main() -> int:
    fixed_now = dt.datetime(2026, 5, 10, 9, 30, 15, tzinfo=dt.UTC)
    run_id = generate_run_id(now=fixed_now, suffix="a1b2c3d4")

    expected_run_id = "ai-insight-20260510T093015Z-a1b2c3d4"
    if run_id != expected_run_id:
        raise AssertionError(f"unexpected run_id: {run_id}")

    expected_energy_key = (
        "curated/source=ai_orchestration/dataset=energy_input/"
        f"run_id={run_id}/payload.json"
    )
    if artifact_key("energy_input", run_id) != expected_energy_key:
        raise AssertionError("energy input artifact key mismatch")

    expected_failed_key = (
        "failed/workflow=ai_insight/component=validate_ai_insight/"
        f"run_id={run_id}/payload.json"
    )
    if failed_payload_key("validate_ai_insight", run_id) != expected_failed_key:
        raise AssertionError("failed payload key mismatch")

    expected_audit_key = f"audit/workflow=ai_insight/run_id={run_id}/summary.json"
    if audit_summary_key(run_id) != expected_audit_key:
        raise AssertionError("audit summary key mismatch")

    if dashboard_snapshot_key() != "dashboard_snapshot_v1.json":
        raise AssertionError("dashboard snapshot key mismatch")

    expected_snapshot_key = f"snapshots/run_id={run_id}/dashboard_snapshot_v1.json"
    if dashboard_snapshot_key(run_id, immutable=True) != expected_snapshot_key:
        raise AssertionError("immutable dashboard snapshot key mismatch")

    state_payload = build_state_payload(
        run_id=run_id,
        lake_bucket="energy-market-lake-example",
        dashboard_bucket="energy-market-dashboard-example",
        status="started",
        artifacts={"energy_input": expected_energy_key},
        summary={"article_count": 18},
    )
    if state_payload["workflow"] != "ai_insight":
        raise AssertionError("state payload workflow mismatch")

    example = load_json(ROOT / "schemas" / "examples" / "energy_input_v1.example.json")
    validation_errors = validate_payload(example, "energy_input")
    if validation_errors:
        raise AssertionError(f"example energy input failed validation: {validation_errors}")

    print(f"Phase 8 runtime self-check passed for {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
