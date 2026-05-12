#!/usr/bin/env python3
"""Self-check Phase 8 Lambda handlers using an in-memory S3 client."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import datetime as dt
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.ai_orchestration import (  # noqa: E402
    failed_payload_key,
    generate_run_id,
    write_s3_json,
)


class MemoryS3:
    """Tiny subset of the S3 client API used by the handlers."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_: object) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise AssertionError(f"missing s3://{Bucket}/{Key}") from exc
        return {"Body": io.BytesIO(body)}


def load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_handler_module():
    path = ROOT / "lambda" / "news_ai_orchestration.py"
    spec = importlib.util.spec_from_file_location("news_ai_orchestration", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> int:
    handlers = load_handler_module()
    run_id = generate_run_id(
        now=dt.datetime(2026, 5, 11, 9, 30, 15, tzinfo=dt.UTC),
        suffix="a1b2c3d4",
    )
    run_success_path(handlers, run_id)
    run_failure_path(handlers, run_id)

    print(f"Phase 8 handler self-check passed for {run_id}")
    return 0


def run_success_path(handlers, run_id: str) -> None:
    s3 = MemoryS3()
    lake_bucket = "energy-market-lake-test"
    dashboard_bucket = "energy-market-dashboard-test"
    dashboard_data_key = "inputs/dashboard-data.json"
    dashboard_data = load_json(ROOT / "dashboard-ui" / "public" / "dashboard-data.json")
    write_s3_json(s3, lake_bucket, dashboard_data_key, dashboard_data)

    state = handlers.handle_event(
        {
            "action": "InitializeRun",
            "run_id": run_id,
            "lake_bucket": lake_bucket,
            "dashboard_bucket": dashboard_bucket,
            "dashboard_data_key": dashboard_data_key,
        },
        s3,
    )
    if state["status"] != "initialized":
        raise AssertionError("run was not initialized")
    if state["artifacts"]["dashboard_data"] != dashboard_data_key:
        raise AssertionError("dashboard data artifact was not initialized")

    state = {
        **state,
        "action": "ExportEnergyInput",
    }
    state = handlers.handle_event(state, s3)
    assert state["status"] == "energy_input_exported"

    news_example = load_json(ROOT / "schemas" / "examples" / "news_summary_v1.example.json")
    state = {
        **state,
        "action": "IngestNewsSummary",
        "news_articles": news_example["articles"],
    }
    state = handlers.handle_event(state, s3)
    assert state["status"] == "news_summary_ingested"

    for action, expected_status in [
        ("CreateAiInputBundle", "ai_input_bundle_created"),
        ("MergeAiInsightDeterministic", "ai_insight_merged"),
        ("PublishDashboardSnapshot", "dashboard_snapshot_published"),
    ]:
        state = {**state, "action": action}
        state = handlers.handle_event(state, s3)
        assert state["status"] == expected_status

    latest_key = "dashboard_snapshot_v1.json"
    immutable_key = f"snapshots/run_id={run_id}/dashboard_snapshot_v1.json"
    if (dashboard_bucket, latest_key) not in s3.objects:
        raise AssertionError("latest dashboard snapshot was not written")
    if (dashboard_bucket, immutable_key) not in s3.objects:
        raise AssertionError("immutable dashboard snapshot was not written")


def run_failure_path(handlers, run_id: str) -> None:
    s3 = MemoryS3()
    lake_bucket = "energy-market-lake-test"
    dashboard_bucket = "energy-market-dashboard-test"
    existing_snapshot = b'{"schema_version":"dashboard_snapshot_v1","status":"previous-good"}'
    s3.objects[(dashboard_bucket, "dashboard_snapshot_v1.json")] = existing_snapshot
    bad_article = {
        "source": "rss",
        "publisher": "Example Energy News",
        "title": "Bad article",
        "published_at": "2026-04-05T09:30:00Z",
        "summary": "Missing URL, topics, regions, entities, and source_reference.",
    }

    event = {
        "action": "IngestNewsSummary",
        "run_id": run_id,
        "lake_bucket": lake_bucket,
        "dashboard_bucket": dashboard_bucket,
        "news_articles": [bad_article],
    }
    try:
        handlers.handle_event(event, s3)
    except ValueError:
        pass
    else:
        raise AssertionError("bad news summary unexpectedly passed")

    failed_key = failed_payload_key("ingest_news_summary", run_id)
    if (lake_bucket, failed_key) not in s3.objects:
        raise AssertionError("failed validation record was not written")

    failed_record = json.loads(s3.objects[(lake_bucket, failed_key)].decode("utf-8"))
    if failed_record["run_id"] != run_id:
        raise AssertionError("failed record run_id mismatch")
    if failed_record["component"] != "ingest_news_summary":
        raise AssertionError("failed record component mismatch")
    if failed_record["schema_name"] != "news_summary_v1":
        raise AssertionError("failed record schema mismatch")
    if not failed_record["reason"]:
        raise AssertionError("failed record reason was empty")

    dashboard_key = "dashboard_snapshot_v1.json"
    if s3.objects[(dashboard_bucket, dashboard_key)] != existing_snapshot:
        raise AssertionError("existing dashboard snapshot changed after validation failure")


if __name__ == "__main__":
    raise SystemExit(main())
