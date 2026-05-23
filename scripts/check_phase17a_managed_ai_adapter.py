#!/usr/bin/env python3
"""Self-check the Phase 17A Bedrock adapter with fake clients only."""

from __future__ import annotations

import datetime as dt
import importlib.util
import io
import json
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from energy_market.ai_orchestration import (  # noqa: E402
    artifact_key,
    failed_payload_key,
    generate_run_id,
    raise_for_validation_errors,
    write_s3_json,
)
from energy_market.managed_ai import (  # noqa: E402
    build_bedrock_request,
    build_mistral_request,
    parse_bedrock_response,
    provider_from_model_id,
)
from energy_market.news_ai import build_ai_insight  # noqa: E402


class MemoryS3:
    """Tiny subset of the S3 client API used by the handlers."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, **_: object) -> None:
        self.objects[(Bucket, Key)] = Body

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        try:
            body = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise AssertionError(f"missing s3://{Bucket}/{Key}") from exc
        return {"Body": io.BytesIO(body)}


class FakeBedrock:
    """Fake Bedrock Runtime client for local proof."""

    def __init__(
        self,
        payload: dict[str, Any],
        *,
        expected_max_tokens: int = 800,
        expected_temperature: float = 0.2,
        expected_provider: str = "anthropic",
        response_shape: str = "anthropic",
    ) -> None:
        self.payload = payload
        self.expected_max_tokens = expected_max_tokens
        self.expected_temperature = expected_temperature
        self.expected_provider = expected_provider
        self.response_shape = response_shape
        self.calls: list[dict[str, Any]] = []

    def invoke_model(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        request = json.loads(kwargs["body"].decode("utf-8"))
        if request["max_tokens"] != self.expected_max_tokens:
            raise AssertionError("max token limit was not passed to Bedrock request")
        if request["temperature"] != self.expected_temperature:
            raise AssertionError("temperature was not passed to Bedrock request")
        if self.expected_provider == "mistral":
            if "anthropic_version" in request:
                raise AssertionError("Mistral request used Anthropic format")
            if not isinstance(request["messages"][0]["content"], str):
                raise AssertionError("Mistral message content must be a string")
        else:
            if request.get("anthropic_version") != "bedrock-2023-05-31":
                raise AssertionError("Anthropic request version was missing")
            content = request["messages"][0]["content"]
            if not isinstance(content, list) or content[0].get("type") != "text":
                raise AssertionError("Anthropic message content must be text blocks")
        text = json.dumps(self.payload)
        if self.response_shape == "mistral":
            body = {"choices": [{"message": {"role": "assistant", "content": text}}]}
        else:
            body = {"content": [{"type": "text", "text": text}]}
        return {
            "body": io.BytesIO(
                json.dumps(body).encode("utf-8"),
            ),
        }


def load_json(path: Path) -> dict[str, Any]:
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
    bundle = load_json(ROOT / "docs" / "evidence" / "ai" / "ai_input_bundle_v1.sample.json")
    managed_payload = build_ai_insight(bundle)
    managed_payload["insights"][0]["validation_notes"] = [
        "Managed Bedrock adapter fake-client output.",
        "No live model invocation was made in Phase 17A.",
    ]
    raise_for_validation_errors(managed_payload, "ai_insight", "phase17a_fake_payload")

    request = build_bedrock_request(bundle, max_tokens=700, temperature=0.1)
    if "Return only valid JSON" not in request["messages"][0]["content"][0]["text"]:
        raise AssertionError("prompt does not constrain the model response")
    mistral_request = build_mistral_request(bundle, max_tokens=700, temperature=0.1)
    if "anthropic_version" in mistral_request:
        raise AssertionError("Mistral request should not include Anthropic version")
    if not isinstance(mistral_request["messages"][0]["content"], str):
        raise AssertionError("Mistral request should use string message content")
    if provider_from_model_id("mistral.ministral-3-8b-instruct") != "mistral":
        raise AssertionError("Mistral provider inference failed")

    parsed = parse_bedrock_response(
        {"body": io.BytesIO(json.dumps(managed_payload).encode("utf-8"))},
    )
    if parsed["schema_version"] != "ai_insight_v1":
        raise AssertionError("direct JSON response parsing failed")
    mistral_parsed = parse_bedrock_response(
        {
            "body": io.BytesIO(
                json.dumps(
                    {"choices": [{"message": {"content": json.dumps(managed_payload)}}]},
                ).encode("utf-8"),
            ),
        },
    )
    if mistral_parsed["schema_version"] != "ai_insight_v1":
        raise AssertionError("Mistral choices response parsing failed")

    handlers = load_handler_module()
    run_id = generate_run_id(
        now=dt.datetime(2026, 5, 22, 10, 30, 0, tzinfo=dt.UTC),
        suffix="17a00001",
    )
    run_managed_success_path(
        handlers,
        run_id,
        bundle,
        managed_payload,
        model_id="test.bedrock-model-v1",
        expected_provider="anthropic",
        response_shape="anthropic",
    )
    run_managed_success_path(
        handlers,
        run_id,
        bundle,
        managed_payload,
        model_id="mistral.ministral-3-8b-instruct",
        expected_provider="mistral",
        response_shape="mistral",
    )
    run_managed_failure_path(handlers, run_id, bundle)

    print(f"Phase 17 managed AI adapter self-check passed for {run_id}")
    return 0


def run_managed_success_path(
    handlers: Any,
    run_id: str,
    bundle: dict[str, Any],
    managed_payload: dict[str, Any],
    model_id: str,
    expected_provider: str,
    response_shape: str,
) -> None:
    s3 = MemoryS3()
    lake_bucket = "energy-market-lake-test"
    dashboard_bucket = "energy-market-dashboard-test"
    bundle_key = artifact_key("ai_input_bundle", run_id)
    write_s3_json(s3, lake_bucket, bundle_key, bundle)

    fake_bedrock = FakeBedrock(
        managed_payload,
        expected_max_tokens=700,
        expected_temperature=0.1,
        expected_provider=expected_provider,
        response_shape=response_shape,
    )
    handlers._bedrock_client = lambda: fake_bedrock
    state = handlers.handle_event(
        {
            "action": "MergeAiInsightManaged",
            "run_id": run_id,
            "lake_bucket": lake_bucket,
            "dashboard_bucket": dashboard_bucket,
            "artifacts": {"ai_input_bundle": bundle_key},
            "bedrock_model_id": model_id,
            "bedrock_max_tokens": 700,
            "bedrock_temperature": 0.1,
        },
        s3,
    )

    if state["status"] != "ai_insight_managed":
        raise AssertionError("managed merge status was not returned")
    if state["summary"]["ai_provider"] != "bedrock":
        raise AssertionError("managed merge did not record Bedrock provider")
    if fake_bedrock.calls[0]["modelId"] != model_id:
        raise AssertionError("model ID was not passed to Bedrock")
    if state["summary"]["bedrock_provider"] != "auto":
        raise AssertionError("provider should default to auto in state summary")

    ai_key = artifact_key("ai_insight", run_id)
    payload = json.loads(s3.objects[(lake_bucket, ai_key)].decode("utf-8"))
    raise_for_validation_errors(payload, "ai_insight", "phase17a_written_payload")


def run_managed_failure_path(
    handlers: Any,
    run_id: str,
    bundle: dict[str, Any],
) -> None:
    s3 = MemoryS3()
    lake_bucket = "energy-market-lake-test"
    dashboard_bucket = "energy-market-dashboard-test"
    bundle_key = artifact_key("ai_input_bundle", run_id)
    write_s3_json(s3, lake_bucket, bundle_key, bundle)

    handlers._bedrock_client = lambda: FakeBedrock({"schema_version": "bad"})
    try:
        handlers.handle_event(
            {
                "action": "MergeAiInsightManaged",
                "run_id": run_id,
                "lake_bucket": lake_bucket,
                "dashboard_bucket": dashboard_bucket,
                "artifacts": {"ai_input_bundle": bundle_key},
                "bedrock_model_id": "test.bedrock-model-v1",
            },
            s3,
        )
    except ValueError:
        pass
    else:
        raise AssertionError("invalid managed AI output unexpectedly passed")

    failed_key = failed_payload_key("merge_ai_insight_managed", run_id)
    if (lake_bucket, failed_key) not in s3.objects:
        raise AssertionError("managed AI validation failure was not quarantined")


if __name__ == "__main__":
    raise SystemExit(main())
