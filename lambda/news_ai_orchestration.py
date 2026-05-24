"""Lambda handlers for Phase 8 deterministic AI insight orchestration.

Step Functions invokes this Lambda with an `action` field for each workflow
state. The handler keeps full artifacts in S3 and returns the hybrid state
payload used by the next state.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any, Callable


MODULE_DIR = Path(__file__).resolve().parent
ROOT = MODULE_DIR if (MODULE_DIR / "energy_market").exists() else MODULE_DIR.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from energy_market.ai_orchestration import (  # noqa: E402
    artifact_key,
    build_failed_record,
    build_state_payload,
    dashboard_snapshot_key,
    failed_payload_key,
    generate_run_id,
    raise_for_validation_errors,
    read_s3_json,
    write_s3_json,
)
from energy_market.managed_ai import (  # noqa: E402
    DEFAULT_MAX_TOKENS,
    invoke_bedrock_ai_insight,
)
from energy_market.news_ai import (  # noqa: E402
    DEFAULT_FEEDS,
    build_ai_insight,
    build_bundle,
    build_energy_input,
    build_news_summary,
    build_snapshot,
    fetch_articles,
)


ActionHandler = Callable[[dict[str, Any], Any], dict[str, Any]]


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    """AWS Lambda entrypoint."""
    return handle_event(event, _s3_client())


def handle_event(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Dispatch one Step Functions action."""
    action = event.get("action")
    handlers: dict[str, ActionHandler] = {
        "InitializeRun": initialize_run,
        "ExportEnergyInput": export_energy_input,
        "IngestNewsSummary": ingest_news_summary,
        "CreateAiInputBundle": create_ai_input_bundle,
        "MergeAiInsightDeterministic": merge_ai_insight_deterministic,
        "MergeAiInsightManaged": merge_ai_insight_managed,
        "PublishDashboardSnapshot": publish_dashboard_snapshot,
    }

    handler = handlers.get(str(action))
    if handler is None:
        allowed = ", ".join(sorted(handlers))
        raise ValueError(f"unknown action {action!r}; expected one of {allowed}")

    try:
        return handler(event, s3_client)
    except Exception as exc:
        _write_failed_record(event, s3_client, exc)
        raise


def initialize_run(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Create the initial Step Functions state payload for one orchestration run."""
    del s3_client
    run_id = str(event.get("run_id") or generate_run_id())
    lake_bucket = event.get("lake_bucket") or _env("DATA_BUCKET")
    dashboard_bucket = event.get("dashboard_bucket") or _env("DASHBOARD_BUCKET")
    dashboard_data_key = event.get("dashboard_data_key") or _env("DASHBOARD_DATA_KEY")
    if not lake_bucket:
        raise ValueError("lake_bucket or DATA_BUCKET is required")
    if not dashboard_bucket:
        raise ValueError("dashboard_bucket or DASHBOARD_BUCKET is required")
    if not dashboard_data_key:
        raise ValueError("dashboard_data_key or DASHBOARD_DATA_KEY is required")

    return build_state_payload(
        run_id=run_id,
        lake_bucket=str(lake_bucket),
        dashboard_bucket=str(dashboard_bucket),
        status="initialized",
        artifacts={"dashboard_data": str(dashboard_data_key)},
        summary={"mode": _env("AI_ORCHESTRATION_MODE", "deterministic")},
    )


def export_energy_input(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Build and write `energy_input_v1` from dashboard data JSON."""
    state = _state(event)
    dashboard_data = _read_input_json(
        event,
        s3_client,
        artifact_name="dashboard_data",
        inline_name="dashboard_data",
    )
    payload = build_energy_input(dashboard_data)
    _validate_or_raise(payload, "energy_input", "export_energy_input")

    key = artifact_key("energy_input", state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, payload)

    return _with_artifact(
        state,
        status="energy_input_exported",
        artifact_name="energy_input",
        artifact_key_value=key,
        summary_updates={"energy_record_count": len(payload.get("records", []))},
    )


def ingest_news_summary(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Fetch or accept news articles and write `news_summary_v1`."""
    state = _state(event)
    articles = event.get("news_articles")
    if articles is None:
        feeds = _news_feeds(event)
        limit_per_feed = int(event.get("news_limit_per_feed", _env("NEWS_LIMIT_PER_FEED", "4")))
        max_articles = int(event.get("news_max_articles", _env("NEWS_MAX_ARTICLES", "18")))
        articles = fetch_articles(feeds, limit_per_feed, max_articles)

    if not articles:
        raise ValueError("news summary contains no articles")

    payload = build_news_summary(articles)
    _validate_or_raise(payload, "news_summary", "ingest_news_summary")

    key = artifact_key("news_summary", state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, payload)

    return _with_artifact(
        state,
        status="news_summary_ingested",
        artifact_name="news_summary",
        artifact_key_value=key,
        summary_updates={"article_count": len(payload.get("articles", []))},
    )


def create_ai_input_bundle(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Read validated inputs and write the AI input bundle artifact."""
    state = _state(event)
    energy_input = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "energy_input"),
    )
    news_summary = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "news_summary"),
    )

    payload = build_bundle(energy_input, news_summary)
    _validate_or_raise(payload, "ai_input_bundle", "create_ai_input_bundle")
    key = artifact_key("ai_input_bundle", state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, payload)

    return _with_artifact(
        state,
        status="ai_input_bundle_created",
        artifact_name="ai_input_bundle",
        artifact_key_value=key,
    )


def merge_ai_insight_deterministic(
    event: dict[str, Any],
    s3_client: Any,
) -> dict[str, Any]:
    """Read the AI bundle and write a deterministic `ai_insight_v1` artifact."""
    state = _state(event)
    bundle = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "ai_input_bundle"),
    )

    payload = build_ai_insight(bundle)
    _validate_or_raise(payload, "ai_insight", "merge_ai_insight_deterministic")

    key = artifact_key("ai_insight", state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, payload)
    insight = payload.get("insights", [{}])[0]

    return _with_artifact(
        state,
        status="ai_insight_merged",
        artifact_name="ai_insight",
        artifact_key_value=key,
        summary_updates={
            "insight_count": len(payload.get("insights", [])),
            "risk_level": insight.get("risk_level", "watch"),
        },
    )


def merge_ai_insight_managed(
    event: dict[str, Any],
    s3_client: Any,
    bedrock_client: Any | None = None,
) -> dict[str, Any]:
    """Read the AI bundle and write a Bedrock-generated `ai_insight_v1` artifact."""
    state = _state(event)
    bundle = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "ai_input_bundle"),
    )
    model_id = str(event.get("bedrock_model_id") or _env("BEDROCK_MODEL_ID"))
    if not model_id:
        raise ValueError("bedrock_model_id or BEDROCK_MODEL_ID is required")

    payload = invoke_bedrock_ai_insight(
        bedrock_client or _bedrock_client(),
        model_id=model_id,
        bundle=bundle,
        provider=event.get("bedrock_provider") or _env("BEDROCK_PROVIDER"),
        max_tokens=int(
            event.get(
                "bedrock_max_tokens",
                _env("BEDROCK_MAX_TOKENS", str(DEFAULT_MAX_TOKENS)),
            ),
        ),
        temperature=float(
            event.get(
                "bedrock_temperature",
                _env("BEDROCK_TEMPERATURE", "0.2"),
            ),
        ),
    )
    _validate_or_raise(payload, "ai_insight", "merge_ai_insight_managed")

    key = artifact_key("ai_insight", state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, payload)
    insight = payload.get("insights", [{}])[0]

    return _with_artifact(
        state,
        status="ai_insight_managed",
        artifact_name="ai_insight",
        artifact_key_value=key,
        summary_updates={
            "insight_count": len(payload.get("insights", [])),
            "risk_level": insight.get("risk_level", "watch"),
            "ai_provider": "bedrock",
            "bedrock_provider": event.get("bedrock_provider") or _env("BEDROCK_PROVIDER", "auto"),
            "bedrock_model_id": model_id,
        },
    )


def publish_dashboard_snapshot(event: dict[str, Any], s3_client: Any) -> dict[str, Any]:
    """Build and publish the public-safe dashboard snapshot."""
    state = _state(event)
    energy_input = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "energy_input"),
    )
    news_summary = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "news_summary"),
    )
    ai_insight = read_s3_json(
        s3_client,
        state["lake_bucket"],
        _artifact(state, "ai_insight"),
    )

    payload = build_snapshot(energy_input, news_summary, ai_insight)
    _validate_or_raise(payload, "dashboard_snapshot", "publish_dashboard_snapshot")

    latest_key = dashboard_snapshot_key()
    immutable_key = dashboard_snapshot_key(state["run_id"], immutable=True)
    write_s3_json(s3_client, state["dashboard_bucket"], immutable_key, payload)
    write_s3_json(s3_client, state["dashboard_bucket"], latest_key, payload)

    return _with_artifact(
        state,
        status="dashboard_snapshot_published",
        artifact_name="dashboard_snapshot",
        artifact_key_value=latest_key,
        summary_updates={"immutable_dashboard_snapshot": immutable_key},
    )


def _state(event: dict[str, Any]) -> dict[str, Any]:
    run_id = _required(event, "run_id")
    lake_bucket = event.get("lake_bucket") or _env("DATA_BUCKET")
    dashboard_bucket = event.get("dashboard_bucket") or _env("DASHBOARD_BUCKET")
    if not lake_bucket:
        raise ValueError("lake_bucket or DATA_BUCKET is required")
    if not dashboard_bucket:
        raise ValueError("dashboard_bucket or DASHBOARD_BUCKET is required")

    return build_state_payload(
        run_id=run_id,
        lake_bucket=lake_bucket,
        dashboard_bucket=dashboard_bucket,
        status=str(event.get("status", "started")),
        artifacts=dict(event.get("artifacts", {})),
        summary=dict(event.get("summary", {})),
    )


def _with_artifact(
    state: dict[str, Any],
    *,
    status: str,
    artifact_name: str,
    artifact_key_value: str,
    summary_updates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    artifacts = dict(state.get("artifacts", {}))
    artifacts[artifact_name] = artifact_key_value
    summary = dict(state.get("summary", {}))
    summary.update(summary_updates or {})

    return build_state_payload(
        run_id=state["run_id"],
        lake_bucket=state["lake_bucket"],
        dashboard_bucket=state["dashboard_bucket"],
        status=status,
        artifacts=artifacts,
        summary=summary,
    )


def _read_input_json(
    event: dict[str, Any],
    s3_client: Any,
    *,
    artifact_name: str,
    inline_name: str,
) -> dict[str, Any]:
    inline_payload = event.get(inline_name)
    if inline_payload is not None:
        return inline_payload

    state = _state(event)
    key = _artifact(state, artifact_name)
    return read_s3_json(s3_client, state["lake_bucket"], key)


def _artifact(state: dict[str, Any], name: str) -> str:
    artifacts = state.get("artifacts", {})
    key = artifacts.get(name)
    if not key:
        raise ValueError(f"artifact {name!r} is required")
    return str(key)


def _validate_or_raise(payload: dict[str, Any], contract: str, component: str) -> None:
    raise_for_validation_errors(payload, contract, component)


def _write_failed_record(event: dict[str, Any], s3_client: Any, exc: Exception) -> None:
    try:
        state = _state(event)
    except Exception:
        return

    component = _failure_component(event, exc)
    schema_name = _failure_schema(exc)
    reason = str(exc)
    payload = _failure_payload(event, exc)
    record = build_failed_record(
        run_id=state["run_id"],
        component=component,
        schema_name=schema_name,
        reason=reason,
        payload=payload,
    )
    key = failed_payload_key(component, state["run_id"])
    write_s3_json(s3_client, state["lake_bucket"], key, record)


def _failure_component(event: dict[str, Any], exc: Exception) -> str:
    component = getattr(exc, "component", None)
    if component:
        return str(component)
    return str(event.get("action", "unknown")).lower()


def _failure_schema(exc: Exception) -> str:
    contract = getattr(exc, "contract", "")
    if contract:
        return f"{contract}_v1"
    return "unknown"


def _failure_payload(event: dict[str, Any], exc: Exception) -> dict[str, Any]:
    payload = getattr(exc, "payload", None)
    if isinstance(payload, dict):
        return payload
    return {
        "action": event.get("action"),
        "artifacts": event.get("artifacts", {}),
        "summary": event.get("summary", {}),
    }


def _news_feeds(event: dict[str, Any]) -> list[str]:
    feeds = event.get("news_feeds")
    if isinstance(feeds, list):
        return [str(feed) for feed in feeds if str(feed).strip()]

    configured = _env("NEWS_FEEDS", "")
    if configured:
        return [feed.strip() for feed in configured.split(",") if feed.strip()]

    return DEFAULT_FEEDS


def _required(event: dict[str, Any], name: str) -> str:
    value = event.get(name)
    if value in (None, ""):
        raise ValueError(f"{name} is required")
    return str(value)


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


def _s3_client() -> Any:
    import boto3  # noqa: WPS433

    return boto3.client("s3")


def _bedrock_client() -> Any:
    import boto3  # noqa: WPS433

    return boto3.client("bedrock-runtime")
