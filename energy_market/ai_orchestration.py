"""Shared helpers for Phase 8 AWS AI insight orchestration.

The helpers keep Step Functions payloads small by treating S3 as the artifact
store of record. They are intentionally dependency-light so local scripts and
future Lambda handlers can use the same run IDs, keys, and validation behavior.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import uuid
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"

RUN_ID_PATTERN = re.compile(
    r"^ai-insight-\d{8}T\d{6}Z-[0-9a-f]{8}$",
)

ARTIFACT_DATASETS = {
    "energy_input": "energy_input",
    "news_summary": "news_summary",
    "ai_input_bundle": "ai_input_bundle",
    "ai_insight": "ai_insight",
}

CONTRACT_SCHEMAS = {
    "energy_input": "energy_input_v1.schema.json",
    "news_summary": "news_summary_v1.schema.json",
    "ai_input_bundle": "ai_input_bundle_v1.schema.json",
    "ai_insight": "ai_insight_v1.schema.json",
    "dashboard_snapshot": "dashboard_snapshot_v1.schema.json",
}


class PayloadValidationError(ValueError):
    """Raised when a contract payload does not pass validation."""

    def __init__(
        self,
        *,
        component: str,
        contract: str,
        errors: list[str],
        payload: dict[str, Any],
    ) -> None:
        self.component = component
        self.contract = contract
        self.errors = errors
        self.payload = payload
        reason = errors[0] if errors else "validation failed"
        super().__init__(f"{contract} validation failed in {component}: {reason}")


def generate_run_id(now: dt.datetime | None = None, suffix: str | None = None) -> str:
    """Return a traceable, collision-safe Phase 8 run ID."""
    timestamp_source = now or dt.datetime.now(dt.UTC)
    timestamp = timestamp_source.astimezone(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    short_suffix = suffix or uuid.uuid4().hex[:8]

    if not re.fullmatch(r"[0-9a-f]{8}", short_suffix):
        raise ValueError("run ID suffix must be 8 lowercase hexadecimal characters")

    return f"ai-insight-{timestamp}-{short_suffix}"


def validate_run_id(run_id: str) -> None:
    """Raise ValueError when a run ID does not match the locked Phase 8 shape."""
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(
            "run_id must match ai-insight-YYYYMMDDTHHMMSSZ-<8-char-uuid>",
        )


def artifact_key(dataset: str, run_id: str, filename: str = "payload.json") -> str:
    """Return the private lake S3 key for one contract artifact."""
    validate_run_id(run_id)
    dataset_name = ARTIFACT_DATASETS.get(dataset)
    if dataset_name is None:
        allowed = ", ".join(sorted(ARTIFACT_DATASETS))
        raise ValueError(f"unknown artifact dataset {dataset!r}; expected one of {allowed}")

    return (
        f"curated/source=ai_orchestration/dataset={dataset_name}/"
        f"run_id={run_id}/{filename}"
    )


def failed_payload_key(component: str, run_id: str) -> str:
    """Return the failed-zone key for a rejected Phase 8 payload."""
    validate_run_id(run_id)
    safe_component = _safe_path_token(component, field_name="component")
    return (
        "failed/workflow=ai_insight/"
        f"component={safe_component}/run_id={run_id}/payload.json"
    )


def audit_summary_key(run_id: str) -> str:
    """Return the audit-zone summary key for a completed Phase 8 run."""
    validate_run_id(run_id)
    return f"audit/workflow=ai_insight/run_id={run_id}/summary.json"


def dashboard_snapshot_key(run_id: str | None = None, immutable: bool = False) -> str:
    """Return the public dashboard snapshot key."""
    if immutable:
        if not run_id:
            raise ValueError("immutable dashboard snapshots require run_id")
        validate_run_id(run_id)
        return f"snapshots/run_id={run_id}/dashboard_snapshot_v1.json"

    return "dashboard_snapshot_v1.json"


def build_state_payload(
    *,
    run_id: str,
    lake_bucket: str,
    dashboard_bucket: str,
    status: str = "started",
    artifacts: dict[str, str] | None = None,
    summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the hybrid Step Functions payload shape locked for Phase 8."""
    validate_run_id(run_id)
    return {
        "workflow": "ai_insight",
        "run_id": run_id,
        "status": status,
        "lake_bucket": lake_bucket,
        "dashboard_bucket": dashboard_bucket,
        "artifacts": artifacts or {},
        "summary": summary or {},
    }


def read_s3_json(s3_client: Any, bucket: str, key: str) -> dict[str, Any]:
    """Read JSON from S3 using an injected boto3 S3 client."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def write_s3_json(s3_client: Any, bucket: str, key: str, payload: dict[str, Any]) -> None:
    """Write pretty JSON to S3 using an injected boto3 S3 client."""
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8") + b"\n",
        ContentType="application/json",
    )


def validate_payload(
    payload: dict[str, Any],
    contract: str,
    schema_dir: Path = SCHEMA_DIR,
) -> list[str]:
    """Return JSON Schema validation errors for a Phase 8 contract payload."""
    schema_name = CONTRACT_SCHEMAS.get(contract)
    if schema_name is None:
        allowed = ", ".join(sorted(CONTRACT_SCHEMAS))
        raise ValueError(f"unknown contract {contract!r}; expected one of {allowed}")
    schema = _load_json(schema_dir / schema_name)
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    errors: list[str] = []
    for error in sorted(validator.iter_errors(payload), key=lambda err: list(err.path)):
        location = ".".join(str(part) for part in error.path) or "<root>"
        errors.append(f"{location}: {error.message}")
    return errors


def raise_for_validation_errors(
    payload: dict[str, Any],
    contract: str,
    component: str,
    schema_dir: Path = SCHEMA_DIR,
) -> None:
    """Raise a structured validation error when a payload fails its contract."""
    errors = validate_payload(payload, contract, schema_dir=schema_dir)
    if errors:
        raise PayloadValidationError(
            component=component,
            contract=contract,
            errors=errors,
            payload=payload,
        )


def build_failed_record(
    *,
    run_id: str,
    component: str,
    schema_name: str,
    reason: str,
    payload: dict[str, Any],
    status: str = "validation_failed",
) -> dict[str, Any]:
    """Build a structured failed-zone record for a rejected payload."""
    validate_run_id(run_id)
    return {
        "workflow": "ai_insight",
        "run_id": run_id,
        "component": component,
        "schema_name": schema_name,
        "status": status,
        "reason": reason,
        "payload": payload,
        "failed_at": dt.datetime.now(dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _safe_path_token(value: str, *, field_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError(f"{field_name} must contain only letters, numbers, . _ or -")
    return value
