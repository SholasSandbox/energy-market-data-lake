"""Managed AI adapter helpers for Phase 17.

The adapter is intentionally provider-light and client-injected so it can be
tested without live Bedrock calls. The first concrete target is Bedrock Runtime
`invoke_model`, with the existing `ai_insight_v1` schema remaining the safety
gate outside this module.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any


DEFAULT_MAX_TOKENS = 1600
DEFAULT_TEMPERATURE = 0.2
PROVIDER_ANTHROPIC = "anthropic"
PROVIDER_MISTRAL = "mistral"
REFERENCE_FIELD_ALLOWLISTS = {
    "energy_references": frozenset({"source", "metric", "reference"}),
    "news_references": frozenset({"publisher", "title", "url"}),
}


class ManagedAIResponseError(ValueError):
    """Raised when a managed model response cannot be parsed as JSON."""


def build_ai_insight_prompt(bundle: dict[str, Any]) -> str:
    """Return a constrained prompt for producing `ai_insight_v1` JSON."""
    return "\n".join(
        [
            "You are producing a controlled energy-market AI insight.",
            "Return only valid JSON matching schemas/ai_insight_v1.schema.json.",
            "The JSON root object must be the ai_insight_v1 object itself.",
            "The root object must contain schema_version, generated_at, and insights.",
            "Each insight must contain id, title, summary, region, risk_level,",
            "confidence, time_window, energy_references, news_references, and",
            "validation_notes.",
            "Do not use a generic references field.",
            "energy_references must be an array of source/metric/reference objects.",
            "news_references must be an array of publisher/title/url objects.",
            "Do not add value, date, timestamp, or other fields to references.",
            "time_window must be an object with start and end date-time strings.",
            "Do not return time_window as a plain string.",
            "validation_notes must be an array of strings, not a single string.",
            "Return exactly one concise insight unless the input requires more.",
            "Keep summary and validation_notes brief.",
            "Never truncate JSON; shorten prose if the token budget is tight.",
            "Do not wrap the payload in ai_insight_v1, ai_insight, result,",
            "output, response, data, or any other key.",
            "Do not include markdown fences, commentary, or private fields.",
            "The first output character must be { and the final character must be }.",
            "Use only the validated bundle content below.",
            "",
            json.dumps(bundle, indent=2, sort_keys=True),
        ],
    )


def build_bedrock_request(
    bundle: dict[str, Any],
    *,
    provider: str = PROVIDER_ANTHROPIC,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
) -> dict[str, Any]:
    """Build a provider-specific Bedrock request body."""
    normalized_provider = normalize_provider(provider)
    if normalized_provider == PROVIDER_MISTRAL:
        return build_mistral_request(
            bundle,
            max_tokens=max_tokens,
            temperature=temperature,
        )
    return build_anthropic_request(
        bundle,
        max_tokens=max_tokens,
        temperature=temperature,
    )


def build_anthropic_request(
    bundle: dict[str, Any],
    *,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
) -> dict[str, Any]:
    """Build an Anthropic-compatible Bedrock request body."""
    return {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": build_ai_insight_prompt(bundle),
                    },
                ],
            },
        ],
    }


def build_mistral_request(
    bundle: dict[str, Any],
    *,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
) -> dict[str, Any]:
    """Build a Mistral chat-completion request body for Bedrock."""
    return {
        "messages": [
            {
                "role": "user",
                "content": build_ai_insight_prompt(bundle),
            },
        ],
        "max_tokens": max_tokens,
        "temperature": temperature,
    }


def normalize_provider(provider: str | None) -> str:
    """Return the supported provider name for a Bedrock model family."""
    normalized = (provider or PROVIDER_ANTHROPIC).strip().lower()
    if normalized in {PROVIDER_ANTHROPIC, PROVIDER_MISTRAL}:
        return normalized
    raise ValueError(f"unsupported managed AI provider {provider!r}")


def provider_from_model_id(model_id: str) -> str:
    """Infer provider from the Bedrock model ID prefix."""
    if model_id.startswith("mistral."):
        return PROVIDER_MISTRAL
    if model_id.startswith("anthropic."):
        return PROVIDER_ANTHROPIC
    return PROVIDER_ANTHROPIC


def invoke_bedrock_ai_insight(
    bedrock_client: Any,
    *,
    model_id: str,
    bundle: dict[str, Any],
    provider: str | None = None,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    temperature: float = DEFAULT_TEMPERATURE,
) -> dict[str, Any]:
    """Invoke Bedrock Runtime and return the parsed `ai_insight_v1` payload."""
    request = build_bedrock_request(
        bundle,
        provider=provider or provider_from_model_id(model_id),
        max_tokens=max_tokens,
        temperature=temperature,
    )
    response = bedrock_client.invoke_model(
        modelId=model_id,
        body=json.dumps(request).encode("utf-8"),
        contentType="application/json",
        accept="application/json",
    )
    return parse_bedrock_response(response)


def normalize_ai_insight_reference_objects(
    payload: dict[str, Any],
) -> dict[str, Any]:
    """Remove model-added reference metadata before strict schema validation.

    Managed models can add descriptive fields such as ``value`` even when the
    prompt forbids them. Reference objects have a locked, lossless identity
    shape, so prune only fields outside those explicit allowlists. All other
    payload content remains untouched for the schema validator to accept or
    reject.
    """
    normalized = copy.deepcopy(payload)
    insights = normalized.get("insights")
    if not isinstance(insights, list):
        return normalized

    for insight in insights:
        if not isinstance(insight, dict):
            continue
        for field_name, allowed_fields in REFERENCE_FIELD_ALLOWLISTS.items():
            references = insight.get(field_name)
            if not isinstance(references, list):
                continue
            for index, reference in enumerate(references):
                if isinstance(reference, dict):
                    references[index] = {
                        key: value
                        for key, value in reference.items()
                        if key in allowed_fields
                    }

    return normalized


def parse_bedrock_response(response: dict[str, Any]) -> dict[str, Any]:
    """Parse common Bedrock response shapes into a JSON object."""
    payload = _read_response_body(response)
    payload = _unwrap_ai_insight_payload(payload)
    if _looks_like_ai_insight(payload):
        return payload

    text = _extract_text(payload)
    return _parse_json_text(text)


def _read_response_body(response: dict[str, Any]) -> dict[str, Any]:
    body = response.get("body", response.get("Body", response))
    if hasattr(body, "read"):
        body = body.read()
    if isinstance(body, bytes):
        body = body.decode("utf-8")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError as exc:
            raise ManagedAIResponseError("Bedrock response body was not JSON") from exc
    if isinstance(body, dict):
        return body
    raise ManagedAIResponseError("Bedrock response body used an unsupported type")


def _extract_text(payload: dict[str, Any]) -> str:
    content = payload.get("content")
    if isinstance(content, list):
        text_parts = [
            item.get("text", "")
            for item in content
            if isinstance(item, dict) and item.get("type", "text") == "text"
        ]
        text = "\n".join(part for part in text_parts if part)
        if text:
            return text

    output = payload.get("output")
    if isinstance(output, dict):
        message = output.get("message", {})
        message_content = message.get("content", [])
        if isinstance(message_content, list):
            text = "\n".join(
                item.get("text", "")
                for item in message_content
                if isinstance(item, dict) and item.get("text")
            )
            if text:
                return text

    choices = payload.get("choices")
    if isinstance(choices, list):
        for choice in choices:
            if not isinstance(choice, dict):
                continue
            message = choice.get("message", {})
            if isinstance(message, dict) and isinstance(message.get("content"), str):
                return message["content"]
            if isinstance(choice.get("text"), str):
                return choice["text"]

    completion = payload.get("completion")
    if isinstance(completion, str):
        return completion

    raise ManagedAIResponseError("Bedrock response did not contain text output")


def _parse_json_text(text: str) -> dict[str, Any]:
    cleaned = _strip_markdown_fences(text.strip())
    try:
        payload = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ManagedAIResponseError(_json_parse_error_message(text, cleaned)) from exc
    if not isinstance(payload, dict):
        raise ManagedAIResponseError("managed AI JSON output must be an object")
    return _unwrap_ai_insight_payload(payload)


def _strip_markdown_fences(text: str) -> str:
    match = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", text, flags=re.DOTALL)
    if match:
        return match.group(1)
    return text


def _json_parse_error_message(original: str, cleaned: str) -> str:
    stripped = original.lstrip()
    if stripped.startswith("```") and not re.fullmatch(
        r"```(?:json)?\s*.*?\s*```",
        original.strip(),
        flags=re.DOTALL,
    ):
        return "managed AI text output used an incomplete markdown fence"
    if _looks_truncated_json(cleaned):
        return "managed AI text output appears truncated before valid JSON completed"
    return "managed AI text output was not JSON"


def _looks_truncated_json(text: str) -> bool:
    stripped = text.strip()
    if not stripped:
        return False
    if stripped[0] not in "{[":
        return False
    return stripped.count("{") != stripped.count("}") or stripped.count("[") != stripped.count("]")


def _looks_like_ai_insight(payload: dict[str, Any]) -> bool:
    return payload.get("schema_version") == "ai_insight_v1"


def _unwrap_ai_insight_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Accept the observed one-key Mistral wrapper without broad unwrapping."""
    if _looks_like_ai_insight(payload):
        return payload
    if set(payload) == {"ai_insight_v1"} and isinstance(payload["ai_insight_v1"], dict):
        nested = payload["ai_insight_v1"]
        if _looks_like_ai_insight(nested):
            return nested
    if set(payload) == {"ai_insight"} and isinstance(payload["ai_insight"], dict):
        nested = payload["ai_insight"]
        if _looks_like_ai_insight(nested):
            return nested
    return payload
