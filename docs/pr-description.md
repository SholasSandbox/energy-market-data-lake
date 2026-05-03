# PR Description

## Summary

Extended the serverless energy data lake project into a local news-aware insight dashboard MVP.

This update adds schema-controlled energy, news, AI insight, and dashboard snapshot contracts; a local evidence pipeline; deterministic AI-style insight generation; failure validation for malformed samples; and a React dashboard view that displays approved public snapshot JSON only.

## What Changed

- Added local RSS/news evidence flow.
- Added energy input, AI input bundle, AI insight, and dashboard snapshot evidence.
- Added validation for good contracts and known-bad failure samples.
- Updated the React dashboard to show AI insight, confidence, source references, data quality, and stale-data warning.
- Added Week 4 documentation polish, demo walkthrough, setup guidance, and dashboard screenshot evidence.
- Added README demo evidence section linking the walkthrough, screenshot, and validation command.

## Architecture Notes

- Raw, curated, failed, and audit data remain private.
- The dashboard reads only approved `dashboard_snapshot_v1.sample.json`.
- AI output must pass `ai_insight_v1` validation before publishing.
- Current AI merge is deterministic local logic; OpenClaw or Bedrock remains a future cloud/runtime extension.

## Validation

```bash
source .venv/bin/activate
python scripts/ingest_news_local.py
python scripts/export_energy_input_local.py
python scripts/create_ai_input_bundle_local.py
python scripts/merge_ai_insight_local.py
python scripts/publish_dashboard_snapshot_local.py
python scripts/validate_contracts.py --include-evidence --check-failures
```
