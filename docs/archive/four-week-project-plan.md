# 4 Week Project Plan - Energy Data Lake + News Insight Dashboard

<!-- markdownlint-disable MD013 -->

Archived note: this document preserves the original 4-week MVP delivery plan.
It is historical context, not the current project source of truth. Use
`PLANS.md` and the phase-specific checklists for current status.

## Purpose

Ship a portfolio-ready MVP that extends the existing serverless energy data lake into a news-aware insight dashboard. The plan keeps the current Lambda, S3, Glue, Athena, and React foundation, then adds news ingestion, strict data contracts, an AI merge step, and a polished dashboard story.

## Current Status

Weeks 1, 2, and 3 are complete for the local MVP. Week 4 is in progress.

Implemented locally:

- schema contracts and examples
- local RSS/news ingestion evidence
- local energy input export
- curated evidence and failed evidence paths
- AI input bundle
- deterministic local AI insight merge
- validation of good and intentionally bad samples
- dashboard snapshot publisher
- React dashboard section that loads `dashboard_snapshot_v1.sample.json`
- data freshness warning for old local demo evidence
- concise demo walkthrough

Still target/future:

- Step Functions orchestration
- cloud-hosted OpenClaw or Bedrock `InvokeModel`
- SNS failure notifications
- CloudFront-hosted production dashboard
- fresh live data refresh before a public demo

## Delivery Principle

Build the ugly-but-solid version first, then polish. Completed evidence matters more than architectural elegance.

Scope rule:

- Week 1 proves the skeleton.
- Week 2 proves the data path.
- Week 3 proves AI output can be controlled.
- Week 4 makes it demo-ready.

## Local MVP Target

By the end of 4 weeks, the project should show:

- scheduled or manually runnable energy ingestion
- news summary ingestion
- private raw, curated, failed, and audit data zones
- validated JSON contracts
- AI-generated insight JSON with confidence and source references
- rejection of malformed AI output
- dashboard JSON that is safe for public consumption
- React dashboard view with charts, table, insight panel, and data-quality signal
- clear README, diagrams, screenshots, and demo script

## Non-Goals

- No production-grade multi-user app.
- No real-time streaming.
- No autonomous trading decisions.
- No unreviewed AI output reaching the dashboard.
- No RDS, NAT Gateway, always-on EC2, or expensive always-running infrastructure.
- No architecture redesign after Week 1 unless something is genuinely blocking delivery.

## Week 1 - Skeleton First

Goal: create the minimum working structure for the expanded project without breaking the existing energy lakehouse demo.

Status: complete.

### Week 1 Outcomes

- Active docs explain implemented baseline vs target extension.
- Schemas exist for the core contracts.
- News ingestion has a local runnable skeleton.
- Dashboard can load a safe sample snapshot.
- Architecture diagrams show trust boundaries.

### Week 1 Work Items

1. Create schema contracts:
   - `energy_input_v1.json`
   - `news_summary_v1.json`
   - `ai_insight_v1.json`
   - `dashboard_snapshot_v1.json`

2. Add a lightweight news ingestion path:
   - start as local script or Lambda-compatible helper
   - read 1-3 RSS feeds
   - extract title, URL, publisher, timestamp, summary, and topic tags
   - write local JSON first, then S3 later
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/ingest_news_local.py
     python scripts/validate_contracts.py --include-evidence
     ```

3. Define private/public storage boundaries:
   - private: `raw/`, `curated/`, `audit/`, `failed/`
   - public: approved dashboard JSON only

4. Add sample dashboard snapshot:
   - place a sample `dashboard_snapshot_v1.json` payload where the React app can load it
   - include at least one insight card, one chart-ready metric set, and one data-quality status

5. Verify current baseline still works:
   - dashboard generation command still runs
   - React app still starts locally
   - existing diagrams and README remain consistent

### Week 1 Acceptance Gate

- Existing dashboard flow still works.
- News ingest skeleton produces valid sample JSON.
- Public dashboard path does not read raw or curated private data.
- README and diagrams clearly say what is implemented and what is target.

### Week 1 Avoid

- polishing UI
- adding many feeds
- tuning AI prompts
- adding Step Functions, Streams, or Bedrock before contracts exist

## Week 2 - Data Pipeline

Goal: make energy and news data move through a dependable private pipeline with validation and failure handling.

Status: complete.

### Week 2 Outcomes

- Energy data can be exported or normalized into `energy_input_v1.json`.
- News data can be normalized into `news_summary_v1.json`.
- Good records move to curated output.
- Bad records move to failed output.
- Dashboard publisher can produce a basic safe snapshot.

### Week 2 Work Items

1. Build validation helpers:
   - validate required fields
   - validate timestamps
   - validate source URLs
   - validate confidence/risk enums where relevant

2. Normalize energy inputs:
   - use current Athena/dashboard data as the first energy source
   - map market price, demand, region, source, and timestamp into `energy_input_v1.json`
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/export_energy_input_local.py
     python scripts/validate_contracts.py --include-evidence
     ```

3. Normalize news summaries:
   - keep summaries short and source-linked
   - attach publisher, URL, timestamp, topic, and extracted market entities
   - write curated local evidence to `docs/evidence/curated/news_summary_v1.sample.json`
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/ingest_news_local.py
     python scripts/validate_contracts.py --include-evidence
     ```

4. Add failure routing:
   - invalid input writes to `failed/` or local `docs/evidence/failed/`
   - failures are visible in logs or evidence
   - no invalid record is published
   - failed sample naming: `docs/evidence/failed/bad_<contract_name>.sample.json`
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/validate_contracts.py --include-evidence --check-failures
     ```

5. Add a basic publisher:
   - reads curated energy/news samples
   - writes `dashboard_snapshot_v1.json`
   - keeps only public-safe fields
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/publish_dashboard_snapshot_local.py
     python scripts/validate_contracts.py --include-evidence
     ```

### Week 2 Acceptance Gate

- A full local/demo run produces:
  - energy input JSON
  - news summary JSON
  - curated normalized output
  - dashboard snapshot JSON
- A deliberately bad sample is rejected.
- Failure evidence is visible.
- Dashboard reads approved snapshot JSON only.

### Week 2 Avoid

- perfect classification
- real-time ingestion
- complex orchestration
- advanced dashboard charts before the data contract is stable

## Week 3 - AI Merge

Goal: merge energy and news into traceable, validated insights without allowing malformed AI output downstream.

Status: complete for the deterministic local MVP path.

### Week 3 Outcomes

- Local deterministic AI path reads curated inputs.
- AI output follows `ai_insight_v1.json`.
- Invalid AI output is rejected.
- Valid AI output is converted into dashboard snapshot data.
- Every insight has confidence, risk level, and source references.

### Week 3 Work Items

1. Create the AI input bundle:
   - latest energy facts
   - relevant news summaries
   - explicit expected output schema
   - instruction to cite source IDs/URLs
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/create_ai_input_bundle_local.py
     ```

2. Add AI merge workflow:
   - deterministic local merge first
   - local OpenClaw later if needed
   - deterministic sample fixture for tests/demos
   - no cloud AI until local validation works
   - local deterministic merge check:

     ```bash
     source .venv/bin/activate
     python scripts/merge_ai_insight_local.py
     ```

3. Validate AI output:
   - required fields
   - source reference count
   - confidence score range
   - risk level enum
   - dashboard-safe content only
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/validate_contracts.py --include-evidence
     ```

4. Add quarantine behavior:
   - invalid AI output goes to `failed/`
   - previous good snapshot remains available
   - evidence records explain why output failed

5. Publish valid insights:
   - convert valid `ai_insight_v1.json` into `dashboard_snapshot_v1.json`
   - include insight headline, explanation, confidence, risk, sources, and timestamp

### Week 3 Acceptance Gate

- Valid AI output reaches dashboard snapshot JSON.
- Invalid AI output does not reach dashboard snapshot JSON.
- Every insight has source references and confidence.
- Demo can show both success and failure behavior.

### Optional Cloud AI Path

Only after local validation works:

- Bedrock `InvokeModel`
- or managed compute running OpenClaw

### Week 3 Avoid

- multi-agent orchestration
- fine-tuning
- automated decisions
- publishing raw model text directly

## Week 4 - Dashboard + Portfolio Polish

Goal: turn the project into a clear hiring/demo artifact with visible business value.

Status: in progress.

### Week 4 Early Gate

- Dashboard renders without errors.
- All metrics pull from local snapshot JSON.
- Demo script runs in < 5 minutes.
- README is current and clear.

Current Week 4 progress:

- React dashboard loads and displays `dashboard_snapshot_v1.sample.json`.
- AI insight section is visible in the app.
- Dashboard labels the output as local demo evidence.
- Old data shows a visible freshness warning.
- Demo walkthrough exists at `docs/demo-walkthrough.md`.
- README, this plan, and the walkthrough have been aligned around implemented local MVP vs target AWS extension.

### Week 4 Outcomes

- Dashboard has energy metrics, news-linked insights, and data quality.
- README tells the story cleanly.
- Diagrams are easy to read.
- Screenshots and demo script exist.
- Cost, security, and failure-handling story is explicit.

### Week 4 Work Items

1. Dashboard polish:
   - insight panel
   - market chart or KPI strip
   - news/source references
   - confidence and risk labels
   - data-quality status

2. Demo assets:
   - screenshots
   - sample dashboard JSON
   - demo script
   - latest evidence run
   - local check:

     ```bash
     source .venv/bin/activate
     python scripts/ingest_news_local.py
     python scripts/export_energy_input_local.py
     python scripts/create_ai_input_bundle_local.py
     python scripts/merge_ai_insight_local.py
     python scripts/publish_dashboard_snapshot_local.py
     python scripts/validate_contracts.py --include-evidence --check-failures
     ```

3. Documentation polish:
   - README current scope
   - 4-week plan
   - architecture diagrams
   - setup notes
   - known limitations

4. Security and failure story:
   - public dashboard reads approved JSON only
   - raw, curated, audit, and failed data stay private
   - bad input and bad AI output are quarantined
   - secrets stay in SSM or Secrets Manager

5. Cost story:
   - scheduled/serverless first
   - S3 lifecycle
   - Parquet/partitioning
   - no always-on infrastructure
   - AWS Budget alert before live demo

### Week 4 Acceptance Gate

- Demo walkthrough takes less than 10 minutes.
- The repo explains what is implemented, what is target, and what is stretch.
- Dashboard shows at least one energy/news insight with sources and confidence.
- Failure behavior is documented and demonstrable.
- README, diagrams, and plan agree.

### Week 4 Avoid

- redesigning the whole UI
- adding auth/admin workflows
- building an API service unless the static JSON path is already done
- starting new cloud services for polish alone

## Final Demo Script

Use `docs/demo-walkthrough.md` as the live demo script.

Short version:

1. Explain the business problem: energy market teams need to connect price/demand movement with external news context.
2. Show architecture: private ingestion and lakehouse, public dashboard boundary, AI validation gate.
3. Run or show ingestion evidence: energy input and curated news summaries.
4. Show AI merge: valid insight with confidence and sources, invalid output rejected.
5. Show dashboard: energy metrics, news-linked insight, data quality, freshness warning.
6. Close with cost/security: serverless schedule, no public raw data, no always-on database or VM.

## Weekly Done Checklist

| Week | Done Means |
| --- | --- |
| Week 1 | Complete: structure, schemas, news skeleton, sample snapshot, diagrams |
| Week 2 | Complete: energy/news normalization, validation, failure path, basic publisher |
| Week 3 | Complete: AI merge, schema validation, confidence, source references, reject bad output |
| Week 4 | In progress: dashboard polish, README, screenshots, demo script, cost/security notes |

## Success Criteria

Basic MVP:

- local/demo pipeline works end to end
- dashboard displays approved snapshot JSON
- docs and diagrams are coherent

Strong portfolio project:

- deployed or easily runnable dashboard
- screenshots
- architecture with trust boundaries
- explicit security/cost/failure handling
- source-referenced AI insights

Not required for MVP:

- fully productionized platform
- continuous deployment
- real-time ingestion
- enterprise auth
- high-availability multi-region design
