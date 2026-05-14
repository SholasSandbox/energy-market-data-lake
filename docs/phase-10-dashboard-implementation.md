# Phase 10: Dashboard Implementation

Use this checklist to turn the post-Phase 8/9 platform into a clearer,
operator-focused React dashboard without expanding the AWS architecture before
the visible product surface is strong.

## Goal

Implement the first dashboard slice from `docs/dashboard-ia-spec.md` so the
project reads as a decision-support product, not only as a well-proven data
pipeline.

The important portfolio hook is:

```text
trusted lakehouse and AI orchestration -> approved public snapshot JSON ->
operator dashboard with KPIs, risk exceptions, market context, and quality state
```

## Branch

```text
feature/phase10-dashboard-implementation
```

The planning branch used to create this checklist is:

```text
feature/phase10-dashboard-implementation-plan
```

## Current State

- Phase 8 AWS AI insight orchestration is implemented and live-proven through
  manual Step Functions execution.
- Phase 9 Terraform import and operating hardening is complete.
- The Phase 8 schedule remains disabled by design.
- The React dashboard can load approved `dashboard_snapshot_v1` sample data.
- The current planning docs still contain some Phase 8/9 "next work" language
  that should be cleaned up during Phase 10.

## Target State

- The React dashboard has a strong `Overview` implementation that matches the
  first implementation slice in `docs/dashboard-ia-spec.md`.
- Public UI state is still driven only by approved dashboard JSON.
- The dashboard makes risk, movement, source references, and data quality easy
  to explain in a short demo.
- Docs describe Phase 8 and Phase 9 as completed foundations, with Phase 10 as
  the active implementation slice.
- Verification evidence proves the dashboard still builds and the JSON
  contracts still pass.

## Scope Boundary

In scope:

- `Overview` page implementation.
- Global filter bar.
- Executive KPI strip with deltas.
- P&L drivers panel.
- Risk coverage panel.
- Exception table.
- Compact market context strip.
- Data freshness and contract status visibility.
- Demo and README updates that reflect the implemented Phase 10 state.

Out of scope:

- Bedrock or OpenClaw cloud model invocation.
- Enabling the Phase 8 EventBridge schedule.
- CloudWatch alarm implementation.
- Production auth, accounts, or admin panels.
- Multi-page dashboard completion beyond light placeholders or navigation.
- New data sources unless needed to support existing approved snapshot fields.

## State Transition

From:

```text
AWS orchestration and Terraform posture are proven, but the React product
surface is still closer to a demo scaffold than an operator dashboard.
```

To:

```text
The dashboard has a focused, build-verified Overview experience that exposes
the lakehouse and AI orchestration value through approved public snapshot data.
```

Smallest useful transition:

```text
Implement the Overview slice using existing snapshot data, prove the build and
contract checks, then update docs and demo material to match.
```

## Prioritized Work

Use this order unless a concrete blocker appears:

1. Preserve the trust boundary: the dashboard must keep reading only approved
   public snapshot data.
2. Implement the `Overview` slice before adding secondary pages or AWS features.
3. Treat diagram drift as documentation risk, not an excuse to expand
   architecture.
4. Keep Bedrock, OpenClaw cloud hosting, schedule enablement, alarms, and
   CloudFront outside Phase 10 unless they become explicit follow-up phases.

## Diagram Fidelity Review

Reviewed on 2026-05-14 against the current Terraform, Lambda handlers, Glue
paths, React app, and Phase 9 closeout notes.

### Faithful Enough For Phase 10

- `diagrams/architecture.mmd` is still faithful as a compact lakehouse diagram:
  EventBridge, ingestion Lambda, raw S3, Glue crawler/catalog, Glue ETL,
  curated S3, Athena, Elexon, ENTSO-E, and ENTSOG are all represented at the
  right level.
- `diagrams/news-dashboard-high-level.mmd` remains directionally faithful for
  the public/private boundary and approved dashboard JSON story.
- `diagrams/news-dashboard-detailed.mmd` remains useful as the target
  trust-boundary diagram: raw, curated, audit, failed, validation, SNS, and
  public dashboard JSON are still the right conceptual boundaries.

### Drift To Account For

- `diagrams/flow_diagram.py` is stale for the current implementation. It names
  ENTSOG as an external source, but the raw and curated zones still describe
  only electricity. The implementation now writes ENTSOG raw gas payloads and
  curated gas Parquet.
- `diagrams/flow_diagram.py` and `diagrams/architecture_overview.py` show daily
  schedule-led ingestion as the normal operating path. Phase 9 deliberately
  leaves both the older ingestion schedule and the Phase 8 orchestration
  schedule disabled.
- `diagrams/architecture_overview.py` is faithful as a lakehouse overview, but
  it does not include Phase 8 Step Functions, SNS, the AI orchestration Lambda,
  failed/audit paths, or the separate dashboard snapshot bucket.
- `diagrams/news-dashboard-detailed.mmd` shows separate energy and news ingest
  Lambdas. The implementation currently uses the existing ingestion Lambda for
  energy and one Phase 8 `news_ai_orchestration` Lambda that dispatches workflow
  actions from Step Functions.
- `diagrams/news-dashboard-detailed.mmd` includes CloudWatch alarms and AWS
  Budget as observability elements. CloudWatch logs and SNS failure notification
  exist in the operating story, but CloudWatch alarms were explicitly deferred
  in Phase 9.
- `diagrams/news-dashboard-detailed.mmd` shows optional Bedrock/OpenClaw cloud
  AI. The implemented Phase 8 path is deterministic; model invocation remains
  deferred.
- The public delivery diagrams say GitHub Pages or CloudFront/S3. The current
  implementation publishes approved snapshot JSON to a dashboard bucket, but
  public website hosting and CloudFront remain deferred.

### Phase 10 Decision

Do not block Phase 10 dashboard implementation on diagram redraws. The diagrams
are faithful enough to preserve the architecture story if they are read with
the target-versus-current distinction above.

Required before Phase 10 closeout:

- [ ] Update or annotate `diagrams/flow_diagram.py` so gas raw and curated paths
  match implementation.
- [ ] Update or annotate `diagrams/architecture_overview.py` as a lakehouse-only
  overview, or extend it to include the Phase 8 orchestration resources.
- [ ] Keep `diagrams/news-dashboard-detailed.mmd` as the target architecture
  unless Phase 10 changes the public snapshot contract.
- [ ] Regenerate rendered diagram assets if any diagram source changes.

## Implementation Checklist

### 1. Preflight

- [ ] Start from clean `main`.
- [ ] Create `feature/phase10-dashboard-implementation`.
- [x] Confirm `dashboard-ui` dependencies install cleanly.
- [x] Run the current contract validation baseline.
- [x] Run the current React build baseline.
- [x] Record any pre-existing failures before editing.

Preflight started on 2026-05-14.

Current boundary:

- Running on `feature/phase10-dashboard-implementation-plan`, not clean `main`.
- Planning docs are still uncommitted:
  - `PLANS.md`
  - `README.md`
  - `docs/phase-10-dashboard-implementation.md`
- Do not start Phase 10 implementation edits until these planning changes are
  committed, merged, or intentionally carried into the implementation branch.

Baseline proof:

- `.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures`
  passed.
- `npm --prefix dashboard-ui ls --depth=0` passed with installed dashboard
  dependencies present.
- `npm --prefix dashboard-ui run build` passed.
- No pre-existing validation or build failures were found.

Commands:

```bash
git switch main
git pull --ff-only
git switch -c feature/phase10-dashboard-implementation

.venv/bin/python scripts/validate_contracts.py \
  --include-evidence \
  --check-failures
npm --prefix dashboard-ui run build
```

### 2. Data And UI Contract Review

- [ ] Inspect `dashboard-ui/public/dashboard_snapshot_v1.sample.json`.
- [ ] Inspect `schemas/dashboard_snapshot_v1.schema.json`.
- [ ] Map available snapshot fields to the Phase 10 UI sections.
- [ ] Decide which values are real, derived, mocked, or placeholder.
- [ ] Avoid adding UI fields that cannot be explained from approved data.

### 3. Overview Layout

- [ ] Make `Overview` the primary implementation target.
- [ ] Add or refine a persistent global header.
- [ ] Add filter controls for date range, book, segment, and risk state if the
  current data can support them.
- [ ] Keep filters deterministic and local to the approved snapshot data.
- [ ] Preserve mobile and desktop readability.

### 4. Executive KPIs

- [ ] Add portfolio gross margin.
- [ ] Add open exposure.
- [ ] Add weighted hedge cover.
- [ ] Add market price versus recent average.
- [ ] Show deltas or directional movement where available.
- [ ] Make stale or incomplete source data visible, not hidden.

### 5. P&L Drivers

- [ ] Add a compact panel for margin drivers by book or segment.
- [ ] Show contribution, movement, and source reference where possible.
- [ ] Keep the chart readable without requiring a hover-only explanation.
- [ ] Highlight negative contribution clearly but sparingly.

### 6. Risk Coverage

- [ ] Add hedge coverage versus target.
- [ ] Add open exposure versus limit.
- [ ] Add breached or watch-state books.
- [ ] Make the risk state easy to explain in one sentence during a demo.

### 7. Exception Table

- [ ] Add a table sorted by risk or margin impact by default.
- [ ] Include book or segment, margin, exposure, hedge cover, risk state, and
  next inspection cue.
- [ ] Add row highlighting for breached or watch-state rows.
- [ ] Keep the table useful with the current sample data volume.

### 8. Market Context Strip

- [ ] Add compact power and gas context.
- [ ] Include latest market movement and relevant news signal.
- [ ] Link market movement to source references from the snapshot where
  possible.
- [ ] Keep this supporting context below the portfolio/risk story.

### 9. Data Quality And Trust

- [ ] Show snapshot timestamp and freshness state.
- [ ] Show contract or validation status.
- [ ] Show source coverage for energy, gas, and news where available.
- [ ] Keep private raw, curated, failed, and audit paths out of the public UI.

### 10. Documentation And Demo

- [ ] Update `README.md` active priorities.
- [ ] Update `PLANS.md` so Phase 10 is the active implementation slice.
- [ ] Update `docs/demo-walkthrough.md` after the UI is implemented.
- [ ] Update `docs/linkedin-project.md` after the demo story is stable.
- [ ] Capture a fresh screenshot under `docs/evidence/screenshots/`.

### 11. Verification

- [ ] Run contract validation.
- [ ] Run TypeScript/build checks.
- [ ] Run the dashboard locally.
- [ ] Capture desktop and mobile visual evidence.
- [ ] Confirm no private lake data is fetched directly by the dashboard.
- [ ] Confirm Phase 8 schedule remains disabled if Terraform is touched.

Commands:

```bash
.venv/bin/python scripts/validate_contracts.py \
  --include-evidence \
  --check-failures
npm --prefix dashboard-ui run build
npm --prefix dashboard-ui run dev
```

## Project Plan

### Slice 1: Baseline And Mapping

Outcome:

- Current UI build state is known.
- Snapshot fields are mapped to the target dashboard sections.
- Gaps are documented before UI work begins.

Proof:

- Contract validation result.
- React build result.
- Short implementation notes in this file or PR description.

### Slice 2: Overview Skeleton

Outcome:

- The `Overview` page has the target layout structure.
- Header, filters, KPI row, panels, table, context strip, and quality area are
  placed without final polish dependency.

Proof:

- React app renders locally.
- No layout overlap on desktop or mobile.

### Slice 3: Data Binding

Outcome:

- Overview sections read from `dashboard_snapshot_v1` data or clearly marked
  derived values.
- Empty, stale, and missing-data states are handled.

Proof:

- Build passes.
- Contract validation passes.
- Snapshot-only public data boundary is preserved.

### Slice 4: Interaction And Polish

Outcome:

- Filters and table sorting make the page easier to inspect.
- Risk states, deltas, and source references are visually legible.
- Mobile layout remains usable.

Proof:

- Browser verification across desktop and mobile viewports.
- Screenshot evidence captured.

### Slice 5: Documentation Closeout

Outcome:

- `README.md`, `PLANS.md`, demo walkthrough, and LinkedIn copy reflect the
  completed Phase 10 implementation.
- Phase 8 and Phase 9 are described as completed foundations, not pending work.

Proof:

- Final build and validation commands pass.
- Demo walkthrough can be completed in under five minutes.

## Failure Path

If the dashboard data is too thin:

- do not invent unsupported production claims
- implement visible empty or limited-evidence states
- document the missing field as a future contract extension

If the UI scope expands:

- keep `Overview` as the only required page
- defer full `Portfolio Risk`, `Market Context`, and `Data Quality` pages

If contract validation fails:

- fix schema or sample data before changing UI assumptions
- do not loosen validation only to make the UI easier

If build or visual checks fail late:

- preserve the working snapshot boundary
- cut polish before cutting trust, validation, or demo clarity

## Definition Of Done

- `Overview` implements the first dashboard slice from
  `docs/dashboard-ia-spec.md`.
- Public dashboard data is still loaded from approved snapshot JSON only.
- Contract validation passes.
- React build passes.
- Desktop and mobile screenshots are captured.
- README, PLANS, and demo docs describe Phase 10 accurately.
- The demo story clearly connects lakehouse data, AI orchestration controls,
  and the visible dashboard decision surface.
