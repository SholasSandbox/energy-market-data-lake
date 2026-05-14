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

### Diagram Drift Resolved In Follow-Up

- `diagrams/flow_diagram.py` and its rendered PNG now show ENTSOG raw gas
  inputs and curated gas outputs.
- `diagrams/flow_diagram.py` and `diagrams/architecture_overview.py` now show
  schedules as deployed but disabled rather than normal daily execution.
- `diagrams/architecture_overview.py` and its rendered PNG now include the
  Phase 8 manual orchestration path, `news_ai_orchestration`, private
  audit/failed paths, and the public dashboard JSON boundary.
- `diagrams/news-dashboard-detailed.mmd` now reflects the implemented
  `news_ai_orchestration` Lambda dispatch path from Step Functions.
- `diagrams/news-dashboard-detailed.mmd` now shows implemented CloudWatch logs
  and SNS failure notification while keeping CloudWatch alarms and budget
  alerts out of the current-state diagram.
- Bedrock/OpenClaw model invocation and CloudFront/static website hosting are
  now shown as deferred follow-up paths rather than implemented components.

### Phase 10 Decision

Do not block Phase 10 dashboard implementation on diagram redraws. The diagrams
are faithful enough to preserve the architecture story if they are read with
the target-versus-current distinction above.

Required before Phase 10 diagram-fidelity follow-up closeout:

- [x] Update or annotate `diagrams/flow_diagram.py` so gas raw and curated paths
  match implementation.
- [x] Update or annotate `diagrams/architecture_overview.py` as a lakehouse-only
  overview, or extend it to include the Phase 8 orchestration resources.
- [x] Keep `diagrams/news-dashboard-detailed.mmd` as the target architecture
  unless Phase 10 changes the public snapshot contract.
- [x] Regenerate rendered diagram assets if any diagram source changes.

Diagram-fidelity follow-up note:

- `diagrams/flow_diagram.py` and `diagrams/flow_diagram.png` now show ENTSOG
  raw gas inputs and curated gas outputs.
- `diagrams/architecture_overview.py` and `diagrams/architecture_overview.png`
  now include disabled schedules, the Phase 8 manual orchestration path,
  private audit/failed paths, and the public dashboard JSON boundary.
- `diagrams/architecture.mmd` now has a rendered `diagrams/architecture.svg`.
- `diagrams/news-dashboard-high-level.mmd` and
  `diagrams/news-dashboard-detailed.mmd` now reflect the implemented manual
  Step Functions path, deterministic merge boundary, deferred model invocation,
  deferred static hosting, and public snapshot contract.

## Implementation Checklist

### 1. Preflight

- [x] Start from clean `main`.
- [x] Create `feature/phase10-dashboard-implementation`.
- [x] Confirm `dashboard-ui` dependencies install cleanly.
- [x] Run the current contract validation baseline.
- [x] Run the current React build baseline.
- [x] Record any pre-existing failures before editing.

Preflight started on 2026-05-14.

Current boundary:

- Phase 10 planning was committed on
  `feature/phase10-dashboard-implementation-plan`.
- Planning was merged into `main` and pushed to `origin/main`.
- `feature/phase10-dashboard-implementation` was created from clean synced
  `main`.
- Actual Phase 10 implementation edits can now proceed on
  `feature/phase10-dashboard-implementation`.

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

- [x] Inspect `dashboard-ui/public/dashboard_snapshot_v1.sample.json`.
- [x] Inspect `schemas/dashboard_snapshot_v1.schema.json`.
- [x] Map available snapshot fields to the Phase 10 UI sections.
- [x] Decide which values are real, derived, mocked, or placeholder.
- [x] Avoid adding UI fields that cannot be explained from approved data.

Mapping result:

- `dashboard_snapshot_v1` supports public summary cards, validated insight,
  source references, curated news articles, snapshot freshness, and public
  contract checks.
- `dashboard-data.json` supports portfolio KPIs, P&L drivers, hedge coverage,
  open exposure, exception rows, market panels, gas context, and lakehouse data
  quality.
- Phase 10 uses these existing approved JSON payloads. It does not add raw,
  curated, failed, or audit lake paths to the public UI.
- Portfolio values remain demo/sample operating values until a future contract
  extension exposes them through `dashboard_snapshot_v1`.

### 3. Overview Layout

- [x] Make `Overview` the primary implementation target.
- [x] Add or refine a persistent global header.
- [ ] Add filter controls for date range, book, segment, and risk state if the
  current data can support them.
- [x] Keep filters deterministic and local to the approved snapshot data.
- [x] Preserve mobile and desktop readability.

Implementation note:

- Navigation now uses the Phase 10 IA:
  `Overview`, `Portfolio Risk`, `Market Context`, and `Data Quality`.
- The `Overview` page now includes alerts, executive KPIs, P&L drivers, risk
  coverage, exception table, compact market/news context, AI snapshot, and
  trust state.
- `Export Snapshot` now downloads a local JSON bundle containing the current
  dashboard data and approved dashboard snapshot.
- Heading and metric typography has been tightened for smaller viewports.
- Filter controls remain display-only until supported by deterministic local
  filtering.
- Responsive visual proof was captured for desktop, tablet, and mobile on
  2026-05-14.

### 4. Executive KPIs

- [x] Add portfolio gross margin.
- [x] Add open exposure.
- [x] Add weighted hedge cover.
- [x] Add market price versus recent average.
- [x] Show deltas or directional movement where available.
- [x] Make stale or incomplete source data visible, not hidden.

### 5. P&L Drivers

- [x] Add a compact panel for margin drivers by book or segment.
- [x] Show contribution, movement, and source reference where possible.
- [x] Keep the chart readable without requiring a hover-only explanation.
- [x] Highlight negative contribution clearly but sparingly.

### 6. Risk Coverage

- [x] Add hedge coverage versus target.
- [x] Add open exposure versus limit.
- [x] Add breached or watch-state books.
- [x] Make the risk state easy to explain in one sentence during a demo.

### 7. Exception Table

- [x] Add a table sorted by risk or margin impact by default.
- [x] Include book or segment, margin, exposure, hedge cover, risk state, and
  next inspection cue.
- [x] Add row highlighting for breached or watch-state rows.
- [x] Keep the table useful with the current sample data volume.

### 8. Market Context Strip

- [x] Add compact power and gas context.
- [x] Include latest market movement and relevant news signal.
- [x] Link market movement to source references from the snapshot where
  possible.
- [x] Keep this supporting context below the portfolio/risk story.

### 9. Data Quality And Trust

- [x] Show snapshot timestamp and freshness state.
- [x] Show contract or validation status.
- [x] Show source coverage for energy, gas, and news where available.
- [x] Keep private raw, curated, failed, and audit paths out of the public UI.

### 10. Documentation And Demo

- [x] Update `README.md` active priorities.
- [x] Update `PLANS.md` so Phase 10 is the active implementation slice.
- [x] Update `docs/demo-walkthrough.md` after the UI is implemented.
- [x] Update `docs/linkedin-project.md` after the demo story is stable.
- [x] Capture a fresh screenshot under `docs/evidence/screenshots/`.

Documentation closeout note:

- `README.md` now describes the Phase 10 operator-focused `Overview` page and
  links the desktop, tablet, and mobile screenshot evidence.
- `PLANS.md` now marks Phase 10 as active in documentation/demo closeout
  rather than pre-implementation.
- `docs/demo-walkthrough.md` now follows the implemented Phase 10 navigation:
  `Overview`, `Portfolio Risk`, `Market Context`, and `Data Quality`.
- `docs/linkedin-project.md` now includes the Step Functions orchestration and
  operator dashboard story.

### 11. Verification

- [x] Run contract validation.
- [x] Run TypeScript/build checks.
- [x] Run the dashboard locally.
- [x] Capture desktop and mobile visual evidence.
- [x] Confirm no private lake data is fetched directly by the dashboard.
- [x] Confirm Phase 8 schedule remains disabled if Terraform is touched.

Verification note:

- Contract validation passed after the Phase 10 Overview implementation.
- `npm --prefix dashboard-ui run build` passed after the implementation.
- Vite served the app at `http://127.0.0.1:5173/`.
- `dashboard-data.json` and `dashboard_snapshot_v1.sample.json` both returned
  HTTP 200 from the local dev server.
- Responsive screenshot evidence was captured under
  `docs/evidence/screenshots/`:
  `dashboard-phase10-overview-desktop-20260514.png`,
  `dashboard-phase10-overview-tablet-20260514.png`, and
  `dashboard-phase10-overview-mobile-20260514.png`.
- The React app fetches only `dashboard-data.json` and
  `dashboard_snapshot_v1.sample.json`. A private S3 URI remains present as a
  source reference string in the approved snapshot, but it is not fetched by the
  dashboard.
- In-app browser visual verification was not available in this environment
  because the required browser-control tool was not exposed; Playwright CLI
  screenshots were used for visual evidence instead.
- Terraform was not touched.

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
