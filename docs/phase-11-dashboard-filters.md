# Phase 11: Deterministic Dashboard Filter Wiring

Use this checklist to turn the Phase 10 dashboard filter bar from display
controls into deterministic local interactions while preserving the public
snapshot trust boundary.

## Goal

Wire date range, book, segment, and risk filters into the React dashboard so
operators can narrow the visible portfolio story without fetching private lake
data or introducing new AWS infrastructure.

The important product hook is:

```text
approved public dashboard JSON -> deterministic filter state -> shareable
operator view -> exportable filtered snapshot
```

## Branch

```text
feature/phase11-dashboard-filters
```

The implementation branch was merged through PR #10. The planning branch used
to create this checklist was:

```text
feature/phase11-dashboard-filters-plan
```

## Current State

- Phase 10 dashboard implementation is complete and merged.
- Diagram fidelity follow-up is complete and merged.
- Target operating model and AWS service companion diagrams are captured in
  `docs/target-operating-model.md`.
- Phase 11 deterministic dashboard filter wiring is complete and merged.
- The dashboard has URL-backed date range, book, segment, and risk filters.
- Filtered portfolio KPIs, P&L drivers, risk panels, exception rows, and market
  date slices are derived locally from the approved dashboard payload.
- The React app still reads approved public JSON only:
  `dashboard-data.json` and `dashboard_snapshot_v1.sample.json`.
- Export snapshot includes selected filter metadata and filtered view summary
  counts.
- Desktop and mobile screenshot evidence is captured under
  `docs/evidence/screenshots/`.

## Target State

- Filter controls update visible dashboard state deterministically.
- Filter state is encoded in the URL query string for shareable views.
- KPIs, P&L drivers, risk panels, exception rows, market context, and export
  snapshot remain consistent with the selected filters.
- Empty or narrowed states are explicit and demoable.
- Build, contract validation, and visual evidence still pass.

## Scope Boundary

In scope:

- Date range filter wired to local market series and visible freshness context.
- Book filter wired to portfolio cards, P&L drivers, risk panels, and exception
  rows.
- Segment filter wired to the same portfolio/risk surfaces.
- Risk status filter wired to alert strip, exception table, and risk panels.
- URL query-string encoding and restore-on-load behavior.
- Export snapshot metadata for selected filters.
- Focused docs and demo updates.

Out of scope:

- Bedrock or OpenClaw model invocation.
- CloudFront/static hosting.
- CloudWatch alarms.
- EventBridge schedule enablement.
- New AWS infrastructure.
- New source ingestion.
- New public snapshot schema unless a gap is documented and approved first.
- Full redesign of secondary pages beyond making them filter-aware.

## State Transition

From:

```text
The Phase 10 dashboard presents a strong Overview surface, but filters are
display-only and cannot narrow the operating view.
```

To:

```text
The dashboard has deterministic local filters that preserve the approved JSON
boundary and produce shareable, exportable filtered views.
```

Smallest useful transition:

```text
Implement URL-backed local filter state, apply it to Overview portfolio and
risk sections first, then prove build, contracts, and visual behavior.
```

## Prioritized Work

Phase 11 is complete. This order is retained as implementation rationale and
future regression guidance:

1. Preserve the public data boundary.
2. Define filter state and URL serialization before UI polishing.
3. Apply filters to portfolio and exception data before market context.
4. Make empty states clear.
5. Update export snapshot after filtered views are deterministic.
6. Capture fresh visual proof only after behavior is stable.

## Implementation Checklist

### 1. Preflight

- [x] Start from clean `main`.
- [x] Create `feature/phase11-dashboard-filters`.
- [x] Run the current React build baseline.
- [x] Run the current contract validation baseline.
- [x] Confirm Phase 10 screenshots and docs remain present.
- [x] Record any pre-existing failures before editing.

Commands:

```bash
git switch main
git pull --ff-only
git switch -c feature/phase11-dashboard-filters

npm --prefix dashboard-ui run build
.venv/bin/python scripts/validate_contracts.py \
  --include-evidence \
  --check-failures
```

### 2. Filter State Model

- [x] Define a single typed filter state object.
- [x] Support date range, book, segment, and risk status.
- [x] Derive default values from the current approved dashboard data.
- [x] Encode filter state in URL query parameters.
- [x] Restore filter state on page load.
- [x] Keep invalid query values from breaking render.

### 3. Data Derivation

- [x] Filter portfolio books by book, segment, and risk status.
- [x] Filter exception rows consistently with portfolio selections.
- [x] Filter market series by selected date range where local data supports it.
- [x] Recalculate KPI summary values from the filtered subset where possible.
- [x] Clearly label values that remain whole-portfolio or whole-market context.
- [x] Add empty-state copy for no matching books or rows.

### 4. UI Behavior

- [x] Make filter controls interactive.
- [x] Keep controls keyboard-accessible and mobile-readable.
- [x] Add clear/reset behavior.
- [x] Preserve the current Phase 10 visual hierarchy.
- [x] Avoid layout shifts when filters produce smaller result sets.

### 5. Export Snapshot

- [x] Include selected filters in the exported JSON bundle.
- [x] Include generated-at timestamp.
- [x] Include filtered KPI or row counts where deterministic.
- [x] Preserve original approved dashboard data references.

### 6. Documentation And Demo

- [x] Update `README.md` active priorities.
- [x] Update `PLANS.md` so Phase 11 is active implementation.
- [x] Update `docs/demo-walkthrough.md` with a short filter demo.
- [x] Add verification notes to this checklist.
- [x] Capture fresh desktop and mobile screenshot evidence if the UI changes
  materially.

### 7. Verification

- [x] Run React build.
- [x] Run contract validation.
- [x] Run Markdown lint on touched docs.
- [x] Run `git diff --check`.
- [x] Run local dashboard and confirm public JSON endpoints return HTTP 200.
- [x] Verify URL query filters can be copied, reloaded, and restored.
- [x] Verify export snapshot includes selected filter metadata.
- [x] Confirm the React app still does not fetch private lake paths.

Status review notes:

- `dashboard-ui/src/App.tsx` defines typed filter state, URL serialization,
  restore-on-load behavior, local data derivation, and export metadata.
- Filter options are derived from public exception rows, not private lake paths.
- Empty states are present for exception rows, P&L drivers, coverage, and
  exposure when a filter combination narrows the view to no matching books.
- Current limitation: filter matching relies on the public snapshot labels and
  exception-row fields already available in `dashboard-data.json`; deeper
  schema changes remain future work.

Verification notes:

- `npm --prefix dashboard-ui run build`
- `.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures`
- `npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md docs/phase-11-dashboard-filters.md`
- `curl -fsS http://127.0.0.1:5173/dashboard-data.json`
- `npx playwright screenshot` for
  `?range=7D&segment=EV&risk=breach&book=EV+Flex+Portfolio#overview`
- Screenshot evidence:
  `docs/evidence/screenshots/dashboard-phase11-filters-desktop-20260516.png`
- Mobile screenshot evidence:
  `docs/evidence/screenshots/dashboard-phase11-filters-mobile-20260516.png`

Commands:

```bash
npm --prefix dashboard-ui run build
.venv/bin/python scripts/validate_contracts.py \
  --include-evidence \
  --check-failures
npx markdownlint-cli2 README.md PLANS.md docs/demo-walkthrough.md \
  docs/phase-11-dashboard-filters.md
git diff --check
```

## Failure Path

If dashboard data is too thin:

- keep the filter but show a clear limited-evidence state
- document the missing field as a future contract extension
- do not invent production claims

If filter semantics are ambiguous:

- prefer narrower behavior that can be explained in one sentence
- label whole-portfolio values instead of recalculating unsupported metrics
- keep URL parameters stable and simple

If mobile layout regresses:

- preserve readability before adding more filter controls
- use compact controls or a stacked filter layout before changing the page
  hierarchy

If contract validation fails:

- fix schema or sample data before changing UI assumptions
- do not loosen validation only to satisfy filter behavior

## Definition Of Done

- Filters are interactive and deterministic.
- Filter state is shareable through the URL.
- Portfolio and exception views reflect selected filters.
- Export snapshot includes selected filter metadata.
- Public dashboard data is still loaded from approved JSON only.
- Contract validation passes.
- React build passes.
- Docs and demo walkthrough describe Phase 11 accurately.
