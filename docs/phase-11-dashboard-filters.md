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

The planning branch used to create this checklist is:

```text
feature/phase11-dashboard-filters-plan
```

## Current State

- Phase 10 dashboard implementation is complete and merged.
- Diagram fidelity follow-up is complete and merged.
- The dashboard has a global filter bar, but the controls are currently
  display/readout controls.
- The React app still reads approved public JSON only:
  `dashboard-data.json` and `dashboard_snapshot_v1.sample.json`.
- Export snapshot works, but it does not yet include selected filter metadata
  or filtered view summaries.

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

Use this order unless a concrete blocker appears:

1. Preserve the public data boundary.
2. Define filter state and URL serialization before UI polishing.
3. Apply filters to portfolio and exception data before market context.
4. Make empty states clear.
5. Update export snapshot after filtered views are deterministic.
6. Capture fresh visual proof only after behavior is stable.

## Implementation Checklist

### 1. Preflight

- [ ] Start from clean `main`.
- [ ] Create `feature/phase11-dashboard-filters`.
- [ ] Run the current React build baseline.
- [ ] Run the current contract validation baseline.
- [ ] Confirm Phase 10 screenshots and docs remain present.
- [ ] Record any pre-existing failures before editing.

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

- [ ] Define a single typed filter state object.
- [ ] Support date range, book, segment, and risk status.
- [ ] Derive default values from the current approved dashboard data.
- [ ] Encode filter state in URL query parameters.
- [ ] Restore filter state on page load.
- [ ] Keep invalid query values from breaking render.

### 3. Data Derivation

- [ ] Filter portfolio books by book, segment, and risk status.
- [ ] Filter exception rows consistently with portfolio selections.
- [ ] Filter market series by selected date range where local data supports it.
- [ ] Recalculate KPI summary values from the filtered subset where possible.
- [ ] Clearly label values that remain whole-portfolio or whole-market context.
- [ ] Add empty-state copy for no matching books or rows.

### 4. UI Behavior

- [ ] Make filter controls interactive.
- [ ] Keep controls keyboard-accessible and mobile-readable.
- [ ] Add clear/reset behavior.
- [ ] Preserve the current Phase 10 visual hierarchy.
- [ ] Avoid layout shifts when filters produce smaller result sets.

### 5. Export Snapshot

- [ ] Include selected filters in the exported JSON bundle.
- [ ] Include generated-at timestamp.
- [ ] Include filtered KPI or row counts where deterministic.
- [ ] Preserve original approved dashboard data references.

### 6. Documentation And Demo

- [ ] Update `README.md` active priorities.
- [ ] Update `PLANS.md` so Phase 11 is active implementation.
- [ ] Update `docs/demo-walkthrough.md` with a short filter demo.
- [ ] Add verification notes to this checklist.
- [ ] Capture fresh desktop and mobile screenshot evidence if the UI changes
  materially.

### 7. Verification

- [ ] Run React build.
- [ ] Run contract validation.
- [ ] Run Markdown lint on touched docs.
- [ ] Run `git diff --check`.
- [ ] Run local dashboard and confirm public JSON endpoints return HTTP 200.
- [ ] Verify URL query filters can be copied, reloaded, and restored.
- [ ] Verify export snapshot includes selected filter metadata.
- [ ] Confirm the React app still does not fetch private lake paths.

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
