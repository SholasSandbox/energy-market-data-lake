# Dashboard Information Architecture Spec

Purpose: redesign the current dashboard into a decision-first operator view that can later be implemented in React + TypeScript.

Audience:

- hiring managers reviewing the project
- technical interviewers asking for design rationale
- a future implementation pass in React

## Product Goal

The dashboard should answer four questions quickly:

1. Are we healthy right now?
2. What changed?
3. Where is the risk or loss coming from?
4. What should the operator inspect next?

The current dashboard is visually clean, but it behaves more like a polished report than an operating dashboard. The redesign shifts visual priority toward exceptions, deltas, and investigation paths.

## Primary Navigation

Replace the current two-peer-tab model with intent-based navigation:

- `Overview`
- `Portfolio Risk`
- `Market Context`
- `Data Quality`

Reason:

- `Overview` is the landing page and default demo screen.
- `Portfolio Risk` supports hedging and P&L discussion.
- `Market Context` explains external conditions without competing with portfolio controls.
- `Data Quality` preserves trust without over-weighting technical metrics.

## Page Blueprint

### 1. Global Header

Persistent across all pages.

Contents:

- title: `Energy Market Data Lake & Analytics Platform`
- last refresh timestamp
- market region
- scenario badge, for example `Conservative Hedge Posture`
- global filters: `Date range`, `Book`, `Segment`, `Risk status`
- action button: `Export snapshot`

Behavior:

- filters update charts, KPIs, and tables together
- filters are encoded in the URL query string for shareable views

### 2. Overview

The default page. This is the "five second read" screen.

Section A: Alert strip

- `Books breaching limits`
- `Loss-making books`
- `Settlement completeness`
- `Open exposure above limit`

Rules:

- show red only for true issues
- include one-line explanation under each alert
- if no issues exist, replace with a neutral "All books within limits" state

Section B: Executive KPI row

- `Portfolio Gross Margin`
- `Open Exposure %`
- `Weighted Hedge Cover`
- `Market Price vs 7D Avg`
- `Peak Demand vs 7D Avg`
- `Data freshness`

Each KPI card should show:

- primary value
- delta or comparison
- small trend hint
- status tag such as `stable`, `watch`, `breach`

Section C: Main story grid

Left, large:

- `Portfolio P&L Drivers`
- chart type: sorted bar chart or waterfall

Right, stacked:

- `Risk Position`
- chart type: hedge coverage vs target band
- chart type: hedged vs unhedged volume

Section D: Investigation table

- exception-first table
- default sorting: `breach desc`, `margin asc`, `unhedged exposure desc`

Section E: Compact market context footer

- market price trend
- demand trend
- intraday profile preview

This keeps the overview page useful without letting market charts dominate the first screen.

### 3. Portfolio Risk

Focused on commercial and hedge discussion.

Sections:

- KPI strip: margin, open exposure, hedge cover, breached books
- P&L by book
- hedge coverage vs target
- hedge cost vs market price
- risk table with row highlighting

This page supports interview discussion around hedging posture, loss drivers, and risk policy.

### 4. Market Context

Focused on external market conditions.

Sections:

- price trend over selected range
- demand trend over selected range
- intraday latest profile
- optional regime callout such as `prices elevated vs 7D average`

This page should explain portfolio outcomes, not compete with them.

### 5. Data Quality

Technical trust page.

Sections:

- settlement completeness trend
- freshness indicators
- source coverage checks
- last successful ingestion / ETL / Athena generation timestamps

Keep this page available for credibility, but separate it from the business story.

## Layout Grid

Use a 12-column desktop grid in the React implementation.

Desktop:

- header: 12 columns
- alert strip: 12 columns
- KPI row: 12 columns, 6 cards in a `repeat(6, 1fr)` grid when space allows
- main story:
  - P&L drivers: 8 columns
  - risk stack: 4 columns
- table: 12 columns
- market footer: 12 columns split into 3 equal panels

Tablet:

- alert strip: 2 columns
- KPI cards: 3 columns
- charts: stacked, with one large panel followed by medium panels

Mobile:

- single-column flow
- filters collapse into a sheet or popover
- investigation table becomes card rows with key fields only

## Visual Hierarchy Rules

- only one large hero region above the fold
- one primary chart on the page, not three equal charts
- red reserved for breaches or losses only
- amber reserved for warning states near a threshold
- blue and green used for neutral operational context
- use section subtitles to explain why the chart matters, not only what it is

Example:

- not `Hedge Coverage vs Target Band`
- better: `Coverage remains within policy bands except EV Flex`

## Component Plan For React + TypeScript

Suggested component tree:

- `DashboardLayout`
- `GlobalHeader`
- `GlobalFilterBar`
- `AlertStrip`
- `KpiCard`
- `SectionHeader`
- `PnlDriversPanel`
- `RiskCoveragePanel`
- `OpenExposurePanel`
- `MarketTrendPanel`
- `IntradayProfilePanel`
- `ExceptionTable`
- `QualityPanel`

Suggested page modules:

- `pages/overview`
- `pages/portfolio-risk`
- `pages/market-context`
- `pages/data-quality`

## Typed Data Model

Suggested TypeScript interfaces:

```ts
export interface DashboardSummary {
  asOf: string;
  scenario: string;
  portfolioGrossMargin: number;
  lossBookCount: number;
  breachBookCount: number;
  weightedHedgeCoverPct: number;
  openExposurePct: number;
  settlementCompletenessPct: number;
  marketPrice: number;
  marketPriceVs7dPct: number;
}

export interface PortfolioBook {
  book: string;
  segment: "residential" | "sme" | "industrial" | "public-sector" | "ev";
  volumeMwh: number;
  hedgeCoveragePct: number;
  targetMinPct: number;
  targetMaxPct: number;
  openExposureMwh: number;
  hedgePrice: number;
  marketPrice: number;
  revenue: number;
  grossMargin: number;
  grossMarginPerMwh: number;
  riskStatus: "within-limits" | "warning" | "breach";
  breachReason: string;
}

export interface MarketSeriesPoint {
  date: string;
  totalDemandMwh: number;
  avgSellPrice: number;
  avgBuyPrice: number;
  peakDemandMw: number;
  settlementRows: number;
}
```

## Keep, Merge, Remove

Keep:

- gross margin by book
- hedged vs unhedged volume
- hedge coverage vs target band
- daily demand trend
- daily price trend
- intraday profile

Merge:

- market price and demand should appear together in a compact context band on `Overview`
- quality and freshness should be grouped into one trust section

Remove or demote:

- a full-sized settlement completeness chart on the main landing page
- oversized hero area that does not support action

## Interaction Model

Minimum interaction set:

- change date range
- filter by segment or book
- click chart series to isolate a subset
- click a chart bar to highlight the corresponding row in the table
- sort table by margin, exposure, breach state

Good dashboards are not just visual. They reduce the amount of searching a user has to do.

## First Implementation Slice

To keep scope controlled, implement in this order:

1. `Overview` page only
2. global filter bar
3. executive KPIs with deltas
4. P&L drivers panel
5. risk coverage panel
6. exception table
7. compact market context strip

This first slice is enough to improve usability materially before building the other pages.

## Mapping From Current Dashboard

Current section -> new destination:

- market KPI cards -> `Overview` market context strip
- market charts -> `Market Context`
- utility KPI cards -> `Overview` executive KPIs
- utility P&L and hedge charts -> `Overview` and `Portfolio Risk`
- utility book table -> `Overview` exception table and `Portfolio Risk` full table
- settlement completeness chart -> `Data Quality`

## Why This Will Be Better

- it reduces cognitive load by putting exceptions first
- it aligns layout with operator questions
- it gives the page a clearer scan path
- it creates a natural React component model
- it improves demo quality because the narrative becomes obvious
