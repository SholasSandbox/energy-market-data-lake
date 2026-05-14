import type {
  AlertItem,
  CoveragePoint,
  DashboardData,
  ExceptionRow,
  ExposurePoint,
  MarketPanel,
  NavItem,
  QualityCheck,
  SummaryCard,
  DriverBar,
} from "./types";

export const navItems: NavItem[] = [
  "Overview",
  "Portfolio Risk",
  "Market Context",
  "Data Quality",
];

export const alerts: AlertItem[] = [
  {
    label: "Books Breaching Limits",
    value: "1",
    detail: "EV Flex is profitable, but margin is still below the policy floor.",
    status: "investigate",
  },
  {
    label: "Loss-Making Books",
    value: "0",
    detail: "No active book is loss-making in the current market snapshot.",
    status: "healthy",
  },
  {
    label: "Settlement Completeness",
    value: "47/48",
    detail: "One period missing. Quality issue is visible without dominating the page.",
    status: "watch",
  },
  {
    label: "Open Exposure Above Limit",
    value: "No",
    detail: "Portfolio open exposure remains comfortably below the 25% control limit.",
    status: "healthy",
  },
];

export const summaryCards: SummaryCard[] = [
  {
    label: "Portfolio Gross Margin",
    value: "£6.65m",
    trend: "4/5 books margin-positive",
    detail: "Margin is concentrated in the fixed and industrial books.",
  },
  {
    label: "Open Exposure",
    value: "13.5%",
    trend: "Policy limit 25%",
    detail: "Open volume remains inside portfolio risk appetite.",
  },
  {
    label: "Weighted Hedge Cover",
    value: "86.5%",
    trend: "Conservative hedge posture",
    detail: "Major books remain heavily forward-covered.",
  },
  {
    label: "Market Price",
    value: "£101.40/MWh",
    trend: "7-day avg £93.80/MWh",
    detail: "Current level is +8.1% versus the trailing 7-day complete-day average.",
  },
  {
    label: "Peak Demand",
    value: "28,990 MW",
    trend: "7-day avg 27,980 MW",
    detail: "Current level is +3.6% versus the trailing 7-day complete-day average.",
  },
  {
    label: "Data Freshness",
    value: "2026-03-10",
    trend: "Latest curated daily snapshot",
    detail: "Dashboard values reflect the latest validated dataset date.",
  },
];

export const driverBars: DriverBar[] = [
  { label: "Residential Fixed South", value: 3_304_697 },
  { label: "SME Indexed Midlands", value: 817_062 },
  { label: "Industrial Flex", value: 1_640_943 },
  { label: "Public Sector Framework", value: 823_779 },
  { label: "EV Flex Portfolio", value: -63_908, tone: "loss" },
];

export const coveragePoints: CoveragePoint[] = [
  { label: "Residential", value: 92, targetMin: 85, targetMax: 97 },
  { label: "SME", value: 84, targetMin: 80, targetMax: 92 },
  { label: "Industrial", value: 90, targetMin: 85, targetMax: 98 },
  { label: "Public Sector", value: 82, targetMin: 80, targetMax: 92 },
  { label: "EV Flex", value: 76, targetMin: 75, targetMax: 90, flagged: false },
];

export const exposurePoints: ExposurePoint[] = [
  { label: "Residential", hedged: 92, open: 8 },
  { label: "SME", hedged: 84, open: 16 },
  { label: "Industrial", hedged: 90, open: 10 },
  { label: "Public Sector", hedged: 82, open: 18 },
];

export const exceptionRows: ExceptionRow[] = [
  {
    book: "EV Flex Portfolio",
    segment: "EV",
    grossMargin: "£0.06m",
    marginPerMwh: "£1.60",
    hedgeCover: "76%",
    targetBand: "75% - 90%",
    openExposure: "24%",
    riskStatus: "breach",
    breachReason: "low-margin",
    tone: "critical",
  },
  {
    book: "SME Indexed Midlands",
    segment: "SME",
    grossMargin: "£0.82m",
    marginPerMwh: "£9.08",
    hedgeCover: "84%",
    targetBand: "80% - 92%",
    openExposure: "16%",
    riskStatus: "watch",
    breachReason: "near margin floor",
    tone: "warning",
  },
  {
    book: "Public Sector Framework",
    segment: "Public Sector",
    grossMargin: "£0.82m",
    marginPerMwh: "£11.77",
    hedgeCover: "82%",
    targetBand: "80% - 92%",
    openExposure: "18%",
    riskStatus: "within limits",
    breachReason: "none",
  },
  {
    book: "Industrial Flex",
    segment: "Industrial",
    grossMargin: "£1.64m",
    marginPerMwh: "£27.35",
    hedgeCover: "90%",
    targetBand: "85% - 98%",
    openExposure: "10%",
    riskStatus: "within limits",
    breachReason: "none",
  },
  {
    book: "Residential Fixed South",
    segment: "Residential",
    grossMargin: "£3.30m",
    marginPerMwh: "£24.48",
    hedgeCover: "92%",
    targetBand: "85% - 97%",
    openExposure: "8%",
    riskStatus: "within limits",
    breachReason: "none",
  },
];

export const marketPanels: MarketPanel[] = [
  {
    title: "GB Elexon Spot vs 7-Day Average",
    legend: [
      { label: "Spot", tone: "teal" },
      { label: "7-Day Avg", tone: "amber" },
    ],
    note: "GB Elexon spot remains above the trailing 7-day complete-day average.",
    region: "GB",
    source: "Elexon",
    series: [
      { label: "Spot", tone: "teal", values: [94, 96, 98, 99, 101, 100, 102, 104, 103, 105, 107, 109] },
      { label: "7-Day Avg", tone: "amber", values: [94, 95, 96, 96.8, 97.6, 98, 98.6, 100, 101, 102, 103.1, 104.3] },
    ],
  },
  {
    title: "GB Elexon Demand Trend",
    legend: [{ label: "Demand", tone: "blue" }],
    note: "GB demand context supports the operating story without leading the page.",
    region: "GB",
    source: "Elexon",
    series: [
      { label: "Demand", tone: "blue", values: [26200, 26750, 26980, 27100, 27440, 27620, 27910, 28220, 28130, 28540, 28810, 28990] },
    ],
  },
  {
    title: "GB Elexon Intraday Profile",
    legend: [
      { label: "Demand", tone: "blue" },
      { label: "Price", tone: "amber" },
    ],
    note: "Latest-day GB intraday demand and system price profile from Elexon.",
    region: "GB",
    source: "Elexon",
    series: [
      { label: "Demand", tone: "blue", values: [21000, 20500, 20200, 20100, 20400, 21200, 22800, 24400, 26100, 27400, 28300, 27900] },
      { label: "Price", tone: "amber", values: [88, 84, 82, 81, 83, 89, 96, 104, 118, 112, 106, 101] },
    ],
  },
  {
    title: "ENTSO-E Regional Day-Ahead Composite",
    legend: [{ label: "Day-Ahead", tone: "teal" }],
    note: "Simple cross-market ENTSO-E composite across continental regions.",
    region: "ALL",
    source: "ENTSO-E",
    series: [
      { label: "Day-Ahead", tone: "teal", values: [91, 95, 99, 102, 98, 96, 97, 100, 104, 108, 106, 103] },
    ],
  },
  {
    title: "ENTSO-E France Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "amber" }],
    note: "French day-ahead prices provide cross-market context for the UK view.",
    region: "FR",
    source: "ENTSO-E",
    series: [
      { label: "Day-Ahead", tone: "amber", values: [84, 86, 88, 91, 90, 89, 87, 92, 95, 97, 96, 94] },
    ],
  },
  {
    title: "ENTSO-E Germany Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "blue" }],
    note: "German price movement is useful directional context for continental power conditions.",
    region: "DE",
    source: "ENTSO-E",
    series: [
      { label: "Day-Ahead", tone: "blue", values: [79, 82, 85, 88, 86, 84, 83, 89, 93, 96, 94, 92] },
    ],
  },
  {
    title: "ENTSO-E Netherlands Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "teal" }],
    note: "Dutch prices round out the regional picture without changing the main page hierarchy.",
    region: "NL",
    source: "ENTSO-E",
    series: [
      { label: "Day-Ahead", tone: "teal", values: [83, 85, 87, 89, 88, 87, 86, 90, 93, 95, 94, 91] },
    ],
  },
];

export const qualityChecks: QualityCheck[] = [
  {
    label: "GB Elexon Settlement Capture",
    source: "Elexon",
    dataset: "Settlement periods",
    region: "GB",
    latestDate: "2026-03-10",
    captured: 47,
    expected: 48,
    status: "watch",
    detail: "One settlement period is missing on the latest GB operating day.",
    series: [48, 48, 48, 48, 47, 48, 48],
  },
  {
    label: "FR ENTSO-E Day-Ahead Intervals",
    source: "ENTSO-E",
    dataset: "Day-ahead price",
    region: "FR",
    latestDate: "2026-03-11",
    captured: 24,
    expected: 24,
    status: "healthy",
    detail: "French day-ahead intervals are complete for the latest delivery date.",
    series: [24, 24, 24, 24, 24, 24, 24],
  },
  {
    label: "DE ENTSO-E Day-Ahead Intervals",
    source: "ENTSO-E",
    dataset: "Day-ahead price",
    region: "DE",
    latestDate: "2026-03-11",
    captured: 24,
    expected: 24,
    status: "healthy",
    detail: "German day-ahead intervals are complete for the latest delivery date.",
    series: [24, 24, 24, 24, 24, 24, 24],
  },
  {
    label: "NL ENTSO-E Day-Ahead Intervals",
    source: "ENTSO-E",
    dataset: "Day-ahead price",
    region: "NL",
    latestDate: "2026-03-11",
    captured: 23,
    expected: 24,
    status: "watch",
    detail: "Dutch day-ahead intervals are almost complete but still missing one hourly point.",
    series: [24, 24, 24, 24, 24, 23, 23],
  },
];

export const sampleDashboardData: DashboardData = {
  metadata: {
    asOf: "2026-03-10 22:40:38 UTC",
    latestDate: "2026-03-10",
    region: "GB",
    scenario: "Conservative Hedge Posture",
    table: "curated_dataset_electricity",
    bucket: "energy-market-lake-464975959576-20260306",
    dataFreshness: "Athena daily snapshot",
  },
  navItems,
  overview: {
    alerts,
    summaryCards,
    pnlDrivers: driverBars,
    coveragePoints,
    exposurePoints,
    exceptionRows,
    marketPanels,
  },
  dataQuality: {
    checks: qualityChecks,
  },
};
