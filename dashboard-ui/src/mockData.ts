import type {
  AlertItem,
  CoveragePoint,
  DashboardData,
  ExceptionRow,
  ExposurePoint,
  MarketPanel,
  NavItem,
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
    trend: "+4.2% vs prior run",
    detail: "Driven by strong fixed-book hedge carry.",
  },
  {
    label: "Open Exposure",
    value: "13.5%",
    trend: "Below 25% limit",
    detail: "Comfortable headroom against policy ceiling.",
  },
  {
    label: "Weighted Hedge Cover",
    value: "86.5%",
    trend: "Stable vs 7D",
    detail: "Conservative posture retained across major books.",
  },
  {
    label: "Market Price vs 7D Avg",
    value: "+8.1%",
    trend: "Elevated regime",
    detail: "Higher spot prices strengthen hedge value.",
  },
  {
    label: "Peak Demand vs 7D Avg",
    value: "+3.6%",
    trend: "Within seasonal band",
    detail: "Demand is firm but not at a stress level.",
  },
  {
    label: "Data Freshness",
    value: "15m",
    trend: "Ingestion on schedule",
    detail: "Latest snapshot is suitable for portfolio review.",
  },
];

export const driverBars: DriverBar[] = [
  { label: "Residential Fixed South", value: 3_304_697 },
  { label: "SME Indexed Midlands", value: 817_062 },
  { label: "Industrial Flex", value: 1_640_943 },
  { label: "Public Sector Framework", value: 823_779 },
  { label: "EV Flex Portfolio", value: 63_908, tone: "loss" },
];

export const coveragePoints: CoveragePoint[] = [
  { label: "Residential", value: 92, targetMin: 85, targetMax: 97 },
  { label: "SME", value: 84, targetMin: 80, targetMax: 92 },
  { label: "Industrial", value: 90, targetMin: 85, targetMax: 98 },
  { label: "Public Sector", value: 82, targetMin: 80, targetMax: 92 },
  { label: "EV Flex", value: 76, targetMin: 75, targetMax: 90, flagged: true },
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
    title: "Price Trend",
    legend: [
      { label: "Sell", tone: "teal" },
      { label: "Buy", tone: "amber" },
    ],
    note: "Compact context, not the lead visual.",
    series: [
      { label: "Sell", tone: "teal", values: [94, 96, 98, 99, 101, 100, 102, 104, 103, 105, 107, 109] },
      { label: "Buy", tone: "amber", values: [90, 92, 94, 95, 96, 97, 98, 99, 100, 101, 101, 102] },
    ],
  },
  {
    title: "Demand Trend",
    legend: [{ label: "Demand", tone: "blue" }],
    note: "Placed lower because it explains, not decides.",
    series: [
      { label: "Demand", tone: "blue", values: [26200, 26750, 26980, 27100, 27440, 27620, 27910, 28220, 28130, 28540, 28810, 28990] },
    ],
  },
  {
    title: "Intraday Profile Preview",
    legend: [
      { label: "Demand", tone: "blue" },
      { label: "Price", tone: "amber" },
    ],
    note: "Useful for drill-down without taking main-story prominence.",
    series: [
      { label: "Demand", tone: "blue", values: [21000, 20500, 20200, 20100, 20400, 21200, 22800, 24400, 26100, 27400, 28300, 27900] },
      { label: "Price", tone: "amber", values: [88, 84, 82, 81, 83, 89, 96, 104, 118, 112, 106, 101] },
    ],
  },
  {
    title: "ENTSO-E GB Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "teal" }],
    note: "Day-ahead context for GB from the ENTSO-E feed.",
    series: [
      { label: "Day-Ahead", tone: "teal", values: [91, 95, 99, 102, 98, 96, 97, 100, 104, 108, 106, 103] },
    ],
  },
  {
    title: "ENTSO-E France Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "amber" }],
    note: "French day-ahead prices provide cross-market context for the UK view.",
    series: [
      { label: "Day-Ahead", tone: "amber", values: [84, 86, 88, 91, 90, 89, 87, 92, 95, 97, 96, 94] },
    ],
  },
  {
    title: "ENTSO-E Germany Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "blue" }],
    note: "German price movement is useful directional context for continental power conditions.",
    series: [
      { label: "Day-Ahead", tone: "blue", values: [79, 82, 85, 88, 86, 84, 83, 89, 93, 96, 94, 92] },
    ],
  },
  {
    title: "ENTSO-E Netherlands Day-Ahead",
    legend: [{ label: "Day-Ahead", tone: "teal" }],
    note: "Dutch prices round out the regional picture without changing the main page hierarchy.",
    series: [
      { label: "Day-Ahead", tone: "teal", values: [83, 85, 87, 89, 88, 87, 86, 90, 93, 95, 94, 91] },
    ],
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
};
