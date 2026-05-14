export type NavItem = "Overview" | "Portfolio Risk" | "Market Context" | "Data Quality";
export type DashboardSnapshotStatus = "ok" | "watch" | "error";

export interface AlertItem {
  label: string;
  value: string;
  detail: string;
  status: "healthy" | "watch" | "investigate";
}

export interface SummaryCard {
  label: string;
  value: string;
  trend: string;
  detail: string;
}

export interface GasPointDirection {
  pointDirection: string;
  pointLabel: string;
  direction: string;
  flowKwhD: number;
  allocationKwhD: number;
  deltaKwhD: number;
  flow: string;
  allocation: string;
  delta: string;
  status: "complete" | "incomplete";
}

export interface GasTrendPoint {
  date: string;
  flowKwhD: number;
  allocationKwhD: number;
  deltaKwhD: number;
  completenessPct: number;
  flow: string;
  allocation: string;
  delta: string;
  complete: string;
}

export interface GasContext {
  latestDate: string;
  summaryCards: SummaryCard[];
  pointDirections: GasPointDirection[];
  trendPoints?: GasTrendPoint[];
  completenessStatus: "healthy" | "watch";
}

export interface DriverBar {
  label: string;
  value: number;
  tone?: "default" | "loss";
}

export interface CoveragePoint {
  label: string;
  value: number;
  targetMin: number;
  targetMax: number;
  flagged?: boolean;
}

export interface ExposurePoint {
  label: string;
  hedged: number;
  open: number;
}

export interface ExceptionRow {
  book: string;
  segment: string;
  grossMargin: string;
  marginPerMwh: string;
  hedgeCover: string;
  targetBand: string;
  openExposure: string;
  riskStatus: string;
  breachReason: string;
  tone?: "critical" | "warning";
}

export interface MarketSeries {
  label: string;
  tone: "blue" | "teal" | "amber";
  values: number[];
}

export interface MarketPanel {
  title: string;
  legend: Array<{ label: string; tone: "blue" | "teal" | "amber" }>;
  note: string;
  region: string;
  source: string;
  series: MarketSeries[];
}

export interface QualityCheck {
  label: string;
  source: string;
  dataset: string;
  region: string;
  latestDate: string;
  captured: number;
  expected: number;
  status: "healthy" | "watch" | "investigate";
  detail: string;
  series: number[];
}

export interface DashboardMetadata {
  asOf: string;
  latestDate: string;
  region: string;
  scenario: string;
  table: string;
  bucket: string;
  dataFreshness: string;
}

export interface OverviewData {
  alerts: AlertItem[];
  summaryCards: SummaryCard[];
  gasContext?: GasContext;
  pnlDrivers: DriverBar[];
  coveragePoints: CoveragePoint[];
  exposurePoints: ExposurePoint[];
  exceptionRows: ExceptionRow[];
  marketPanels: MarketPanel[];
}

export interface DashboardData {
  metadata: DashboardMetadata;
  navItems: NavItem[];
  overview: OverviewData;
  dataQuality: {
    checks: QualityCheck[];
  };
}

export interface DashboardSnapshotSummaryCard {
  label: string;
  value: string;
  trend: string;
  status: DashboardSnapshotStatus;
}

export interface DashboardSnapshotSource {
  label: string;
  url: string;
}

export interface DashboardSnapshotInsight {
  id: string;
  title: string;
  summary: string;
  risk_level: "low" | "watch" | "high";
  confidence: number;
  sources: DashboardSnapshotSource[];
}

export interface DashboardSnapshotNewsArticle {
  publisher: string;
  title: string;
  url: string;
  published_at: string;
  summary: string;
  topics: string[];
  regions: string[];
}

export interface DashboardSnapshotQualityCheck {
  label: string;
  status: DashboardSnapshotStatus;
  detail: string;
}

export interface DashboardSnapshot {
  schema_version: "dashboard_snapshot_v1";
  generated_at: string;
  metadata: {
    region: string;
    latest_date: string;
    data_freshness: string;
    status: DashboardSnapshotStatus;
  };
  summary_cards: DashboardSnapshotSummaryCard[];
  insights: DashboardSnapshotInsight[];
  news_articles?: DashboardSnapshotNewsArticle[];
  data_quality: {
    status: DashboardSnapshotStatus;
    checks: DashboardSnapshotQualityCheck[];
  };
}
