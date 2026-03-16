export type NavItem = "Overview" | "Portfolio Risk" | "Market Context" | "Data Quality";

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
  series: MarketSeries[];
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
}
