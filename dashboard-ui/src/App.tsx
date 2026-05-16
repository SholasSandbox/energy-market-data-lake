import { useEffect, useState } from "react";
import { sampleDashboardData } from "./mockData";
import type {
  AlertItem,
  CoveragePoint,
  DashboardData,
  DashboardSnapshot,
  DashboardSnapshotNewsArticle,
  DriverBar,
  ExceptionRow,
  ExposurePoint,
  GasContext,
  GasTrendPoint,
  MarketPanel,
  MarketSeries,
  NavItem,
  QualityCheck,
  SummaryCard,
} from "./types";

const NAV_ITEMS: NavItem[] = ["Overview", "Portfolio Risk", "Market Context", "Data Quality"];
const NAV_HASHES: Record<NavItem, string> = {
  Overview: "overview",
  "Portfolio Risk": "portfolio-risk",
  "Market Context": "market-context",
  "Data Quality": "quality",
};

const DATE_RANGE_OPTIONS = ["7D", "14D", "30D"] as const;
type DateRangeOption = (typeof DATE_RANGE_OPTIONS)[number];
type FilterState = {
  dateRange: DateRangeOption;
  segment: string;
  risk: string;
  book: string;
};

type FilterOptions = {
  segments: string[];
  risks: string[];
  books: string[];
};

type FilterSummary = {
  activeFilterCount: number;
  exceptionRowCount: number;
  pnlDriverCount: number;
  marketPanelCount: number;
};

const DEFAULT_FILTERS: FilterState = {
  dateRange: "30D",
  segment: "ALL",
  risk: "ALL",
  book: "ALL",
};

const DATE_RANGE_POINTS: Record<DateRangeOption, number> = {
  "7D": 7,
  "14D": 14,
  "30D": 30,
};

function navFromHash(hash: string): NavItem {
  const normalized = hash.replace(/^#/, "").toLowerCase();
  return NAV_ITEMS.find((item) => NAV_HASHES[item] === normalized) ?? "Overview";
}

function parseFilterState(search: string): FilterState {
  const params = new URLSearchParams(search);
  const dateRange = DATE_RANGE_OPTIONS.includes(params.get("range") as DateRangeOption)
    ? (params.get("range") as DateRangeOption)
    : DEFAULT_FILTERS.dateRange;

  return {
    dateRange,
    segment: params.get("segment") || DEFAULT_FILTERS.segment,
    risk: params.get("risk") || DEFAULT_FILTERS.risk,
    book: params.get("book") || DEFAULT_FILTERS.book,
  };
}

function filterStateToSearch(filters: FilterState) {
  const params = new URLSearchParams();
  if (filters.dateRange !== DEFAULT_FILTERS.dateRange) params.set("range", filters.dateRange);
  if (filters.segment !== DEFAULT_FILTERS.segment) params.set("segment", filters.segment);
  if (filters.risk !== DEFAULT_FILTERS.risk) params.set("risk", filters.risk);
  if (filters.book !== DEFAULT_FILTERS.book) params.set("book", filters.book);

  const query = params.toString();
  return query ? `?${query}` : "";
}

function writeLocationState(filters: FilterState, activeNav: NavItem) {
  const nextUrl = `${window.location.pathname}${filterStateToSearch(filters)}#${NAV_HASHES[activeNav]}`;
  const currentUrl = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  if (nextUrl !== currentUrl) {
    window.history.replaceState(null, "", nextUrl);
  }
}

function buildFilterOptions(rows: ExceptionRow[]): FilterOptions {
  return {
    segments: uniqueSorted(rows.map((row) => row.segment)),
    risks: uniqueSorted(rows.map((row) => row.riskStatus)),
    books: uniqueSorted(rows.map((row) => row.book)),
  };
}

function uniqueSorted(values: string[]) {
  return Array.from(new Set(values.filter(Boolean))).sort((left, right) => left.localeCompare(right));
}

function buildFilteredDashboardData(data: DashboardData, filters: FilterState): DashboardData {
  const filteredRows = data.overview.exceptionRows.filter((row) => rowMatchesFilters(row, filters));
  const hasPortfolioFilters = filters.book !== "ALL" || filters.segment !== "ALL" || filters.risk !== "ALL";
  const portfolioRows = hasPortfolioFilters ? filteredRows : data.overview.exceptionRows;

  const pnlDrivers = data.overview.pnlDrivers.filter((bar) => portfolioItemMatchesRows(bar.label, portfolioRows));
  const coveragePoints = data.overview.coveragePoints.filter((point) => portfolioItemMatchesRows(point.label, portfolioRows));
  const exposurePoints = data.overview.exposurePoints.filter((point) => portfolioItemMatchesRows(point.label, portfolioRows));
  const marketPanels = data.overview.marketPanels.map((panel) => sliceMarketPanel(panel, filters.dateRange));

  return {
    ...data,
    overview: {
      ...data.overview,
      alerts: buildFilteredAlerts(data.overview.alerts, filteredRows, pnlDrivers),
      summaryCards: buildFilteredSummaryCards(data, filteredRows, pnlDrivers, marketPanels),
      pnlDrivers,
      coveragePoints,
      exposurePoints,
      exceptionRows: filteredRows,
      marketPanels,
    },
  };
}

function buildFilterSummary(_sourceData: DashboardData, filteredData: DashboardData, filters: FilterState): FilterSummary {
  return {
    activeFilterCount: [
      filters.dateRange !== DEFAULT_FILTERS.dateRange,
      filters.segment !== DEFAULT_FILTERS.segment,
      filters.risk !== DEFAULT_FILTERS.risk,
      filters.book !== DEFAULT_FILTERS.book,
    ].filter(Boolean).length,
    exceptionRowCount: filteredData.overview.exceptionRows.length,
    pnlDriverCount: filteredData.overview.pnlDrivers.length,
    marketPanelCount: filteredData.overview.marketPanels.length,
  };
}

function rowMatchesFilters(row: ExceptionRow, filters: FilterState) {
  return (
    (filters.book === "ALL" || row.book === filters.book) &&
    (filters.segment === "ALL" || row.segment === filters.segment) &&
    (filters.risk === "ALL" || row.riskStatus === filters.risk)
  );
}

function portfolioItemMatchesRows(label: string, rows: ExceptionRow[]) {
  if (!rows.length) return false;
  const normalizedLabel = normalizeFilterText(label);
  return rows.some((row) => {
    const normalizedBook = normalizeFilterText(row.book);
    const normalizedSegment = normalizeFilterText(row.segment);
    return (
      normalizedBook === normalizedLabel ||
      normalizedBook.includes(normalizedLabel) ||
      normalizedLabel.includes(normalizedSegment) ||
      normalizedSegment.includes(normalizedLabel)
    );
  });
}

function normalizeFilterText(value: string) {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
}

function sliceMarketPanel(panel: MarketPanel, dateRange: DateRangeOption): MarketPanel {
  const pointCount = DATE_RANGE_POINTS[dateRange];
  return {
    ...panel,
    series: panel.series.map((series) => ({
      ...series,
      values: series.values.slice(-pointCount),
    })),
  };
}

function buildFilteredAlerts(
  sourceAlerts: AlertItem[],
  rows: ExceptionRow[],
  pnlDrivers: DriverBar[],
): AlertItem[] {
  const breachCount = rows.filter((row) => row.riskStatus === "breach").length;
  const lossCount = pnlDrivers.filter((bar) => bar.value < 0).length;
  const maxOpenExposure = rows.reduce((max, row) => Math.max(max, parsePercent(row.openExposure)), 0);

  return sourceAlerts.map((alert) => {
    if (alert.label === "Books Breaching Limits") {
      return {
        ...alert,
        value: String(breachCount),
        detail: breachCount
          ? `${breachCount} filtered book${breachCount === 1 ? "" : "s"} require limit review.`
          : "No filtered books are outside policy limits.",
        status: breachCount ? "investigate" : "healthy",
      };
    }

    if (alert.label === "Loss-Making Books") {
      return {
        ...alert,
        value: String(lossCount),
        detail: lossCount
          ? `${lossCount} filtered book${lossCount === 1 ? "" : "s"} show negative gross margin.`
          : "No filtered books are loss-making.",
        status: lossCount ? "investigate" : "healthy",
      };
    }

    if (alert.label === "Open Exposure Above Limit") {
      const aboveLimit = maxOpenExposure > 25;
      return {
        ...alert,
        value: aboveLimit ? "Yes" : "No",
        detail: aboveLimit
          ? "At least one filtered book exceeds the 25% open exposure control threshold."
          : "Filtered portfolio open exposure remains below the 25% control threshold.",
        status: aboveLimit ? "watch" : "healthy",
      };
    }

    return alert;
  });
}

function buildFilteredSummaryCards(
  data: DashboardData,
  rows: ExceptionRow[],
  pnlDrivers: DriverBar[],
  marketPanels: MarketPanel[],
): SummaryCard[] {
  const totalMargin = pnlDrivers.reduce((sum, bar) => sum + bar.value, 0);
  const positiveMarginCount = pnlDrivers.filter((bar) => bar.value > 0).length;
  const averageOpenExposure = average(rows.map((row) => parsePercent(row.openExposure)));
  const averageHedgeCover = average(rows.map((row) => parsePercent(row.hedgeCover)));
  const priceSeries = findSeries(marketPanels, "price") ?? findSeries(marketPanels, "spot");
  const demandSeries = findPanelByTitle(marketPanels, "demand")?.series[0];

  return data.overview.summaryCards.map((card) => {
    if (card.label === "Portfolio Gross Margin") {
      return {
        ...card,
        value: formatCurrencyMillions(totalMargin),
        trend: `${positiveMarginCount}/${pnlDrivers.length} books margin-positive`,
        detail: pnlDrivers.length
          ? "Gross margin is recalculated from the filtered portfolio subset."
          : "No books match the selected portfolio filters.",
      };
    }

    if (card.label === "Open Exposure") {
      return {
        ...card,
        value: rows.length ? `${averageOpenExposure.toFixed(1)}%` : "n/a",
        detail: rows.length
          ? "Average open exposure is recalculated from filtered exception rows."
          : "No filtered exception rows are available for exposure calculation.",
      };
    }

    if (card.label === "Weighted Hedge Cover") {
      return {
        ...card,
        value: rows.length ? `${averageHedgeCover.toFixed(1)}%` : "n/a",
        detail: rows.length
          ? "Hedge cover is recalculated from filtered exception rows."
          : "No filtered exception rows are available for hedge calculation.",
      };
    }

    if (card.label === "Market Price") {
      return {
        ...card,
        value: priceSeries ? `£${lastValue(priceSeries).toFixed(2)}/MWh` : "n/a",
        trend: priceSeries ? `${priceSeries.values.length}-point avg £${average(priceSeries.values).toFixed(2)}/MWh` : "No price series",
        detail: `Market price context reflects the selected ${dataRangeLabel(data)} and date range filter.`,
      };
    }

    if (card.label === "Peak Demand") {
      return {
        ...card,
        value: demandSeries ? `${Math.round(lastValue(demandSeries)).toLocaleString("en-GB")} MW` : "n/a",
        trend: demandSeries ? `${demandSeries.values.length}-point avg ${Math.round(average(demandSeries.values)).toLocaleString("en-GB")} MW` : "No demand series",
        detail: "Demand context is sliced by the selected date range.",
      };
    }

    return card;
  });
}

function parsePercent(value: string) {
  const parsed = Number.parseFloat(value.replace("%", ""));
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCurrencyMillions(value: number) {
  const prefix = value < 0 ? "-£" : "£";
  return `${prefix}${(Math.abs(value) / 1_000_000).toFixed(2)}m`;
}

function average(values: number[]) {
  const validValues = values.filter((value) => Number.isFinite(value));
  if (!validValues.length) return 0;
  return validValues.reduce((sum, value) => sum + value, 0) / validValues.length;
}

function lastValue(series: MarketSeries) {
  return series.values[series.values.length - 1] ?? 0;
}

function findSeries(panels: MarketPanel[], label: string) {
  const normalizedLabel = label.toLowerCase();
  return panels.flatMap((panel) => panel.series).find((series) => series.label.toLowerCase().includes(normalizedLabel));
}

function findPanelByTitle(panels: MarketPanel[], title: string) {
  const normalizedTitle = title.toLowerCase();
  return panels.find((panel) => panel.title.toLowerCase().includes(normalizedTitle));
}

function dataRangeLabel(data: DashboardData) {
  return data.metadata.region === "ALL" ? "portfolio" : data.metadata.region;
}

function formatFilterLabel(value: string) {
  return value === "ALL" ? "All" : value;
}

function App() {
  const [data, setData] = useState<DashboardData>(sampleDashboardData);
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [sourceLabel, setSourceLabel] = useState("Fallback sample");
  const [snapshotSourceLabel, setSnapshotSourceLabel] = useState("No snapshot");
  const [marketRegion, setMarketRegion] = useState("ALL");
  const [filters, setFilters] = useState<FilterState>(() => parseFilterState(window.location.search));
  const [activeNav, setActiveNav] = useState<NavItem>(() => navFromHash(window.location.hash));

  useEffect(() => {
    let cancelled = false;
    const dashboardDataUrl = `${import.meta.env.BASE_URL}dashboard-data.json`;
    const dashboardSnapshotUrl = `${import.meta.env.BASE_URL}dashboard_snapshot_v1.sample.json`;

    async function loadData() {
      try {
        const response = await fetch(dashboardDataUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = (await response.json()) as DashboardData;
        if (!cancelled) {
          setData(payload);
          setSourceLabel("Generated JSON");
        }
      } catch {
        if (!cancelled) {
          setData(sampleDashboardData);
          setSourceLabel("Fallback sample");
        }
      }
    }

    async function loadSnapshot() {
      try {
        const response = await fetch(dashboardSnapshotUrl, { cache: "no-store" });
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const payload = (await response.json()) as DashboardSnapshot;
        if (!cancelled) {
          setSnapshot(payload);
          setSnapshotSourceLabel("AI snapshot");
        }
      } catch {
        if (!cancelled) {
          setSnapshot(null);
          setSnapshotSourceLabel("No snapshot");
        }
      }
    }

    void loadData();
    void loadSnapshot();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    function handleHashChange() {
      setActiveNav(navFromHash(window.location.hash));
    }

    window.addEventListener("hashchange", handleHashChange);
    return () => window.removeEventListener("hashchange", handleHashChange);
  }, []);

  useEffect(() => {
    function handlePopState() {
      setFilters(parseFilterState(window.location.search));
      setActiveNav(navFromHash(window.location.hash));
    }

    window.addEventListener("popstate", handlePopState);
    return () => window.removeEventListener("popstate", handlePopState);
  }, []);

  useEffect(() => {
    writeLocationState(filters, activeNav);
  }, [filters, activeNav]);

  function selectNav(item: NavItem) {
    setActiveNav(item);
  }

  function exportSnapshot() {
    const filteredData = buildFilteredDashboardData(data, filters);
    const filterSummary = buildFilterSummary(data, filteredData, filters);
    const exportPayload = {
      exported_at: new Date().toISOString(),
      selected_filters: filters,
      filtered_view: filterSummary,
      dashboard_data: filteredData,
      dashboard_snapshot: snapshot,
    };
    const blob = new Blob([JSON.stringify(exportPayload, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `energy-market-dashboard-snapshot-${data.metadata.latestDate}.json`;
    link.click();
    URL.revokeObjectURL(url);
  }

  const availableMarketRegions = [
    "ALL",
    ...Array.from(
      new Set(
        data.overview.marketPanels
          .map((panel) => panel.region)
          .filter((region) => region && region !== "ALL"),
      ),
    ),
  ];

  const filterOptions = buildFilterOptions(data.overview.exceptionRows);
  const filteredData = buildFilteredDashboardData(data, filters);
  const filterSummary = buildFilterSummary(data, filteredData, filters);
  const visibleMarketPanels =
    marketRegion === "ALL"
      ? filteredData.overview.marketPanels
      : filteredData.overview.marketPanels.filter((panel) => panel.region === marketRegion);

  return (
    <div className="page-shell">
      <div className="app-frame">
        <Header
          data={data}
          sourceLabel={sourceLabel}
          activeNav={activeNav}
          filters={filters}
          filterOptions={filterOptions}
          filterSummary={filterSummary}
          onChangeFilters={setFilters}
          onSelectNav={selectNav}
          onExportSnapshot={exportSnapshot}
        />
        <main className="content-grid">
          {activeNav === "Data Quality" ? (
            <DataQualityView checks={data.dataQuality.checks} />
          ) : activeNav === "Portfolio Risk" ? (
            <PortfolioRiskView data={filteredData} />
          ) : activeNav === "Market Context" ? (
            <MarketContextView
              marketRegion={marketRegion}
              onSelectMarketRegion={setMarketRegion}
              availableMarketRegions={availableMarketRegions}
              visibleMarketPanels={visibleMarketPanels}
              gasContext={filteredData.overview.gasContext}
            />
          ) : (
            <OverviewView
              data={filteredData}
              snapshot={snapshot}
              snapshotSourceLabel={snapshotSourceLabel}
              visibleMarketPanels={visibleMarketPanels}
            />
          )}
        </main>
      </div>
    </div>
  );
}

function Header({
  data,
  sourceLabel,
  activeNav,
  filters,
  filterOptions,
  filterSummary,
  onChangeFilters,
  onSelectNav,
  onExportSnapshot,
}: {
  data: DashboardData;
  sourceLabel: string;
  activeNav: NavItem;
  filters: FilterState;
  filterOptions: FilterOptions;
  filterSummary: FilterSummary;
  onChangeFilters: (filters: FilterState) => void;
  onSelectNav: (item: NavItem) => void;
  onExportSnapshot: () => void;
}) {
  function updateFilter(key: keyof FilterState, value: string) {
    onChangeFilters({ ...filters, [key]: value });
  }

  function resetFilters() {
    onChangeFilters(DEFAULT_FILTERS);
  }

  return (
    <header className="topbar">
      <div className="hero-copy">
        <div className="hero-title-row">
          <h1>Energy Market Data Lake &amp; Analytics Platform</h1>
        </div>
        <div className="meta-row">
          <span className="tag">As of {data.metadata.asOf}</span>
          <span className="tag">Latest Date: {data.metadata.latestDate}</span>
          <span className="tag">Portfolio Region: {data.metadata.region}</span>
          <span className="tag">Scenario: {data.metadata.scenario}</span>
          <span className="tag">Data Freshness: {data.metadata.dataFreshness}</span>
          <span className="tag">Source: {sourceLabel}</span>
        </div>
      </div>
      <div className="hero-controls">
        <nav className="nav-row" aria-label="Dashboard sections">
          {NAV_ITEMS.map((item) => (
            <button
              key={item}
              type="button"
              className={`nav-pill ${item === activeNav ? "active" : ""}`}
              onClick={() => onSelectNav(item)}
            >
              {item}
            </button>
          ))}
        </nav>
        <div className="filter-row" aria-label="Dashboard filters">
          <FilterSelect
            label="Date Range"
            value={filters.dateRange}
            options={[...DATE_RANGE_OPTIONS]}
            onChange={(value) => updateFilter("dateRange", value)}
          />
          <FilterSelect
            label="Segment"
            value={filters.segment}
            options={["ALL", ...filterOptions.segments]}
            optionLabel={formatFilterLabel}
            onChange={(value) => updateFilter("segment", value)}
          />
          <FilterSelect
            label="Risk"
            value={filters.risk}
            options={["ALL", ...filterOptions.risks]}
            optionLabel={formatFilterLabel}
            onChange={(value) => updateFilter("risk", value)}
          />
          <FilterSelect
            label="Book"
            value={filters.book}
            options={["ALL", ...filterOptions.books]}
            optionLabel={formatFilterLabel}
            onChange={(value) => updateFilter("book", value)}
          />
          <button type="button" className="filter-reset-button" onClick={resetFilters}>
            Reset
          </button>
        </div>
        <div className="filter-state-line">
          {filterSummary.exceptionRowCount} exception rows / {filterSummary.pnlDriverCount} P&amp;L drivers / {filterSummary.marketPanelCount} market panels
        </div>
        <button type="button" className="action-button" onClick={onExportSnapshot}>
          Export Snapshot
        </button>
      </div>
    </header>
  );
}

function FilterSelect({
  label,
  value,
  options,
  optionLabel = (option) => option,
  onChange,
}: {
  label: string;
  value: string;
  options: string[];
  optionLabel?: (option: string) => string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="filter-select-shell">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {options.map((option) => (
          <option key={`${label}-${option}`} value={option}>
            {optionLabel(option)}
          </option>
        ))}
      </select>
    </label>
  );
}

function OverviewView({
  data,
  snapshot,
  snapshotSourceLabel,
  visibleMarketPanels,
}: {
  data: DashboardData;
  snapshot: DashboardSnapshot | null;
  snapshotSourceLabel: string;
  visibleMarketPanels: MarketPanel[];
}) {
  return (
    <>
      <AlertStrip alerts={data.overview.alerts} />
      <ExecutiveSummary cards={data.overview.summaryCards} />
      <PortfolioRiskSection data={data} />
      <ExceptionTable rows={data.overview.exceptionRows} compact />
      <OverviewMarketContextStrip
        marketPanels={visibleMarketPanels}
        gasContext={data.overview.gasContext}
        snapshot={snapshot}
      />
      {snapshot ? (
        <AiSnapshotPanel snapshot={snapshot} sourceLabel={snapshotSourceLabel} />
      ) : null}
      <DataQualityTrustStrip
        checks={data.dataQuality.checks}
        snapshot={snapshot}
      />
    </>
  );
}

function PortfolioRiskView({ data }: { data: DashboardData }) {
  return (
    <>
      <PortfolioRiskSection data={data} />
      <ExceptionTable rows={data.overview.exceptionRows} />
    </>
  );
}

function MarketContextView({
  marketRegion,
  onSelectMarketRegion,
  availableMarketRegions,
  visibleMarketPanels,
  gasContext,
}: {
  marketRegion: string;
  onSelectMarketRegion: (region: string) => void;
  availableMarketRegions: string[];
  visibleMarketPanels: MarketPanel[];
  gasContext?: GasContext;
}) {
  return (
    <>
      <PowerMarketContextSection
        marketRegion={marketRegion}
        onSelectMarketRegion={onSelectMarketRegion}
        availableMarketRegions={availableMarketRegions}
        visibleMarketPanels={visibleMarketPanels}
      />
      <GasView gasContext={gasContext} />
    </>
  );
}

function GasView({ gasContext }: { gasContext?: GasContext }) {
  if (!gasContext?.pointDirections.length) {
    return (
      <section className="panel gas-panel">
        <SectionHeader
          eyebrow="Gas"
          title="Gas Market Context"
          note="No curated ENTSOG gas context is available in the current dashboard payload."
          chip="No gas rows"
        />
      </section>
    );
  }

  return <GasContextPanel gasContext={gasContext} />;
}

function AlertStrip({ alerts }: { alerts: AlertItem[] }) {
  return (
    <section className="panel">
      <SectionHeader
        eyebrow="Energy Overview"
        title="Alert Strip"
        note="The first screen leads with exceptions so the operator can tell what needs inspection before reading charts."
        chip={`${alerts.length} decision alerts`}
      />
      <div className="alert-grid">
        {alerts.map((alert) => (
          <AlertCard key={alert.label} alert={alert} />
        ))}
      </div>
    </section>
  );
}

function ExecutiveSummary({ cards }: { cards: DashboardData["overview"]["summaryCards"] }) {
  return (
    <section className="panel">
      <SectionHeader
        eyebrow="Power Portfolio"
        title="Executive Summary"
        note="Business-facing KPIs stay above the fold and are backed by generated dashboard JSON, not private lake paths."
        chip={`${cards.length} power cards`}
      />
      <div className="summary-grid">
        {cards.map((card) => (
          <article key={card.label} className="summary-card">
            <div className="label">{card.label}</div>
            <div className="metric">{card.value}</div>
            <div className="trend">{card.trend}</div>
            <p>{card.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

function GasSummaryPanel({ gasContext }: { gasContext: GasContext }) {
  return (
    <section className="panel gas-panel">
      <SectionHeader
        eyebrow="Gas Market Context"
        title="ENTSOG Gas Summary"
        note="Compact gas operating context only. Full pointDirection evidence lives in the Market Context page."
        chip={`${gasContext.pointDirections.length} selected pointDirections`}
      />
      <div className="gas-summary-grid">
        {gasContext.summaryCards.map((card) => (
          <article key={card.label} className="gas-summary-card">
            <div className="label">{card.label}</div>
            <div className="gas-metric">{card.value}</div>
            <div className="trend">{card.trend}</div>
            <p>{card.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

function OverviewMarketContextStrip({
  marketPanels,
  gasContext,
  snapshot,
}: {
  marketPanels: MarketPanel[];
  gasContext?: GasContext;
  snapshot: DashboardSnapshot | null;
}) {
  const selectedPanels = marketPanels.slice(0, 2);
  const primaryInsight = snapshot?.insights[0];
  const newsCount = snapshot?.news_articles?.length ?? 0;
  const gasLatestDate = gasContext?.latestDate ?? "n/a";
  const gasCompleteRows = gasContext?.pointDirections.filter((row) => row.status === "complete").length ?? 0;
  const gasTotalRows = gasContext?.pointDirections.length ?? 0;

  return (
    <section className="panel overview-context-panel">
      <SectionHeader
        eyebrow="Market Context"
        title="Compact Market And News Signals"
        note="Supporting context sits below the risk story and uses only public dashboard payload fields."
        chip={`${selectedPanels.length} power signals / ${gasTotalRows} gas points`}
      />
      <div className="overview-context-grid">
        {selectedPanels.map((panel) => (
          <article key={`overview-${panel.title}`} className="overview-context-card">
            <div className="label">{panel.source}</div>
            <h3>{panel.title}</h3>
            <p>{panel.note}</p>
            <div className="context-sparkline">
              <SparklineChart series={panel.series} />
            </div>
          </article>
        ))}
        <article className="overview-context-card overview-context-card-gas">
          <div className="label">ENTSOG Gas</div>
          <h3>{gasCompleteRows}/{gasTotalRows || "0"} pointDirections complete</h3>
          <p>Latest curated gas evidence date {gasLatestDate}. Gas context supports market interpretation without implying direct book exposure.</p>
        </article>
        <article className="overview-context-card overview-context-card-news">
          <div className="label">Public Snapshot</div>
          <h3>{newsCount} curated news references</h3>
          <p>{primaryInsight?.title ?? "No validated public insight is available in the current snapshot."}</p>
        </article>
      </div>
    </section>
  );
}

function PortfolioRiskSection({ data }: { data: DashboardData }) {
  return (
    <section className="story-grid">
      <article className="panel panel-primary">
        <SectionHeader
          eyebrow="Power"
          title="Portfolio P&L Drivers"
          note="The biggest margin contributors are ranked before market context so the operating story starts with portfolio impact."
          chip={`${data.overview.pnlDrivers.length} drivers`}
        />
        <PnlDriversChart bars={data.overview.pnlDrivers} />
      </article>

      <div className="side-stack">
        <article className="panel panel-compact">
          <SectionHeader
            eyebrow="Power Risk"
          title="Coverage vs Policy Band"
          note="Hedge-band compliance is shown separately from gas market context."
          chip={`${data.overview.coveragePoints.length} books`}
        />
        <CoverageChart points={data.overview.coveragePoints} />
        </article>

        <article className="panel panel-compact">
          <SectionHeader
            eyebrow="Power Risk"
          title="Hedged vs Open Exposure"
          note="Open exposure is kept visible beside hedge coverage so risk is not buried in the table."
            chip={`${data.overview.exposurePoints.length} books`}
          />
          <ExposureChart points={data.overview.exposurePoints} />
        </article>
      </div>
    </section>
  );
}

function ExceptionTable({
  rows,
  compact = false,
}: {
  rows: DashboardData["overview"]["exceptionRows"];
  compact?: boolean;
}) {
  const rankedRows = [...rows].sort((left, right) => {
    const toneRank = { critical: 0, warning: 1, none: 2 };
    const leftRank = toneRank[left.tone ?? "none"];
    const rightRank = toneRank[right.tone ?? "none"];
    return leftRank - rightRank;
  });

  return (
    <section className={`panel ${compact ? "exception-panel-compact" : ""}`}>
      <SectionHeader
        eyebrow="Power"
        title="Exception-First Investigation Table"
        note="Rows are sorted by risk state first so breached and watch books remain visible on the Overview page."
        chip={`${rankedRows.length} rows`}
      />
      <div className="table-shell">
        <table>
          <thead>
            <tr>
              <th>Book</th>
              <th>Segment</th>
              <th>Gross Margin</th>
              <th>Margin / MWh</th>
              <th>Hedge Cover</th>
              <th>Target Band</th>
              <th>Open Exposure</th>
              <th>Risk Status</th>
              <th>Breach Reason</th>
            </tr>
          </thead>
          <tbody>
            {rankedRows.length ? (
              rankedRows.map((row) => (
                <tr key={row.book} className={row.tone ? `row-${row.tone}` : undefined}>
                  <td>{row.book}</td>
                  <td>{row.segment}</td>
                  <td>{row.grossMargin}</td>
                  <td>{row.marginPerMwh}</td>
                  <td>{row.hedgeCover}</td>
                  <td>{row.targetBand}</td>
                  <td>{row.openExposure}</td>
                  <td>{row.riskStatus}</td>
                  <td>{row.breachReason}</td>
                </tr>
              ))
            ) : (
              <tr>
                <td colSpan={9}>
                  <EmptyState
                    title="No exception rows match these filters"
                    detail="The selected filter combination produced an empty public dashboard view."
                  />
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

function PowerMarketContextSection({
  marketRegion,
  onSelectMarketRegion,
  availableMarketRegions,
  visibleMarketPanels,
}: {
  marketRegion: string;
  onSelectMarketRegion: (region: string) => void;
  availableMarketRegions: string[];
  visibleMarketPanels: MarketPanel[];
}) {
  return (
    <section className="panel">
      <SectionHeader
        eyebrow="Power"
        title="Power Market Context"
        note="Electricity-only Elexon and ENTSO-E signals. ENTSOG gas metrics are intentionally absent from these cards."
        chip="context panels"
      />
      <div className="market-region-row" role="group" aria-label="Power market regions">
        {availableMarketRegions.map((region) => (
          <button
            key={region}
            type="button"
            className={`market-region-pill ${marketRegion === region ? "active" : ""}`}
            onClick={() => onSelectMarketRegion(region)}
          >
            {region === "ALL" ? "All Regions" : region}
          </button>
        ))}
      </div>
      <div className="market-grid">
        {visibleMarketPanels.length ? (
          visibleMarketPanels.map((panel) => (
            <MarketPanelCard key={panel.title} panel={panel} />
          ))
        ) : (
          <EmptyState
            title="No market panels match this region"
            detail="Choose All Regions or another available region to restore market context."
          />
        )}
      </div>
    </section>
  );
}

function EmptyState({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}

function SectionHeader({
  eyebrow,
  title,
  note,
  chip,
}: {
  eyebrow: string;
  title: string;
  note: string;
  chip: string;
}) {
  return (
    <div className="section-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h2>{title}</h2>
        <p className="section-note">{note}</p>
      </div>
      <span className="layout-chip">{chip}</span>
    </div>
  );
}

function AiSnapshotPanel({
  snapshot,
  sourceLabel,
}: {
  snapshot: DashboardSnapshot;
  sourceLabel: string;
}) {
  const primaryInsight = snapshot.insights[0];
  const newsArticles = snapshot.news_articles ?? [];
  const freshnessAgeDays = getFreshnessAgeDays(snapshot.metadata.latest_date);
  const isStale = freshnessAgeDays !== null && freshnessAgeDays > 14;

  return (
    <section className="panel snapshot-panel">
      <SectionHeader
        eyebrow="Section AI"
        title="Energy News Insight Snapshot"
        note={`Validated ${snapshot.schema_version} output generated ${snapshot.generated_at}. Power metrics, ENTSOG gas context, and wider energy news are intentionally separated.`}
        chip={`${sourceLabel} / ${snapshot.metadata.status}`}
      />
      {isStale ? (
        <div className="snapshot-freshness-warning">
          <strong>Freshness warning:</strong> latest energy date is {snapshot.metadata.latest_date}
          {freshnessAgeDays !== null ? ` (${freshnessAgeDays} days old)` : ""}. This panel is local
          demo evidence, not a live market snapshot.
        </div>
      ) : null}
      <div className="snapshot-grid">
        <div className="snapshot-card-grid">
          {snapshot.summary_cards.map((card) => (
            <article key={card.label} className={`snapshot-card snapshot-${card.status}`}>
              <div className="label">{card.label}</div>
              <div className="snapshot-value">{card.value}</div>
              <div className="trend">{card.trend}</div>
            </article>
          ))}
        </div>

        {primaryInsight ? (
          <article className={`snapshot-insight insight-${primaryInsight.risk_level}`}>
            <div className="snapshot-insight-top">
              <div>
                <div className="label">Validated Insight</div>
                <h3>{primaryInsight.title}</h3>
              </div>
              <div className="snapshot-badges">
                <span className={`risk-badge risk-${primaryInsight.risk_level}`}>
                  {primaryInsight.risk_level}
                </span>
                <span className="confidence-badge">
                  {(primaryInsight.confidence * 100).toFixed(0)}% confidence
                </span>
              </div>
            </div>
            <p>{primaryInsight.summary}</p>
            <div className="snapshot-source-list">
              {primaryInsight.sources.map((source) => (
                <a
                  key={`${primaryInsight.id}-${source.label}-${source.url}`}
                  href={source.url}
                  target="_blank"
                  rel="noreferrer"
                >
                  {source.label}
                </a>
              ))}
            </div>
          </article>
        ) : null}
      </div>

      {newsArticles.length ? <MarketNewsList articles={newsArticles} /> : null}

      <div className="snapshot-quality-row">
        {snapshot.data_quality.checks.map((check) => (
          <article key={check.label} className={`snapshot-quality snapshot-${check.status}`}>
            <div className="snapshot-quality-title">
              <span>{check.label}</span>
              <span>{check.status}</span>
            </div>
            <p>{check.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

function DataQualityTrustStrip({
  checks,
  snapshot,
}: {
  checks: QualityCheck[];
  snapshot: DashboardSnapshot | null;
}) {
  const watchCount = checks.filter((check) => check.status !== "healthy").length;
  const snapshotStatus = snapshot?.data_quality.status ?? "watch";
  const snapshotChecks = snapshot?.data_quality.checks ?? [];

  return (
    <section className="panel trust-strip-panel">
      <SectionHeader
        eyebrow="Trust"
        title="Data Quality And Public Contract State"
        note="Quality status stays on the first screen so stale or limited evidence cannot hide behind a polished dashboard."
        chip={`${watchCount} quality watches / snapshot ${snapshotStatus}`}
      />
      <div className="trust-strip-grid">
        <article className={`trust-strip-card trust-${watchCount ? "watch" : "ok"}`}>
          <div className="label">Lakehouse Quality</div>
          <div className="trust-strip-value">{checks.length - watchCount}/{checks.length}</div>
          <p>{watchCount ? "Some source intervals need review." : "All displayed source checks are healthy."}</p>
        </article>
        <article className={`trust-strip-card trust-${snapshotStatus}`}>
          <div className="label">Public Snapshot</div>
          <div className="trust-strip-value">{snapshot?.schema_version ?? "missing"}</div>
          <p>{snapshot?.metadata.data_freshness ?? "No approved public snapshot is loaded."}</p>
        </article>
        {snapshotChecks.slice(0, 3).map((check) => (
          <article key={`trust-${check.label}`} className={`trust-strip-card trust-${check.status}`}>
            <div className="label">{check.label}</div>
            <div className="trust-strip-value">{check.status}</div>
            <p>{check.detail}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

function MarketNewsList({ articles }: { articles: DashboardSnapshotNewsArticle[] }) {
  return (
    <div className="market-news-list">
      <div className="market-news-list-header">
        <div>
          <div className="label">Curated Market News</div>
          <h3>Gas And Electricity Movement Context</h3>
        </div>
        <span className="layout-chip">{articles.length} articles</span>
      </div>
      <div className="market-news-grid">
        {articles.map((article) => (
          <article key={`${article.publisher}-${article.url}`} className="market-news-card">
            <div className="market-news-meta">
              <span>{article.publisher}</span>
              <span>{article.published_at.slice(0, 10)}</span>
            </div>
            <a href={article.url} target="_blank" rel="noreferrer">
              {article.title}
            </a>
            <p>{article.summary}</p>
            <div className="market-news-tags">
              {[...article.topics, ...article.regions].slice(0, 4).map((tag) => (
                <span key={`${article.url}-${tag}`}>{tag.replace(/_/g, " ")}</span>
              ))}
            </div>
          </article>
        ))}
      </div>
    </div>
  );
}

function GasContextPanel({ gasContext }: { gasContext: GasContext }) {
  const completeRows = gasContext.pointDirections.filter((row) => row.status === "complete").length;

  return (
    <section className="panel gas-panel">
      <SectionHeader
        eyebrow="Section Gas"
        title="Gas Market Context"
        note="ENTSOG context is shown separately from portfolio P&L. It gives current cross-border gas flow and allocation signals without implying direct book exposure."
        chip={`${completeRows}/${gasContext.pointDirections.length} pointDirections complete`}
      />
      <div className="gas-summary-grid">
        {gasContext.summaryCards.map((card) => (
          <article key={card.label} className="gas-summary-card">
            <div className="label">{card.label}</div>
            <div className="gas-metric">{card.value}</div>
            <div className="trend">{card.trend}</div>
            <p>{card.detail}</p>
          </article>
        ))}
      </div>
      <div className="table-shell gas-table-shell">
        <table>
          <thead>
            <tr>
              <th>Point</th>
              <th>Direction</th>
              <th>Physical Flow</th>
              <th>Allocation Proxy</th>
              <th>Delta</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {gasContext.pointDirections.map((row) => (
              <tr
                key={row.pointDirection}
                className={row.status === "complete" ? "row-healthy" : "row-warning"}
              >
                <td>
                  <div className="gas-point-label">{row.pointLabel}</div>
                  <div className="gas-point-id">{row.pointDirection}</div>
                </td>
                <td>{row.direction}</td>
                <td>{row.flow}</td>
                <td>{row.allocation}</td>
                <td className={row.deltaKwhD >= 0 ? "gas-delta-positive" : "gas-delta-negative"}>
                  {row.delta}
                </td>
                <td>
                  <span className={`status-pill status-${row.status === "complete" ? "healthy" : "watch"}`}>
                    {row.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {gasContext.trendPoints?.length ? (
        <GasTrendSection trendPoints={gasContext.trendPoints} />
      ) : null}
    </section>
  );
}

function GasTrendSection({ trendPoints }: { trendPoints: GasTrendPoint[] }) {
  const latest = trendPoints[trendPoints.length - 1];
  const startDate = trendPoints[0]?.date ?? "";
  const endDate = latest?.date ?? "";

  return (
    <div className="gas-trend-section">
      <div className="gas-trend-heading">
        <div>
          <div className="label">7-Day Trend</div>
          <h3>Gas Flow, Allocation, And Completeness</h3>
        </div>
        <span className="layout-chip">{startDate} to {endDate}</span>
      </div>
      <div className="gas-trend-grid">
        <GasTrendCard
          title="Physical Flow vs Allocation"
          value={latest ? `${latest.flow} / ${latest.allocation}` : "n/a"}
          note="Daily total across the selected ENTSOG pointDirections."
          series={[
            { label: "Flow", tone: "teal", values: trendPoints.map((point) => point.flowKwhD / 1_000_000) },
            {
              label: "Allocation",
              tone: "blue",
              values: trendPoints.map((point) => point.allocationKwhD / 1_000_000),
            },
          ]}
        />
        <GasTrendCard
          title="Allocation Delta"
          value={latest?.delta ?? "n/a"}
          note="Allocation minus physical flow. Positive values indicate allocation above flow."
          series={[
            { label: "Delta", tone: "amber", values: trendPoints.map((point) => point.deltaKwhD / 1_000_000) },
          ]}
        />
        <GasTrendCard
          title="Completeness"
          value={latest?.complete ?? "n/a"}
          note="Selected pointDirections with both flow and allocation rows."
          series={[
            { label: "Completeness", tone: "teal", values: trendPoints.map((point) => point.completenessPct) },
          ]}
        />
      </div>
    </div>
  );
}

function GasTrendCard({
  title,
  value,
  note,
  series,
}: {
  title: string;
  value: string;
  note: string;
  series: MarketSeries[];
}) {
  return (
    <article className="gas-trend-card">
      <div className="section-header section-header-market">
        <div>
          <h3>{title}</h3>
          <p className="market-panel-meta">{value}</p>
        </div>
        <div className="legend">
          {series.map((item) => (
            <span key={item.label} className="legend-item">
              <span className={`dot dot-${item.tone}`} />
              {item.label}
            </span>
          ))}
        </div>
      </div>
      <div className="chart-box chart-box-market">
        <SparklineChart series={series} />
        <div className="chart-caption">{note}</div>
      </div>
    </article>
  );
}

function getFreshnessAgeDays(latestDate: string): number | null {
  const parsed = new Date(`${latestDate}T00:00:00Z`);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  const now = new Date();
  const millisecondsPerDay = 24 * 60 * 60 * 1000;
  return Math.max(0, Math.floor((now.getTime() - parsed.getTime()) / millisecondsPerDay));
}

function AlertCard({ alert }: { alert: AlertItem }) {
  return (
    <article className={`alert-card alert-${alert.status}`}>
      <div className="label">{alert.label}</div>
      <div className="metric">{alert.value}</div>
      <p>{alert.detail}</p>
      <span className={`status-pill status-${alert.status}`}>
        {alert.status === "investigate" ? "Investigate" : alert.status === "watch" ? "Watch" : "Healthy"}
      </span>
    </article>
  );
}

function PnlDriversChart({ bars }: { bars: DriverBar[] }) {
  if (!bars.length) {
    return (
      <div className="chart-box chart-box--primary">
        <EmptyState
          title="No P&L drivers match these filters"
          detail="The approved public payload remains loaded; only the visible portfolio subset is empty."
        />
      </div>
    );
  }

  const rankedBars = [...bars].sort((left, right) => right.value - left.value);
  const maxValue = Math.max(...rankedBars.map((bar) => Math.abs(bar.value)), 1);
  const totalImpact = rankedBars.reduce((sum, bar) => sum + Math.abs(bar.value), 0);

  function formatMillions(value: number) {
    const prefix = value < 0 ? "-£" : "£";
    return `${prefix}${(Math.abs(value) / 1_000_000).toFixed(2)}m`;
  }

  return (
    <div className="chart-box chart-box--primary">
      <div className="pnl-driver-list">
        {rankedBars.map((bar, index) => {
          const widthPct = Math.max((Math.abs(bar.value) / maxValue) * 100, 6);
          const impactPct = totalImpact > 0 ? (bar.value / totalImpact) * 100 : 0;

          return (
            <div key={bar.label} className="pnl-driver-row">
              <div className="pnl-driver-rank">{String(index + 1).padStart(2, "0")}</div>
              <div className="pnl-driver-copy">
                <div className="pnl-driver-title-row">
                  <span className="pnl-driver-label">{bar.label}</span>
                  <span className={`pnl-driver-value ${bar.tone === "loss" ? "is-loss" : ""}`}>
                    {formatMillions(bar.value)}
                  </span>
                </div>
                <div className="pnl-driver-track">
                  <div
                    className={`pnl-driver-fill ${percentStepClass("pct-w", widthPct)} ${
                      bar.tone === "loss" ? "pnl-driver-fill-loss" : ""
                    }`}
                  />
                </div>
              </div>
              <div className="pnl-driver-share">
                <span className={`pnl-driver-share-value ${impactPct < 0 ? "is-negative" : ""}`}>
                  {impactPct.toFixed(1)}%
                </span>
                <span className="pnl-driver-share-label">impact</span>
              </div>
            </div>
          );
        })}
      </div>
      <div className="chart-kicker">
        <span className="chart-kicker-label">Readout</span>
        <span className="chart-kicker-text">
          Ranked by gross margin contribution so the largest books explain portfolio outcome first.
        </span>
      </div>
      <div className="chart-caption">
        The impact column is signed so loss-making books show as drag, not zero.
      </div>
    </div>
  );
}

function CoverageChart({ points }: { points: CoveragePoint[] }) {
  if (!points.length) {
    return (
      <div className="chart-box">
        <EmptyState
          title="No hedge coverage rows"
          detail="Coverage is hidden because the selected portfolio subset has no matching books."
        />
      </div>
    );
  }

  return (
    <div className="chart-box">
      <div className="coverage-chart">
        {points.map((point) => (
          <div key={point.label} className="coverage-row">
            <div className="coverage-label">{point.label}</div>
            <div className="coverage-track">
              <div className={`coverage-band-marker coverage-band-min ${percentStepClass("pct-left", point.targetMin)}`} />
              <div className={`coverage-band-marker coverage-band-max ${percentStepClass("pct-left", point.targetMax)}`} />
              <div
                className={`coverage-fill ${percentStepClass("pct-w", Math.max(point.value, 6))} ${
                  point.flagged ? "coverage-fill-flagged" : ""
                }`}
              />
            </div>
            <div className="coverage-value">{point.value.toFixed(0)}%</div>
          </div>
        ))}
      </div>
      <div className="chart-caption">
        Band markers show minimum and maximum hedge policy. Only true hedge-band breaches are highlighted.
      </div>
    </div>
  );
}

function ExposureChart({ points }: { points: ExposurePoint[] }) {
  if (!points.length) {
    return (
      <div className="chart-box">
        <EmptyState
          title="No exposure rows"
          detail="Exposure is hidden because the selected portfolio subset has no matching books."
        />
      </div>
    );
  }

  return (
    <div className="chart-box">
      <div className="stack-chart">
        {points.map((point) => (
          <div key={point.label} className="stack-row">
            <div className="stack-label">{point.label}</div>
            <div className="stack-track">
              <div className={`stack-hedged ${percentStepClass("pct-w", point.hedged)}`} />
              <div className={`stack-open ${percentStepClass("pct-w", point.open)}`} />
            </div>
          </div>
        ))}
      </div>
      <div className="chart-caption">
        Stacked bars communicate open risk faster than a wide table.
      </div>
    </div>
  );
}

function MarketPanelCard({ panel }: { panel: MarketPanel }) {
  return (
    <article className="panel panel-market">
      <div className="section-header section-header-market">
        <div>
          <h3>{panel.title}</h3>
          <p className="market-panel-meta">
            {panel.region === "ALL" ? "Cross-market view" : `Region ${panel.region}`} · {panel.source}
          </p>
        </div>
        <div className="legend">
          {panel.legend.map((item) => (
            <span key={`${panel.title}-${item.label}`} className="legend-item">
              <span className={`dot dot-${item.tone}`} />
              {item.label}
            </span>
          ))}
        </div>
      </div>
      <div className="chart-box chart-box-market">
        <SparklineChart series={panel.series} />
        <div className="chart-caption">{panel.note}</div>
      </div>
    </article>
  );
}

function DataQualityView({ checks }: { checks: QualityCheck[] }) {
  return (
    <>
      <section className="panel">
        <SectionHeader
          eyebrow="Section Q1"
          title="Data Quality Status"
          note="Show ingestion completeness explicitly by source, dataset, and region. This page should make missing intervals obvious without forcing a table scan."
          chip={`${checks.length} checks / latest status`}
        />
        <div className="quality-legend" aria-label="Data quality legend">
          <span className="quality-legend-item">
            <span className="quality-legend-swatch quality-legend-healthy" />
            Healthy: complete or at expected interval count
          </span>
          <span className="quality-legend-item">
            <span className="quality-legend-swatch quality-legend-watch" />
            Watch: near-complete but missing one or two intervals
          </span>
          <span className="quality-legend-item">
            <span className="quality-legend-swatch quality-legend-investigate" />
            Investigate: materially incomplete capture
          </span>
        </div>
        <div className="quality-grid">
          {checks.map((check) => (
            <article key={check.label} className={`quality-card quality-${check.status}`}>
              <div className="quality-card-top">
                <div>
                  <div className="label">{check.label}</div>
                  <div className="quality-meta">
                    {check.source} · {check.dataset} · {check.region}
                  </div>
                </div>
                <span className={`status-pill status-${check.status}`}>
                  {check.status}
                </span>
              </div>
              <div className="quality-metric">
                {check.captured}/{check.expected}
              </div>
              <div className="trend">Latest date {check.latestDate}</div>
              <p>{check.detail}</p>
              <div className="quality-track">
                {check.series.map((value, index) => (
                  <div
                    key={`${check.label}-${index}`}
                    className={`quality-bar ${percentStepClass(
                      "pct-h",
                      Math.max((value / Math.max(check.expected, 1)) * 100, 12),
                    )} ${
                      value < check.expected ? "quality-bar-gap" : ""
                    }`}
                    title={`${value}/${check.expected}`}
                  />
                ))}
              </div>
            </article>
          ))}
        </div>
      </section>

      <section className="panel">
        <SectionHeader
          eyebrow="Section Q2"
          title="Source Completeness Table"
          note="Keep one sortable, audit-friendly view of what was captured, what was expected, and where the gap is."
          chip="source / dataset / region"
        />
        <div className="table-shell">
          <table>
            <thead>
              <tr>
                <th>Label</th>
                <th>Source</th>
                <th>Dataset</th>
                <th>Region</th>
                <th>Latest Date</th>
                <th>Captured</th>
                <th>Expected</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {checks.map((check) => (
                <tr key={`${check.label}-row`} className={`row-${check.status === "healthy" ? "healthy" : check.status === "watch" ? "warning" : "critical"}`}>
                  <td>{check.label}</td>
                  <td>{check.source}</td>
                  <td>{check.dataset}</td>
                  <td>{check.region}</td>
                  <td>{check.latestDate}</td>
                  <td>{check.captured}</td>
                  <td>{check.expected}</td>
                  <td>{check.status}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

function percentStepClass(prefix: "pct-w" | "pct-left" | "pct-h", value: number) {
  const clamped = Math.min(100, Math.max(0, Math.ceil(value / 5) * 5));
  return `${prefix}-${clamped}`;
}

function SparklineChart({ series }: { series: MarketSeries[] }) {
  const allValues = series.flatMap((item) => item.values);
  const min = Math.min(...allValues);
  const max = Math.max(...allValues);
  const range = max - min || 1;
  const width = 100;
  const height = 100;

  return (
    <svg className="sparkline" viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true">
      {series.map((item) => {
        const points = item.values
          .map((value, index) => {
            const x = item.values.length === 1 ? width / 2 : (index / (item.values.length - 1)) * width;
            const y = height - ((value - min) / range) * 72 - 14;
            return `${x},${y}`;
          })
          .join(" ");

        return (
          <polyline
            key={`${item.label}-${item.tone}`}
            className={`sparkline-path sparkline-${item.tone}`}
            points={points}
          />
        );
      })}
    </svg>
  );
}

export default App;
