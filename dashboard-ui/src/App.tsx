import { useEffect, useState } from "react";
import { sampleDashboardData } from "./mockData";
import type {
  AlertItem,
  CoveragePoint,
  DashboardData,
  DashboardSnapshot,
  DashboardSnapshotNewsArticle,
  DriverBar,
  ExposurePoint,
  GasContext,
  GasTrendPoint,
  MarketPanel,
  MarketSeries,
  NavItem,
  QualityCheck,
} from "./types";

const NAV_ITEMS: NavItem[] = ["Energy Overview", "Power", "Gas", "Data Quality"];
const NAV_HASHES: Record<NavItem, string> = {
  "Energy Overview": "overview",
  Power: "power",
  Gas: "gas",
  "Data Quality": "quality",
};

function navFromHash(hash: string): NavItem {
  const normalized = hash.replace(/^#/, "").toLowerCase();
  return NAV_ITEMS.find((item) => NAV_HASHES[item] === normalized) ?? "Energy Overview";
}

function App() {
  const [data, setData] = useState<DashboardData>(sampleDashboardData);
  const [snapshot, setSnapshot] = useState<DashboardSnapshot | null>(null);
  const [sourceLabel, setSourceLabel] = useState("Fallback sample");
  const [snapshotSourceLabel, setSnapshotSourceLabel] = useState("No snapshot");
  const [marketRegion, setMarketRegion] = useState("ALL");
  const [showHeaderNarrative, setShowHeaderNarrative] = useState(false);
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

  function selectNav(item: NavItem) {
    setActiveNav(item);
    window.history.replaceState(null, "", `#${NAV_HASHES[item]}`);
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

  const visibleMarketPanels =
    marketRegion === "ALL"
      ? data.overview.marketPanels
      : data.overview.marketPanels.filter((panel) => panel.region === marketRegion);

  return (
    <div className="page-shell">
      <div className="app-frame">
        <Header
          data={data}
          sourceLabel={sourceLabel}
          activeNav={activeNav}
          onSelectNav={selectNav}
          showHeaderNarrative={showHeaderNarrative}
          onToggleHeaderNarrative={() => setShowHeaderNarrative((current) => !current)}
        />
        <main className="content-grid">
          {activeNav === "Data Quality" ? (
            <DataQualityView checks={data.dataQuality.checks} />
          ) : activeNav === "Power" ? (
            <PowerView
              data={data}
              marketRegion={marketRegion}
              onSelectMarketRegion={setMarketRegion}
              availableMarketRegions={availableMarketRegions}
              visibleMarketPanels={visibleMarketPanels}
            />
          ) : activeNav === "Gas" ? (
            <GasView gasContext={data.overview.gasContext} />
          ) : (
            <EnergyOverviewView
              data={data}
              snapshot={snapshot}
              snapshotSourceLabel={snapshotSourceLabel}
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
  onSelectNav,
  showHeaderNarrative,
  onToggleHeaderNarrative,
}: {
  data: DashboardData;
  sourceLabel: string;
  activeNav: NavItem;
  onSelectNav: (item: NavItem) => void;
  showHeaderNarrative: boolean;
  onToggleHeaderNarrative: () => void;
}) {
  return (
    <header className="topbar">
      <div className="hero-copy">
        <div className="hero-title-row">
          <h1>Energy Market Data Lake &amp; Analytics Platform</h1>
          <button
            type="button"
            className="hero-toggle"
            onClick={onToggleHeaderNarrative}
            aria-expanded={showHeaderNarrative}
          >
            {showHeaderNarrative ? "Hide context" : "Show context"}
          </button>
        </div>
        {showHeaderNarrative ? (
          <p>
            React + TypeScript analytics dashboard wired to generated JSON. Tabs separate power
            portfolio analytics, ENTSOG gas market context, and wider energy news insight so metric
            boundaries stay clear.
          </p>
        ) : null}
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
        <div className="filter-row">
          <button type="button" className="filter-pill">Date Range: 30D</button>
          <button type="button" className="filter-pill">Segment: All</button>
          <button type="button" className="filter-pill">Risk: All</button>
          <button type="button" className="filter-pill">Book: All</button>
        </div>
        <button type="button" className="action-button">Export Snapshot</button>
      </div>
    </header>
  );
}

function EnergyOverviewView({
  data,
  snapshot,
  snapshotSourceLabel,
}: {
  data: DashboardData;
  snapshot: DashboardSnapshot | null;
  snapshotSourceLabel: string;
}) {
  return (
    <>
      <AlertStrip alerts={data.overview.alerts} />
      <ExecutiveSummary cards={data.overview.summaryCards} />
      {data.overview.gasContext?.pointDirections.length ? (
        <GasSummaryPanel gasContext={data.overview.gasContext} />
      ) : null}
      {snapshot ? (
        <AiSnapshotPanel snapshot={snapshot} sourceLabel={snapshotSourceLabel} />
      ) : null}
    </>
  );
}

function PowerView({
  data,
  marketRegion,
  onSelectMarketRegion,
  availableMarketRegions,
  visibleMarketPanels,
}: {
  data: DashboardData;
  marketRegion: string;
  onSelectMarketRegion: (region: string) => void;
  availableMarketRegions: string[];
  visibleMarketPanels: MarketPanel[];
}) {
  return (
    <>
      <PortfolioRiskSection data={data} />
      <ExceptionTable rows={data.overview.exceptionRows} />
      <PowerMarketContextSection
        marketRegion={marketRegion}
        onSelectMarketRegion={onSelectMarketRegion}
        availableMarketRegions={availableMarketRegions}
        visibleMarketPanels={visibleMarketPanels}
      />
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
        note="Cross-energy overview is deliberately compact. Commodity-specific detail lives in the Power and Gas tabs."
        chip="4 decision alerts"
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
        note="These cards describe the current power portfolio and power market snapshot. Gas has its own market-context summary below."
        chip="6 power cards"
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
        note="Compact gas operating context only. Open the Gas tab for the selected pointDirection table."
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

function PortfolioRiskSection({ data }: { data: DashboardData }) {
  return (
    <section className="story-grid">
      <article className="panel panel-primary">
        <SectionHeader
          eyebrow="Power"
          title="Portfolio P&L Drivers"
          note="This is the power portfolio view. It should explain where money is made or lost before any market-context chart."
          chip="8 / 12 columns"
        />
        <PnlDriversChart bars={data.overview.pnlDrivers} />
      </article>

      <div className="side-stack">
        <article className="panel panel-compact">
          <SectionHeader
            eyebrow="Power Risk"
            title="Coverage vs Policy Band"
            note="Hedge-band compliance only. Gas context is not mixed into this portfolio-risk panel."
            chip="4 / 12 columns"
          />
          <CoverageChart points={data.overview.coveragePoints} />
        </article>

        <article className="panel panel-compact">
          <SectionHeader
            eyebrow="Power Risk"
            title="Hedged vs Open Exposure"
            note="Secondary power risk panel. It supports the decision without blending commodity metrics."
            chip="4 / 12 columns"
          />
          <ExposureChart points={data.overview.exposurePoints} />
        </article>
      </div>
    </section>
  );
}

function ExceptionTable({ rows }: { rows: DashboardData["overview"]["exceptionRows"] }) {
  return (
    <section className="panel">
      <SectionHeader
        eyebrow="Power"
        title="Exception-First Investigation Table"
        note="Power portfolio exceptions only. Gas pointDirection completeness and deltas are handled in the Gas tab."
        chip="sticky header"
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
            {rows.map((row) => (
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
            ))}
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
      <div className="market-region-row" role="tablist" aria-label="Power market regions">
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
        {visibleMarketPanels.map((panel) => (
          <MarketPanelCard key={panel.title} panel={panel} />
        ))}
      </div>
    </section>
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
                    className={`pnl-driver-fill ${bar.tone === "loss" ? "pnl-driver-fill-loss" : ""}`}
                    style={{ width: `${widthPct}%` }}
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
        Clicking a row should filter the investigation table below. The impact column is signed so loss-making books show as drag, not zero.
      </div>
    </div>
  );
}

function CoverageChart({ points }: { points: CoveragePoint[] }) {
  return (
    <div className="chart-box">
      <div className="coverage-chart">
        {points.map((point) => (
          <div key={point.label} className="coverage-row">
            <div className="coverage-label">{point.label}</div>
            <div className="coverage-track">
              <div className="coverage-band-marker coverage-band-min" style={{ left: `${point.targetMin}%` }} />
              <div className="coverage-band-marker coverage-band-max" style={{ left: `${point.targetMax}%` }} />
              <div
                className={`coverage-fill ${point.flagged ? "coverage-fill-flagged" : ""}`}
                style={{ width: `${Math.max(point.value, 6)}%` }}
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
  return (
    <div className="chart-box">
      <div className="stack-chart">
        {points.map((point) => (
          <div key={point.label} className="stack-row">
            <div className="stack-label">{point.label}</div>
            <div className="stack-track">
              <div className="stack-hedged" style={{ width: `${point.hedged}%` }} />
              <div className="stack-open" style={{ width: `${point.open}%` }} />
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
                    className={`quality-bar ${
                      value < check.expected ? "quality-bar-gap" : ""
                    }`}
                    style={{ height: `${Math.max((value / Math.max(check.expected, 1)) * 100, 12)}%` }}
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
