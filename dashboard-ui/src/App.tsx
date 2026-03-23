import { useEffect, useState } from "react";
import { sampleDashboardData } from "./mockData";
import type {
  AlertItem,
  CoveragePoint,
  DashboardData,
  DriverBar,
  ExposurePoint,
  MarketPanel,
  MarketSeries,
} from "./types";

function App() {
  const [data, setData] = useState<DashboardData>(sampleDashboardData);
  const [sourceLabel, setSourceLabel] = useState("Fallback sample");
  const [marketRegion, setMarketRegion] = useState("ALL");
  const [showHeaderNarrative, setShowHeaderNarrative] = useState(false);

  useEffect(() => {
    let cancelled = false;
    const dashboardDataUrl = `${import.meta.env.BASE_URL}dashboard-data.json`;

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

    void loadData();

    return () => {
      cancelled = true;
    };
  }, []);

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
          showHeaderNarrative={showHeaderNarrative}
          onToggleHeaderNarrative={() => setShowHeaderNarrative((current) => !current)}
        />
        <main className="content-grid">
          <section className="panel">
            <SectionHeader
              eyebrow="Section A"
              title="Alert Strip"
              note="Show only what needs attention. These cards should be the fastest route to exceptions, not another KPI row."
              chip="12 cols / 4 alerts"
            />
            <div className="alert-grid">
              {data.overview.alerts.map((alert) => (
                <AlertCard key={alert.label} alert={alert} />
              ))}
            </div>
          </section>

          <section className="panel">
            <SectionHeader
              eyebrow="Section B"
              title="Executive Summary"
              note="Every card needs current value, comparison, and a status interpretation. This keeps the summary decision-first rather than decorative."
              chip="12 cols / 6 cards"
            />
            <div className="summary-grid">
              {data.overview.summaryCards.map((card) => (
                <article key={card.label} className="summary-card">
                  <div className="label">{card.label}</div>
                  <div className="metric">{card.value}</div>
                  <div className="trend">{card.trend}</div>
                  <p>{card.detail}</p>
                </article>
              ))}
            </div>
          </section>

          <section className="story-grid">
            <article className="panel panel-primary">
              <SectionHeader
                eyebrow="Section C1"
                title="Portfolio P&L Drivers"
                note="This is the primary chart on the page. The largest visual area should explain where money is made or lost before anything else."
                chip="8 / 12 columns"
              />
              <PnlDriversChart bars={data.overview.pnlDrivers} />
            </article>

            <div className="side-stack">
              <article className="panel panel-compact">
                <SectionHeader
                  eyebrow="Section C2"
                  title="Coverage vs Policy Band"
                  note="This panel is hedge-band compliance only. Margin or P&L exceptions should surface elsewhere."
                  chip="4 / 12 columns"
                />
                <CoverageChart points={data.overview.coveragePoints} />
              </article>

              <article className="panel panel-compact">
                <SectionHeader
                  eyebrow="Section C3"
                  title="Hedged vs Open Exposure"
                  note="Secondary risk panel. Smaller than the P&L story because it supports the decision rather than leading it."
                  chip="4 / 12 columns"
                />
                <ExposureChart points={data.overview.exposurePoints} />
              </article>
            </div>
          </section>

          <section className="panel">
            <SectionHeader
              eyebrow="Section D"
              title="Exception-First Investigation Table"
              note="Default sorting should be breaches first, then weakest margin, then largest unhedged exposure. The table should help confirm what the charts suggested."
              chip="12 cols / sticky header"
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
                  {data.overview.exceptionRows.map((row) => (
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

          <section className="panel">
            <SectionHeader
              eyebrow="Section E"
              title="Market Context Footer"
              note="Keep GB operating signals explicit, then switch into continental ENTSO-E context by region without muddling the portfolio view."
              chip="12 cols / context panels"
            />
            <div className="market-region-row" role="tablist" aria-label="Market context regions">
              {availableMarketRegions.map((region) => (
                <button
                  key={region}
                  type="button"
                  className={`market-region-pill ${marketRegion === region ? "active" : ""}`}
                  onClick={() => setMarketRegion(region)}
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
        </main>
      </div>
    </div>
  );
}

function Header({
  data,
  sourceLabel,
  showHeaderNarrative,
  onToggleHeaderNarrative,
}: {
  data: DashboardData;
  sourceLabel: string;
  showHeaderNarrative: boolean;
  onToggleHeaderNarrative: () => void;
}) {
  return (
    <header className="topbar">
      <div className="hero-copy">
        <div className="hero-title-row">
          <h1>Power Market Data Lake &amp; Analytics Platform</h1>
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
            React + TypeScript overview page wired to generated dashboard JSON. The page is
            arranged to answer health, change, risk source, and next investigation step in one scan.
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
          {data.navItems.map((item) => (
            <button
              key={item}
              type="button"
              className={`nav-pill ${item === "Overview" ? "active" : ""}`}
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
