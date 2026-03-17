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

  useEffect(() => {
    let cancelled = false;

    async function loadData() {
      try {
        const response = await fetch("/dashboard-data.json", { cache: "no-store" });
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

  return (
    <div className="page-shell">
      <div className="app-frame">
        <Header
          data={data}
          sourceLabel={sourceLabel}
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
                  note="Use this to see which book is drifting outside the target hedge range."
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
              note="Market context should support the portfolio narrative. It stays on the page, but below the operating story rather than competing with it above the fold."
              chip="12 cols / 3 compact panels"
            />
            <div className="market-grid">
              {data.overview.marketPanels.map((panel) => (
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
}: {
  data: DashboardData;
  sourceLabel: string;
}) {
  return (
    <header className="topbar">
      <div className="hero-copy">
        <h1>Energy Market Data Lake &amp; Analytics Platform</h1>
        <p>
          React + TypeScript overview page wired to generated dashboard JSON. The page is
          arranged to answer health, change, risk source, and next investigation step in one scan.
        </p>
        <div className="meta-row">
          <span className="tag">As of {data.metadata.asOf}</span>
          <span className="tag">Latest Date: {data.metadata.latestDate}</span>
          <span className="tag">Region: {data.metadata.region}</span>
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
  const maxValue = Math.max(...bars.map((bar) => bar.value), 1);

  return (
    <div className="chart-box chart-box--primary">
      <div className="bar-chart">
        {bars.map((bar) => (
          <div key={bar.label} className="bar-group">
            <div
              className={`bar ${bar.tone === "loss" ? "bar-loss" : ""}`}
              style={{ height: `${Math.max((bar.value / maxValue) * 100, 8)}%` }}
            />
            <span>{bar.label}</span>
          </div>
        ))}
      </div>
      <div className="chart-caption">
        Sorted by contribution. Clicking a bar should filter the investigation table below.
      </div>
    </div>
  );
}

function CoverageChart({ points }: { points: CoveragePoint[] }) {
  return (
    <div className="chart-box">
      <div className="line-band line-band-top" />
      <div className="line-band line-band-mid" />
      <div className="line-band line-band-bottom" />
      <div className="bar-chart coverage-bars">
        {points.map((point) => (
          <div key={point.label} className="bar-group">
            <div
              className={`bar ${point.flagged ? "bar-loss" : ""}`}
              style={{ height: `${Math.max(point.value, 6)}%` }}
            />
            <span>{point.label}</span>
          </div>
        ))}
      </div>
      <div className="chart-caption">
        One exception should be obvious without reading the table.
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
