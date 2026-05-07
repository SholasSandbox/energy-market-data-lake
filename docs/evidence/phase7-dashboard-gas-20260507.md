# Phase 7 Dashboard Gas Evidence

- Timestamp (UTC): 2026-05-07T10:22:00Z
- Scope: Option B, Gas Context + PointDirection Table, plus rolling 7-day gas trends
- Dashboard data file: `dashboard-ui/public/dashboard-data.json`
- Gas source: Athena table `curated_dataset_gas`
- Electricity source: Athena table resolved from `curated_dataset_electricity`

## Implemented UI

The React dashboard now includes a separate **Gas Market Context** section.

The section intentionally sits outside portfolio P&L so the dashboard does not imply that gas flow is directly part of book margin or hedge exposure.

Implemented elements:

```text
4 gas summary cards
1 selected pointDirection table
3 rolling 7-day gas trend charts
freshness date
flow/demand completeness
flow versus allocation delta
```

## Generated Gas Context

Readback from served `dashboard-data.json`:

```text
electricity latestDate: 2026-05-07
gas latestDate: 2026-05-05
total flow: 784.0 GWh/d
allocation proxy: 790.1 GWh/d
completeness: 4/4
pointDirection rows: 4
completenessStatus: healthy
trendPoints: 7
trendRange: 2026-04-29 to 2026-05-05
```

Gas summary cards:

```text
Gas Data Date: 2026-05-05
Total Flow: 784.0 GWh/d
Allocation Proxy: 790.1 GWh/d
Completeness: 4/4
```

PointDirection rows:

| pointDirection | point label | direction | flow | allocation | delta | status |
| --- | --- | --- | ---: | ---: | ---: | --- |
| `BE-TSO-0001ITP-00061entry` | Zeebrugge IZT | entry | 183.1 GWh/d | 184.9 GWh/d | +1.8 GWh/d | complete |
| `BE-TSO-0001ITP-00115exit` | Blaregnies L (BE) / Taisnieres B (FR) | exit | 58.5 GWh/d | 58.5 GWh/d | +0.0 GWh/d | complete |
| `BE-TSO-0001ITP-00555exit` | VIP BENE | exit | 275.9 GWh/d | 282.4 GWh/d | +6.5 GWh/d | complete |
| `CZ-TSO-0001ITP-00537entry` | VIP Brandov | entry | 266.5 GWh/d | 264.4 GWh/d | -2.1 GWh/d | complete |

Rolling trend readback:

```text
trendPoints: 7
trendRange: 2026-04-29 to 2026-05-05
latestFlow: 784.0 GWh/d
latestAllocation: 790.1 GWh/d
latestDelta: +6.1 GWh/d
latestComplete: 4/4
```

Trend charts:

```text
Physical Flow vs Allocation
Allocation Delta
Completeness
```

## Validation

Commands run:

```text
python3 scripts/generate_dashboard.py --bucket energy-market-lake-464975959576-20260405 --output-json dashboard-ui/public/dashboard-data.json
npm run build
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
```

Results:

```text
dashboard-data.json contains overview.gasContext
npm run build: passed
contract validation: All contracts are valid
bad evidence samples: rejected as expected
```

Local HTTP checks:

```text
GET /: 200
GET /dashboard-data.json: 200
GET /dashboard_snapshot_v1.sample.json: 200
```

Served JSON readback:

```text
overview.gasContext.latestDate: 2026-05-05
overview.gasContext.summaryCards[Total Flow]: 784.0 GWh/d
overview.gasContext.summaryCards[Allocation Proxy]: 790.1 GWh/d
overview.gasContext.summaryCards[Completeness]: 4/4
overview.gasContext.pointDirections: 4
overview.gasContext.completenessStatus: healthy
overview.gasContext.trendPoints: 7
```

## Visual QA

Screenshot artifact:

```text
docs/evidence/screenshots/dashboard-phase7-gas-context-20260507.png
docs/evidence/screenshots/dashboard-energy-overview-tabs-20260507.png
docs/evidence/screenshots/dashboard-power-tab-20260507.png
docs/evidence/screenshots/dashboard-gas-tab-20260507.png
docs/evidence/screenshots/dashboard-gas-tab-7day-trends-20260507.png
```

Result:

```text
Dashboard now uses Energy Overview, Power, Gas, and Data Quality tabs.
Energy Overview shows compact cross-energy status without the gas pointDirection table.
Power tab keeps portfolio risk and Elexon/ENTSO-E electricity charts together.
Gas tab owns ENTSOG gas summary cards and the selected pointDirection table.
Gas tab now includes rolling 7-day gas charts below the pointDirection table.
No visible text overlap was observed in the captured desktop tab screenshots.
```

## Outcome

Phase 7 Option B is implemented and validated.

The dashboard now shows ENTSOG gas context and rolling 7-day ENTSOG trends from curated Athena data without mixing gas into portfolio P&L or public AI snapshot semantics.
