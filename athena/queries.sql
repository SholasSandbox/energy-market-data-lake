-- Database: energy_market_lake
-- Table from curated crawler: curated_dataset_electricity

-- 1) Daily demand totals (UK)
SELECT
  date,
  SUM(demand_mw) AS total_demand_mw
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 2) Daily average imbalance prices
SELECT
  date,
  AVG(system_sell_price) AS avg_system_sell_price,
  AVG(system_buy_price) AS avg_system_buy_price
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 3) Peak settlement period demand by day
SELECT
  date,
  MAX(demand_mw) AS peak_demand_mw
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 4) Check data completeness (half-hourly points expected ~48/day)
SELECT
  date,
  COUNT(*) AS settlement_rows
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 5) ENTSO-E day-ahead prices by region
SELECT
  region,
  date,
  AVG(day_ahead_price_eur_mwh) AS avg_day_ahead_price_eur_mwh
FROM curated_dataset_electricity
WHERE source = 'entsoe'
GROUP BY region, date
ORDER BY date DESC, region
LIMIT 40;

-- Table from curated crawler: curated_dataset_gas

-- 6) ENTSOG gas flow and demand proxy by point direction
SELECT
  date,
  point_direction,
  point_label,
  direction_key,
  SUM(flow_kwh_d) AS total_flow_kwh_d,
  SUM(demand_kwh_d) AS total_demand_kwh_d
FROM curated_dataset_gas
WHERE source = 'entsog'
  AND region = 'eu'
GROUP BY date, point_direction, point_label, direction_key
ORDER BY date DESC, point_direction
LIMIT 40;

-- 7) ENTSOG freshness and source coverage
SELECT
  source,
  region,
  MAX(date) AS latest_date,
  COUNT(*) AS row_count
FROM curated_dataset_gas
GROUP BY source, region;
