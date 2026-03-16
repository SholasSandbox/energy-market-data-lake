-- Database: energy_market_lake
-- Table from curated crawler: curated_dataset_electricity

-- 1) Daily demand totals (UK)
SELECT
  date,
  SUM(demand_mw) AS total_demand_mw
FROM curated_dataset_electricity
WHERE region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 2) Daily average imbalance prices
SELECT
  date,
  AVG(system_sell_price) AS avg_system_sell_price,
  AVG(system_buy_price) AS avg_system_buy_price
FROM curated_dataset_electricity
WHERE region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 3) Peak settlement period demand by day
SELECT
  date,
  MAX(demand_mw) AS peak_demand_mw
FROM curated_dataset_electricity
WHERE region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;

-- 4) Check data completeness (half-hourly points expected ~48/day)
SELECT
  date,
  COUNT(*) AS settlement_rows
FROM curated_dataset_electricity
WHERE region = 'gb'
GROUP BY date
ORDER BY date DESC
LIMIT 30;
