-- Athena / Trino SQL for database: energy_market_lake
-- VS Code note: this file uses Athena syntax, not T-SQL.
-- Run one query block at a time in the Athena console or with start-query-execution.
-- Table from curated crawler: curated_dataset_electricity

-- 1) Daily demand totals (UK)
SELECT
  "date",
  SUM(demand_mw) AS total_demand_mw
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY "date"
ORDER BY "date" DESC
LIMIT 30;

-- 2) Daily average imbalance prices
SELECT
  "date",
  AVG(system_sell_price) AS avg_system_sell_price,
  AVG(system_buy_price) AS avg_system_buy_price
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY "date"
ORDER BY "date" DESC
LIMIT 30;

-- 3) Peak settlement period demand by day
SELECT
  "date",
  MAX(demand_mw) AS peak_demand_mw
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY "date"
ORDER BY "date" DESC
LIMIT 30;

-- 4) Check data completeness (half-hourly points expected ~48/day)
SELECT
  "date",
  COUNT(*) AS settlement_rows
FROM curated_dataset_electricity
WHERE source = 'elexon'
  AND region = 'gb'
GROUP BY "date"
ORDER BY "date" DESC
LIMIT 30;

-- 5) ENTSO-E day-ahead prices by region
SELECT
  region,
  "date",
  AVG(day_ahead_price_eur_mwh) AS avg_day_ahead_price_eur_mwh
FROM curated_dataset_electricity
WHERE source = 'entsoe'
GROUP BY region, "date"
ORDER BY "date" DESC, region
LIMIT 40;

-- Table from curated crawler: curated_dataset_gas

-- 6) ENTSOG gas flow and demand proxy by point direction
SELECT
  "date",
  point_direction,
  point_label,
  direction_key,
  SUM(flow_kwh_d) AS total_flow_kwh_d,
  SUM(demand_kwh_d) AS total_demand_kwh_d
FROM curated_dataset_gas
WHERE source = 'entsog'
  AND region = 'eu'
GROUP BY "date", point_direction, point_label, direction_key
ORDER BY "date" DESC, point_direction
LIMIT 40;

-- 7) ENTSOG freshness and source coverage
SELECT
  source,
  region,
  MAX("date") AS latest_date,
  COUNT(*) AS row_count
FROM curated_dataset_gas
GROUP BY source, region;

-- 8) GB electricity daily operating view
WITH elexon_daily AS (
  SELECT
    "date",
    region,
    COUNT(*) AS settlement_rows,
    SUM(demand_mw) AS total_demand_mw,
    MAX(demand_mw) AS peak_demand_mw,
    AVG(system_sell_price) AS avg_system_sell_price,
    AVG(system_buy_price) AS avg_system_buy_price,
    AVG(net_imbalance_volume) AS avg_net_imbalance_volume
  FROM curated_dataset_electricity
  WHERE source = 'elexon'
    AND region = 'gb'
  GROUP BY "date", region
)
SELECT
  "date",
  region,
  settlement_rows,
  total_demand_mw,
  peak_demand_mw,
  avg_system_sell_price,
  avg_system_buy_price,
  avg_net_imbalance_volume
FROM elexon_daily
ORDER BY "date" DESC
LIMIT 30;

-- 9) ENTSO-E day-ahead electricity price curve by market
SELECT
  "date",
  region,
  AVG(day_ahead_price_eur_mwh) AS avg_day_ahead_price_eur_mwh,
  MIN(day_ahead_price_eur_mwh) AS min_day_ahead_price_eur_mwh,
  MAX(day_ahead_price_eur_mwh) AS max_day_ahead_price_eur_mwh,
  COUNT(*) AS price_periods
FROM curated_dataset_electricity
WHERE source = 'entsoe'
GROUP BY "date", region
ORDER BY "date" DESC, region
LIMIT 80;

-- 10) ENTSOG gas daily network view by point direction
SELECT
  "date",
  point_direction,
  point_label,
  direction_key,
  SUM(COALESCE(flow_kwh_d, 0)) AS total_flow_kwh_d,
  SUM(COALESCE(demand_kwh_d, 0)) AS total_demand_kwh_d,
  MAX(last_update_time_utc) AS latest_source_update_utc,
  COUNT(*) AS source_rows
FROM curated_dataset_gas
WHERE source = 'entsog'
  AND region = 'eu'
GROUP BY "date", point_direction, point_label, direction_key
ORDER BY "date" DESC, point_direction
LIMIT 80;

-- 11) Cross-market daily dashboard feed
WITH electricity AS (
  SELECT
    "date",
    SUM(demand_mw) AS gb_total_demand_mw,
    MAX(demand_mw) AS gb_peak_demand_mw,
    AVG(system_buy_price) AS gb_avg_system_buy_price,
    AVG(system_sell_price) AS gb_avg_system_sell_price
  FROM curated_dataset_electricity
  WHERE source = 'elexon'
    AND region = 'gb'
  GROUP BY "date"
),
gas AS (
  SELECT
    "date",
    SUM(COALESCE(flow_kwh_d, 0)) AS eu_total_flow_kwh_d,
    SUM(COALESCE(demand_kwh_d, 0)) AS eu_total_demand_kwh_d
  FROM curated_dataset_gas
  WHERE source = 'entsog'
    AND region = 'eu'
  GROUP BY "date"
)
SELECT
  electricity."date",
  electricity.gb_total_demand_mw,
  electricity.gb_peak_demand_mw,
  electricity.gb_avg_system_buy_price,
  electricity.gb_avg_system_sell_price,
  gas.eu_total_flow_kwh_d,
  gas.eu_total_demand_kwh_d
FROM electricity
LEFT JOIN gas
  ON electricity."date" = gas."date"
ORDER BY electricity."date" DESC
LIMIT 30;

-- 12) Curated table freshness and coverage
SELECT
  'curated_dataset_electricity' AS table_name,
  source,
  region,
  MAX("date") AS latest_date,
  COUNT(*) AS row_count
FROM curated_dataset_electricity
GROUP BY source, region
UNION ALL
SELECT
  'curated_dataset_gas' AS table_name,
  source,
  region,
  MAX("date") AS latest_date,
  COUNT(*) AS row_count
FROM curated_dataset_gas
GROUP BY source, region
ORDER BY table_name, source, region;

-- 13) Curated null-field quality check
SELECT
  'curated_dataset_electricity' AS table_name,
  source,
  region,
  COUNT(*) AS row_count,
  COUNT_IF("date" IS NULL) AS missing_date_rows,
  COUNT_IF(settlement_period IS NULL) AS missing_settlement_period_rows,
  COUNT_IF(demand_mw IS NULL AND day_ahead_price_eur_mwh IS NULL) AS missing_market_signal_rows
FROM curated_dataset_electricity
GROUP BY source, region
UNION ALL
SELECT
  'curated_dataset_gas' AS table_name,
  source,
  region,
  COUNT(*) AS row_count,
  COUNT_IF("date" IS NULL) AS missing_date_rows,
  COUNT_IF(period_from_utc IS NULL) AS missing_settlement_period_rows,
  COUNT_IF(flow_kwh_d IS NULL AND demand_kwh_d IS NULL) AS missing_market_signal_rows
FROM curated_dataset_gas
GROUP BY source, region
ORDER BY table_name, source, region;
