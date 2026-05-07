# Athena Gas Query Evidence

- Timestamp (UTC): 2026-05-06T20:28:00Z
- Region: eu-west-2
- Database: energy_market_lake
- Table: curated_dataset_gas
- Query execution ID: c115051a-287d-4257-8c6f-6e3a0f7ddfd1
- Status: SUCCEEDED
- Result date: 2026-05-03
- Rows returned: 4

## Query

```sql
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
  AND date = '2026-05-03'
GROUP BY date, point_direction, point_label, direction_key
ORDER BY point_direction;
```

## Results

- `BE-TSO-0001ITP-00061entry`
  - Point: Zeebrugge IZT
  - Direction: entry
  - Flow: 83770955 kWh/d
  - Demand proxy: 87050208 kWh/d
- `BE-TSO-0001ITP-00115exit`
  - Point: Blaregnies L (BE) / Taisnieres B (FR)
  - Direction: exit
  - Flow: 52820231 kWh/d
  - Demand proxy: 56000000 kWh/d
- `BE-TSO-0001ITP-00555exit`
  - Point: VIP BENE
  - Direction: exit
  - Flow: 287851996 kWh/d
  - Demand proxy: 288990500 kWh/d
- `CZ-TSO-0001ITP-00537entry`
  - Point: VIP Brandov
  - Direction: entry
  - Flow: 264675740 kWh/d
  - Demand proxy: 265642169 kWh/d
