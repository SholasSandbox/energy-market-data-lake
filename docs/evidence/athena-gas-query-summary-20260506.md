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

| date | point_direction | point_label | direction | total_flow_kwh_d | total_demand_kwh_d |
| --- | --- | --- | --- | ---: | ---: |
| 2026-05-03 | BE-TSO-0001ITP-00061entry | Zeebrugge IZT | entry | 83770955 | 87050208 |
| 2026-05-03 | BE-TSO-0001ITP-00115exit | Blaregnies L (BE) / Taisnieres B (FR) | exit | 52820231 | 56000000 |
| 2026-05-03 | BE-TSO-0001ITP-00555exit | VIP BENE | exit | 287851996 | 288990500 |
| 2026-05-03 | CZ-TSO-0001ITP-00537entry | VIP Brandov | entry | 264675740 | 265642169 |

