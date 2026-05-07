# Expanded Energy News Refresh Evidence

- Date: 2026-05-07
- Scope: expand local RSS evidence for gas and electricity market movement context.
- Output news summary: `docs/evidence/curated/news_summary_v1.sample.json`
- Output dashboard snapshot: `dashboard-ui/public/dashboard_snapshot_v1.sample.json`

## Implementation

The local news ingest now uses a broader default feed set and keeps a balanced mix of gas, power, and wider energy articles.

Default feeds:

```text
Energy Voice
Energy Live News
Power Technology
Offshore Energy
Oilprice.com
Renewable Energy World / Factor This
pv magazine International
```

The dashboard snapshot now includes public-safe `news_articles` alongside the validated summary cards and AI insight. Fields exposed to the dashboard are limited to:

```text
publisher
title
url
published_at
summary
topics
regions
```

## Readback

```text
news_summary_v1 articles: 18
dashboard_snapshot_v1 news_articles: 12
Energy News summary card: 18
```

Topic mix in the dashboard snapshot:

```text
gas_supply: 2
oil_gas: 4
power_prices: 1
power_supply: 6
renewables: 2
policy: 1
```

## Visual Evidence

Screenshot:

```text
docs/evidence/screenshots/dashboard-news-expanded-20260507.png
```

The screenshot confirms:

```text
Energy News count is 18.
Curated Market News renders a Gas And Electricity Movement Context article grid.
The public dashboard shows 12 article cards without exposing raw RSS payloads.
```

## Validation

Commands:

```text
python3 -m py_compile scripts/ingest_news_local.py scripts/publish_dashboard_snapshot_local.py
.venv/bin/python scripts/validate_contracts.py --include-evidence --check-failures
npm run build
git diff --check
```

Result:

```text
All checks passed.
```
