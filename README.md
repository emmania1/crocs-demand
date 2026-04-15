# Crocs Demand Acceleration Intelligence

A self-updating demand-signal dashboard for CROX (Crocs Inc.) covering Reddit, YouTube, news, stock price, and drop-effect analysis. Generates a single static `index.html` you can open in any browser.

## What it tracks

- **Category demand** — Legacy vs Non-Legacy vs HeyDude, 24-month Reddit trajectory
- **Silhouette heat** — per-silhouette YoY growth, volume, and engagement (31 silhouettes across Classic Clog, Echo, Stomp, Mellow, Pollex, Steven Smith, MSCHF, Balenciaga, Bad Bunny, HeyDude Wally/Wendy, etc.)
- **Drop calendar** — upcoming drops (auto-extracted from news + optional manual info-edge rows)
- **Drop effects** — 160+ past drops analyzed for demand lift and CROX 30d return, broken out by drop category
- **News coverage** — 1,700+ articles across 460+ publishers, topic-classified (release / collab / review / culture / financial)
- **Stock overlay** — CROX daily close + monthly Reddit demand + past drops on one chart

## Data sources

| Source | Script | Auth required |
|---|---|---|
| Reddit (3-yr archive) | `fetch_reddit_arctic.py` | No — uses Arctic Shift public API |
| Google News | `fetch_google_news.py` | No — uses public RSS |
| YouTube Data API | `fetch_youtube.py` | Yes — YouTube API key (free tier) |
| CROX stock price | `fetch_stock_price.py` | No — yfinance |
| Google Trends | `fetch_google_trends.py` | No — pytrends |
| Retail reviews | `fetch_retail_signals.py` | Yes — Google Places API key |
| Amazon / StockX | `fetch_amazon_ranks.py`, `fetch_stockx.py` | Scrapers currently blocked; fallbacks noted in each script |

## Setup

```bash
# 1. Create virtualenv + install deps
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# 2. Copy .env.example to .env and fill in API keys
cp .env.example .env
# Edit .env to add YOUTUBE_API_KEY (required for YouTube fetcher)
# Optionally add GOOGLE_PLACES_API_KEY for retail signals

# 3. Run the full pipeline (refreshes all data + regenerates dashboard)
./run_pipeline.sh
```

Or refresh just one data source:
```bash
./venv/bin/python scripts/fetch_reddit_arctic.py
./venv/bin/python scripts/generate_crocs_dashboard.py  # regen dashboard
```

## Dashboard layout

The rendered `index.html` is organized most-to-least-substantial:

1. **Price vs Demand** — CROX stock + monthly Reddit demand + every past drop on one chart
2. **Category Trend** — Legacy / Non-Legacy / HeyDude 24-month trajectory + YoY tiles
3. **Silhouette Heat** — per-silhouette YoY growth + absolute volume
4. **Drop Calendar** — upcoming drops (manual log + news-extracted) + last 90 days of coverage
5. **Drop Effects** — historical analysis: did drops move the stock? Broken out by category
6. **News & Topics** — cadence, topic mix, publisher diversity
7. **Full Table** — per-silhouette drill-down (hidden behind toggle)

Plus a **Generate Summary** button in the top-right that opens an auto-generated exec brief modal.

## Appending manual drops (info edge)

Edit `config/crocs_releases.csv` to add rows for upcoming drops you've heard about from expert calls, industry chatter, or private research. These show up as "manual" (highest-trust) badges in the Drop Calendar.

```csv
release_date,brand,silhouette_key,display_name,collab_partner,source_type,source_note,confidence,added_date
2026-06-15,Crocs,pollex_pod,Pollex Pod Summer,Salehe Bembury,expert_call,"notes here",confirmed,2026-04-15
```

Then regenerate: `./venv/bin/python scripts/generate_crocs_dashboard.py`

## Project structure

```
crocs_demand/
├── config/
│   ├── silhouettes.csv         # 31-silhouette universe
│   └── crocs_releases.csv      # manual upcoming-drops log (user-maintained)
├── scripts/
│   ├── generate_crocs_dashboard.py   # builds index.html
│   ├── fetch_reddit_arctic.py        # 3yr Reddit history
│   ├── fetch_google_news.py          # whole-internet news
│   ├── fetch_youtube.py              # YouTube Data API
│   ├── fetch_stock_price.py          # CROX daily close
│   ├── fetch_retail_signals.py       # Google Maps store reviews
│   ├── fetch_google_trends.py        # search-trend spikes
│   └── fetch_amazon_ranks.py, fetch_stockx.py, fetch_sneaker_news.py
├── data/                       # fetcher output CSVs
├── index.html                  # the dashboard (open in browser)
├── run_pipeline.sh             # refresh everything in one command
├── requirements.txt
└── .env.example
```
