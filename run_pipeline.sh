#!/bin/bash
# Full Crocs demand pipeline — runs all fetchers, then regenerates dashboard.
# Safe to run repeatedly; each fetcher is idempotent per snapshot_date.

set -e
cd "$(dirname "$0")"

# Activate venv (created by: python3 -m venv venv && venv/bin/pip install -r requirements.txt)
if [ -d "venv" ]; then
    source venv/bin/activate
fi

echo "======================================================================"
echo "  CROCS DEMAND PIPELINE — $(date +%Y-%m-%d\ %H:%M)"
echo "======================================================================"

# Primary sources — historical depth + broad coverage + stock
python3 scripts/fetch_reddit_arctic.py || echo "  [warn] Arctic Shift Reddit failed — continuing"
echo
python3 scripts/fetch_google_news.py   || echo "  [warn] Google News failed — continuing"
echo
python3 scripts/fetch_youtube.py       || echo "  [warn] YouTube failed — continuing"
echo
python3 scripts/fetch_stock_price.py   || echo "  [warn] Stock price fetch failed — continuing"
echo
python3 scripts/fetch_retail_signals.py || echo "  [warn] Retail signals skipped (needs GOOGLE_PLACES_API_KEY in .env)"
echo

# Supplementary sources — nice-to-have
python3 scripts/fetch_sneaker_news.py       || echo "  [warn] SneakerNews failed — continuing"
echo
python3 scripts/fetch_google_trends.py      || echo "  [warn] Google Trends failed — continuing"
echo
python3 scripts/fetch_amazon_ranks.py       || echo "  [warn] Amazon scrape failed — continuing"
echo
python3 scripts/fetch_stockx.py             || echo "  [warn] StockX scrape failed — continuing"
echo

# Always regenerate dashboard last, from whatever CSVs we have
python3 scripts/generate_crocs_dashboard.py

echo
echo "======================================================================"
echo "  Done. Dashboard → $(pwd)/index.html"
echo "======================================================================"
