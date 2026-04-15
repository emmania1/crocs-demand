"""
StockX resale premium scraper — collab / limited-edition demand signal.

Queries StockX's public search endpoint for Crocs products and pulls the
median sale price + retail price, computing premium = median / retail.
Premium > 1.0x = aftermarket heat (people paying over MSRP).
Premium < 1.0x = cold (reselling below retail).

Output:
  data/stockx_premiums.csv  — one row per SKU per run (append mode)

Status: EXPERIMENTAL — StockX has no official API. Uses their internal
Algolia search endpoint. May break if they change headers/auth. Per user
decision: run the scraper; fall back to manual CSV snapshots if it breaks.

If it breaks, the fallback is:
  data/stockx_manual.csv     — add rows by hand from StockX web UI
  columns: snapshot_date,silhouette_key,product_name,retail,median_sold_price

Run:
  python3 scripts/fetch_stockx.py
"""
import sys
import time
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

PREMIUMS_OUT = DATA_DIR / "stockx_premiums.csv"
MANUAL_IN    = DATA_DIR / "stockx_manual.csv"

ALGOLIA_URL  = "https://xw7sbct9v6-3.algolianet.com/1/indexes/products/query"
ALGOLIA_APP  = "XW7SBCT9V6"
ALGOLIA_KEY  = "6bfb5abee4dcd8cea8f0ca1ca085c2b3"  # StockX public key (readonly search)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/122.0.0.0 Safari/537.36"),
    "Content-Type": "application/x-www-form-urlencoded",
    "x-algolia-api-key":      ALGOLIA_KEY,
    "x-algolia-application-id": ALGOLIA_APP,
    "Referer": "https://stockx.com/",
    "Origin":  "https://stockx.com",
}

SLEEP_S = 2.0


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def algolia_search(query: str, hits: int = 10) -> list:
    """Query StockX's Algolia search for products matching `query`."""
    try:
        params = (f"query={requests.utils.quote(query)}"
                  f"&hitsPerPage={hits}&facets=*")
        body = {"params": params}
        r = requests.post(ALGOLIA_URL, json=body, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return []
        data = r.json()
        return data.get("hits", [])
    except Exception as e:
        print(f"    error: {e}")
        return []


def parse_hit(hit: dict, silhouette_key: str) -> dict | None:
    title = hit.get("name") or hit.get("title") or ""
    # Only keep items that look like Crocs/HeyDude
    if "croc" not in title.lower() and "heydude" not in title.lower() and "hey dude" not in title.lower():
        return None
    retail = hit.get("retail_price") or hit.get("retailPrice") or 0
    last_sale = hit.get("last_sale") or hit.get("lastSale") or 0
    avg_sale = hit.get("average_deadstock_price") or hit.get("averageDeadstockPrice") or 0
    try:
        retail_f = float(retail or 0)
        avg_f    = float(avg_sale or last_sale or 0)
    except (TypeError, ValueError):
        return None
    if retail_f <= 0 or avg_f <= 0:
        return None
    return {
        "snapshot_date":   datetime.now().strftime("%Y-%m-%d"),
        "silhouette_key":  silhouette_key,
        "product_name":    title,
        "retail":          retail_f,
        "avg_sale_price":  avg_f,
        "premium":         round(avg_f / retail_f, 3),
        "url_key":         hit.get("url_key") or hit.get("urlKey") or "",
    }


def main():
    print("── StockX premiums (aftermarket heat) ────────────────────────")
    silhouettes = load_silhouettes()
    # Focus on non-legacy + collab silhouettes (legacy rarely on StockX)
    targets = silhouettes[silhouettes["category"].isin(
        ["non_legacy_core", "designer", "collab", "premium", "sandal"]
    )].copy()

    new_rows = []
    for _, sil in targets.iterrows():
        query = sil["search_term"]
        print(f"  {query} …", end=" ", flush=True)
        hits = algolia_search(query, hits=5)
        kept = 0
        for h in hits:
            row = parse_hit(h, sil["silhouette_key"])
            if row:
                new_rows.append(row)
                kept += 1
        print(f"{kept} usable hits")
        time.sleep(SLEEP_S)

    if not new_rows:
        print("  No StockX data returned. Likely scraper broke — fall back to manual.")
        if MANUAL_IN.exists():
            print(f"  Manual fallback present at {MANUAL_IN}")
        else:
            print(f"  Create {MANUAL_IN} with columns:")
            print(f"    snapshot_date,silhouette_key,product_name,retail,avg_sale_price,premium")
        return

    new_df = pd.DataFrame(new_rows)
    if PREMIUMS_OUT.exists():
        existing = pd.read_csv(PREMIUMS_OUT)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["snapshot_date", "silhouette_key", "product_name"], keep="last"
        )
    else:
        combined = new_df
    combined.to_csv(PREMIUMS_OUT, index=False)
    print(f"  → {PREMIUMS_OUT} ({len(new_df)} new / {len(combined)} total rows)")


if __name__ == "__main__":
    main()
