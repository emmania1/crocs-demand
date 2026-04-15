"""
Amazon Best Sellers rank scraper — Crocs category intelligence.

Amazon best-seller rank is one of the strongest free consumer-demand signals:
updated hourly, reflects real purchases, covers every major Crocs SKU.

Scrapes the public Best Sellers pages for Crocs-relevant categories:
  - Men's Clogs & Mules
  - Women's Clogs & Mules
  - Men's Outdoor Sandals
  - Women's Outdoor Sandals

For each Crocs product found in the top 100, records: rank, title, price,
rating, review_count, and attempts to match to a silhouette_key.

Output:
  data/amazon_ranks.csv   — one row per SKU per run (append mode)

Status: RISKY — Amazon actively blocks scrapers. If this returns 0 rows or
503 errors, fallback options are (a) use a proxy rotation service, (b) use
the paid Keepa API, or (c) snapshot manually via browser.

Run:
  python3 scripts/fetch_amazon_ranks.py
"""
import sys
import time
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

RANKS_OUT = DATA_DIR / "amazon_ranks.csv"

CATEGORIES = [
    ("mens_clogs",    "https://www.amazon.com/gp/bestsellers/fashion/679453011/"),
    ("womens_clogs",  "https://www.amazon.com/gp/bestsellers/fashion/679462011/"),
    ("mens_sandals",  "https://www.amazon.com/gp/bestsellers/fashion/679388011/"),
    ("womens_sandals","https://www.amazon.com/gp/bestsellers/fashion/679398011/"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
SLEEP_S = 3.0


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def match_silhouette(title: str, silhouettes: pd.DataFrame) -> str:
    t = title.lower()
    if "crocs" not in t and "heydude" not in t and "hey dude" not in t:
        return ""
    for _, r in silhouettes.iterrows():
        terms = [x.strip().lower() for x in str(r["reddit_terms"]).split("|")]
        for term in terms:
            if term and term in t:
                return r["silhouette_key"]
    return ""


def scrape_category(cat_key: str, url: str, silhouettes: pd.DataFrame) -> list:
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"    HTTP {r.status_code} for {cat_key}")
            return []
    except Exception as e:
        print(f"    error: {e}")
        return []

    soup = BeautifulSoup(r.text, "lxml")
    rows = []
    # Amazon's best-sellers page uses a couple DOM structures; try both
    items = soup.select("div.zg-grid-general-faceout, div#gridItemRoot, li.zg-item-immersion")
    rank = 0
    for it in items:
        rank += 1
        title_el = it.select_one("div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1, .p13n-sc-truncate, .p13n-sc-truncated, span[aria-hidden='true']")
        title = title_el.get_text(strip=True) if title_el else ""
        price_el = it.select_one("span._cDEzb_p13n-sc-price_3mJ9Z, span.p13n-sc-price")
        price = price_el.get_text(strip=True) if price_el else ""
        rating_el = it.select_one("span.a-icon-alt")
        rating = rating_el.get_text(strip=True) if rating_el else ""
        reviews_el = it.select_one("span.a-size-small, span.a-size-base")
        reviews = reviews_el.get_text(strip=True) if reviews_el else ""
        link_el = it.select_one("a.a-link-normal")
        link = ("https://www.amazon.com" + link_el["href"]) if link_el and link_el.has_attr("href") else ""

        sil = match_silhouette(title, silhouettes)
        if not sil and "crocs" not in title.lower() and "heydude" not in title.lower() and "hey dude" not in title.lower():
            continue
        rows.append({
            "snapshot_date":  datetime.now().strftime("%Y-%m-%d"),
            "category":       cat_key,
            "rank":           rank,
            "title":          title,
            "silhouette_key": sil,
            "price":          price,
            "rating":         rating,
            "reviews":        reviews,
            "link":           link,
        })
    return rows


def main():
    print("── Amazon Best Sellers (Crocs categories) ────────────────────")
    silhouettes = load_silhouettes()
    all_rows = []
    for cat_key, url in CATEGORIES:
        print(f"  {cat_key} …", end=" ", flush=True)
        rows = scrape_category(cat_key, url, silhouettes)
        all_rows.extend(rows)
        print(f"{len(rows)} Crocs/HeyDude entries")
        time.sleep(SLEEP_S + random.random() * 1.5)

    if not all_rows:
        print("  No rows captured. Amazon likely blocked the request.")
        print("  Options: use proxy rotation, paid Keepa API, or manual snapshot.")
        return

    new_df = pd.DataFrame(all_rows)
    # Append to existing file if present
    if RANKS_OUT.exists():
        existing = pd.read_csv(RANKS_OUT)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["snapshot_date", "category", "rank"], keep="last"
        )
    else:
        combined = new_df
    combined.to_csv(RANKS_OUT, index=False)
    print(f"  → {RANKS_OUT} ({len(new_df)} new / {len(combined)} total rows)")


if __name__ == "__main__":
    main()
