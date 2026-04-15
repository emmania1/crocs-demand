"""
Retail signals — Google Maps review activity per Crocs store location.

Two-pass design:
  1. DISCOVERY: Google Places Text Search across major US cities and outlet malls
     to find official Crocs retail stores. Results cached to data/stores.json
     so discovery only runs when empty or --rediscover flag set.
  2. SNAPSHOT: Places Details for each store — current rating + total review
     count. Appended to data/store_review_history.csv with today's date.

Over time, review-count deltas between snapshots act as a foot-traffic proxy:
  - Rising review volume → store is getting traffic + converting to reviews
  - Rising rating → operational quality holding up
  - Falling volume → traffic softening

Outputs:
  data/crocs_stores.json           — master store list (discovery output)
  data/store_review_history.csv    — append-only snapshot log
  data/store_review_latest.csv     — most-recent snapshot per store (for dashboard)

Requires: GOOGLE_PLACES_API_KEY in crocs_demand/.env
  Get one at: https://console.cloud.google.com/apis/credentials
  Enable "Places API" for the project. Free tier covers ~30k detail lookups/month.

Run:
  python3 scripts/fetch_retail_signals.py              # snapshot only (fast)
  python3 scripts/fetch_retail_signals.py --rediscover # re-run discovery + snapshot
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import date, datetime

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

API_KEY = os.environ.get("GOOGLE_PLACES_API_KEY")

TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"
DETAILS_URL     = "https://maps.googleapis.com/maps/api/place/details/json"

STORES_OUT      = DATA_DIR / "crocs_stores.json"
HISTORY_OUT     = DATA_DIR / "store_review_history.csv"
LATEST_OUT      = DATA_DIR / "store_review_latest.csv"

SLEEP_S = 0.25

# Search queries chosen to cover majors + known outlet clusters.
# Crocs owns ~370 outlet stores in the US; flagship-style locations are rare.
SEARCH_QUERIES = [
    # Major US metros
    "Crocs store New York NY",
    "Crocs store Los Angeles CA",
    "Crocs store Chicago IL",
    "Crocs store Houston TX",
    "Crocs store Phoenix AZ",
    "Crocs store Philadelphia PA",
    "Crocs store San Antonio TX",
    "Crocs store Dallas TX",
    "Crocs store Las Vegas NV",
    "Crocs store Orlando FL",
    "Crocs store Atlanta GA",
    "Crocs store Miami FL",
    "Crocs store Boston MA",
    "Crocs store Seattle WA",
    "Crocs store Denver CO",
    # Outlet-mall clusters (Crocs' primary retail footprint)
    "Crocs outlet premium outlets",
    "Crocs store outlet mall",
    "Crocs Tanger outlets",
    "Crocs store Mall of America",
    # International flagships worth tracking
    "Crocs flagship Tokyo",
    "Crocs store London",
    "Crocs store Shanghai",
]


# ─────────────────────────────────────────────────────────────────────────────
def text_search(query: str, page_token: str = None) -> dict:
    params = {"query": query, "key": API_KEY}
    if page_token:
        params["pagetoken"] = page_token
    r = requests.get(TEXT_SEARCH_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("status") not in ("OK", "ZERO_RESULTS"):
        print(f"    [warn] Places API status={data.get('status')} for query='{query}': {data.get('error_message', '')}")
    return data


def place_details(place_id: str) -> dict:
    params = {
        "place_id": place_id,
        "fields":   "name,rating,user_ratings_total,formatted_address,geometry,business_status",
        "key":      API_KEY,
    }
    r = requests.get(DETAILS_URL, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    if data.get("status") != "OK":
        return {}
    return data.get("result", {})


def looks_like_crocs_store(result: dict) -> bool:
    name = (result.get("name") or "").lower()
    # Filter false positives: Shoe Carnival selling Crocs, etc.
    # Accept any place where "crocs" is in the official business name.
    return "crocs" in name


def discover_stores() -> list:
    seen = {}
    for q in SEARCH_QUERIES:
        print(f"  search: {q!r} …", end=" ", flush=True)
        try:
            data = text_search(q)
            results = data.get("results", [])
            kept = 0
            for r in results:
                if not looks_like_crocs_store(r):
                    continue
                pid = r.get("place_id")
                if pid and pid not in seen:
                    seen[pid] = {
                        "place_id": pid,
                        "name":     r.get("name"),
                        "address":  r.get("formatted_address"),
                        "rating":   r.get("rating"),
                        "review_count": r.get("user_ratings_total"),
                        "lat":      r.get("geometry", {}).get("location", {}).get("lat"),
                        "lng":      r.get("geometry", {}).get("location", {}).get("lng"),
                    }
                    kept += 1
            print(f"{len(results)} results / {kept} kept")
            # Paginate if more results (up to 60 total, 20 per page)
            next_token = data.get("next_page_token")
            for _ in range(2):
                if not next_token:
                    break
                time.sleep(2)  # Google requires a delay before pagetoken is valid
                data = text_search(q, page_token=next_token)
                results = data.get("results", [])
                for r in results:
                    if not looks_like_crocs_store(r):
                        continue
                    pid = r.get("place_id")
                    if pid and pid not in seen:
                        seen[pid] = {
                            "place_id": pid,
                            "name":     r.get("name"),
                            "address":  r.get("formatted_address"),
                            "rating":   r.get("rating"),
                            "review_count": r.get("user_ratings_total"),
                            "lat":      r.get("geometry", {}).get("location", {}).get("lat"),
                            "lng":      r.get("geometry", {}).get("location", {}).get("lng"),
                        }
                next_token = data.get("next_page_token")
        except Exception as e:
            print(f"error: {e}")
        time.sleep(SLEEP_S)
    return list(seen.values())


def snapshot_reviews(stores: list) -> pd.DataFrame:
    today = str(date.today())
    rows = []
    for s in stores:
        pid = s.get("place_id")
        if not pid:
            continue
        try:
            det = place_details(pid)
            rows.append({
                "snapshot_date": today,
                "place_id":      pid,
                "name":          det.get("name") or s.get("name"),
                "address":       det.get("formatted_address") or s.get("address"),
                "rating":        det.get("rating"),
                "review_count":  det.get("user_ratings_total"),
                "status":        det.get("business_status"),
            })
        except Exception as e:
            print(f"    [warn] details err for {pid}: {e}")
        time.sleep(SLEEP_S)
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rediscover", action="store_true",
                        help="Re-run Places Text Search discovery (not just details snapshot)")
    args = parser.parse_args()

    print("── Crocs retail review tracker ──────────────────────────────")
    if not API_KEY:
        print("  [ERROR] GOOGLE_PLACES_API_KEY not set.")
        print("  Setup:")
        print("    1. https://console.cloud.google.com/apis/credentials → create API key")
        print("    2. Enable 'Places API' for the project")
        print("    3. Add to crocs_demand/.env:  GOOGLE_PLACES_API_KEY=<key>")
        sys.exit(1)

    # Load or rediscover store list
    if STORES_OUT.exists() and not args.rediscover:
        stores = json.loads(STORES_OUT.read_text())
        print(f"  Using cached store list: {len(stores)} stores (run with --rediscover to refresh)")
    else:
        print("  Discovering Crocs stores via Google Places Text Search…")
        stores = discover_stores()
        if not stores:
            print("  [ERROR] No stores found. API key may be missing Places API authorization.")
            sys.exit(1)
        STORES_OUT.write_text(json.dumps(stores, indent=2))
        print(f"  → {STORES_OUT} ({len(stores)} stores)")

    # Snapshot reviews
    print(f"  Snapshotting reviews for {len(stores)} stores …")
    snap = snapshot_reviews(stores)
    if snap.empty:
        print("  No snapshot data (API error).")
        return

    # Append to history, write latest
    if HISTORY_OUT.exists():
        hist = pd.read_csv(HISTORY_OUT)
        combined = pd.concat([hist, snap], ignore_index=True).drop_duplicates(
            subset=["snapshot_date", "place_id"], keep="last"
        )
    else:
        combined = snap
    combined.to_csv(HISTORY_OUT, index=False)
    print(f"  → {HISTORY_OUT} ({len(snap)} new rows / {len(combined)} total)")

    snap.to_csv(LATEST_OUT, index=False)
    print(f"  → {LATEST_OUT} ({len(snap)} latest-snapshot rows)")

    # Summary
    if snap["rating"].notna().any():
        avg_rating = snap["rating"].mean()
        total_reviews = snap["review_count"].fillna(0).sum()
        print(f"\n  Summary: {len(snap)} stores · avg rating {avg_rating:.2f} · {int(total_reviews):,} total reviews")


if __name__ == "__main__":
    main()
