"""
Google Trends — RELATIVE spike detection only.

Per memory rule: Google Trends absolute levels are unreliable in 2026 due to
AI-assistant query deflation. We use it strictly for:
  (a) pre/post spike analysis around known release dates
  (b) internal relative ranking between silhouettes in the same time window

NOT for "is Crocs growing" absolute trend calls.

Outputs:
  data/google_trends_weekly.csv  — raw weekly interest, 5-year window,
                                    per silhouette (for relative use only)
  data/google_trends_relative.csv — ranking of silhouettes within last 90d

Status: FRAGILE — pytrends hits Google rate limits often; sometimes fails
entirely for days at a time. Designed to fail gracefully.

Run:
  python3 scripts/fetch_google_trends.py
"""
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

try:
    from pytrends.request import TrendReq
except ImportError:
    print("ERROR: pytrends not installed. Run: pip install -r requirements.txt")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

WEEKLY_OUT    = DATA_DIR / "google_trends_weekly.csv"
RELATIVE_OUT  = DATA_DIR / "google_trends_relative.csv"

TIMEFRAME = "today 5-y"
GEO       = ""   # worldwide; change to "US" for US-only
SLEEP_S   = 2.5  # seconds between requests to avoid 429


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def safe_interest(pytrends, term: str, tries: int = 3) -> pd.DataFrame | None:
    for attempt in range(tries):
        try:
            pytrends.build_payload([term], timeframe=TIMEFRAME, geo=GEO)
            df = pytrends.interest_over_time()
            if df.empty:
                return None
            return df[[term]].rename(columns={term: "interest"})
        except Exception as e:
            print(f"    attempt {attempt+1}/{tries} failed: {e}")
            time.sleep(SLEEP_S * (2 ** attempt))
    return None


def main():
    print("── Google Trends (relative spike detection) ──────────────────")
    silhouettes = load_silhouettes()
    # Tier-1 silhouettes only for first pass — saves rate limit
    t1 = silhouettes[silhouettes["tracking_tier"] == 1].copy()
    print(f"  tracking {len(t1)} Tier-1 silhouettes")

    pytrends = TrendReq(hl="en-US", tz=300, retries=2, backoff_factor=0.5)

    rows = []
    for _, r in t1.iterrows():
        term = r["search_term"]
        print(f"  {term} …", end=" ", flush=True)
        df = safe_interest(pytrends, term)
        if df is None:
            print("no data / rate-limited")
            continue
        for ts, row in df.iterrows():
            rows.append({
                "week":            ts.strftime("%Y-%m-%d"),
                "silhouette_key":  r["silhouette_key"],
                "search_term":     term,
                "interest":        int(row["interest"]),
            })
        print(f"{len(df)} weeks")
        time.sleep(SLEEP_S)

    if not rows:
        print("  No Google Trends data fetched (likely rate-limited). Try again later.")
        return

    weekly = pd.DataFrame(rows)
    weekly.to_csv(WEEKLY_OUT, index=False)
    print(f"  → {WEEKLY_OUT} ({len(weekly)} rows)")

    # Relative ranking within last 90d — avoids absolute-level trap
    cutoff = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    recent = weekly[weekly["week"] >= cutoff].copy()
    if recent.empty:
        print("  (no recent data for relative ranking)")
        return
    rank = (recent.groupby("silhouette_key")["interest"]
                   .mean().reset_index(name="avg_interest_90d"))
    rank["rank_within_crocs"] = rank["avg_interest_90d"].rank(ascending=False).astype(int)
    rank.sort_values("avg_interest_90d", ascending=False).to_csv(RELATIVE_OUT, index=False)
    print(f"  → {RELATIVE_OUT} ({len(rank)} silhouettes ranked)")


if __name__ == "__main__":
    main()
