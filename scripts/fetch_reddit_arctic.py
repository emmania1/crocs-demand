"""
Historical Reddit backfill via Arctic Shift archive — 2+ years of posts per silhouette.

Arctic Shift (arctic-shift.photon-reddit.com) is a free Reddit archive that
goes back to 2020+, not limited by Reddit search's recency bias. Required for
YoY comparisons since the official Reddit JSON API caps at roughly 1 year.

Subreddits covered: r/crocs, r/heydude, r/sneakers, r/femalefashionadvice,
r/malefashionadvice (broader catchments for silhouette mentions).

Outputs (overwritten on each run):
  data/reddit_arctic_raw.csv      — every post, all years
  data/reddit_arctic_monthly.csv  — monthly mention counts per silhouette
  data/reddit_arctic_yoy.csv      — YoY comparisons per silhouette (rolling 30d)

Rate limit: ~1.2s between requests; pages keyword-by-keyword due to Arctic
Shift's OR+after bug. Full backfill takes ~5–10 min depending on match volume.

Run (monthly is enough; replaces any prior Reddit data):
  python3 scripts/fetch_reddit_arctic.py
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

BASE_URL      = "https://arctic-shift.photon-reddit.com/api/posts/search"
USER_AGENT    = "crocs-demand-tracker/0.2"
SUBREDDITS    = ["crocs", "heydude", "sneakers", "femalefashionadvice", "malefashionadvice"]

# Backfill window — 3 full years + current
START_DT      = datetime(2023, 1, 1, tzinfo=timezone.utc)
END_DT        = datetime.now(timezone.utc)
START_TS      = int(START_DT.timestamp())
END_TS        = int(END_DT.timestamp())

SLEEP_S       = 1.2
MAX_PAGES_PER = 40  # max pages per keyword-subreddit pair (100 posts/page → 4000 posts)

RAW_OUT       = DATA_DIR / "reddit_arctic_raw.csv"
MONTHLY_OUT   = DATA_DIR / "reddit_arctic_monthly.csv"
YOY_OUT       = DATA_DIR / "reddit_arctic_yoy.csv"


def utc_dt(ts):
    return datetime.fromtimestamp(int(ts), tz=timezone.utc)


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def _paginate(params_extra: dict, max_pages: int = MAX_PAGES_PER) -> list:
    """Generic Arctic Shift paginator — walks 'after' cursor until exhausted."""
    rows = []
    cursor_ts = START_TS
    page = 0
    base_params = {"limit": 100, "sort": "asc"}
    while page < max_pages:
        page += 1
        params = {**base_params, **params_extra, "after": cursor_ts}
        try:
            r = requests.get(BASE_URL, params=params,
                             headers={"User-Agent": USER_AGENT}, timeout=25)
            if r.status_code == 429:
                print(f"      rate-limited, sleeping 30s")
                time.sleep(30)
                continue
            r.raise_for_status()
            data = r.json().get("data") or []
        except Exception as e:
            print(f"      page {page} err: {e}")
            break
        if not data:
            break
        newest = 0
        for p in data:
            ts = int(p.get("created_utc") or p.get("created", 0))
            if ts < 1_000_000_000:
                continue
            rows.append({
                "subreddit":    p.get("subreddit", params_extra.get("subreddit", "")),
                "post_id":      p.get("id"),
                "created_ts":   ts,
                "date":         utc_dt(ts).strftime("%Y-%m-%d"),
                "month":        utc_dt(ts).strftime("%Y-%m"),
                "year":         utc_dt(ts).year,
                "title":        p.get("title", "") or "",
                "score":        int(p.get("score") or 0),
                "num_comments": int(p.get("num_comments") or 0),
                "permalink":    f"https://reddit.com{p.get('permalink', '')}",
            })
            newest = max(newest, ts)
        if newest <= cursor_ts:
            break
        cursor_ts = newest + 1
        time.sleep(SLEEP_S)
    return rows


def fetch_keyword(subreddit: str, keyword: str) -> list:
    """Title-matched search — good for high-volume subs like r/crocs."""
    rows = _paginate({"subreddit": subreddit, "title": keyword})
    for r in rows:
        r["keyword"] = keyword
    return rows


def fetch_subreddit_all(subreddit: str, max_pages: int = 60) -> list:
    """
    Fetch EVERY post from a subreddit since START_DT, no title filter.
    Used for small subs like r/heydude where per-silhouette title searches
    return 0 because posters use natural language, not model names.
    """
    return _paginate({"subreddit": subreddit}, max_pages=max_pages)


def match_silhouettes_in_title(title: str, silhouettes: pd.DataFrame) -> list:
    """Return list of silhouette_keys whose reddit_terms appear in title."""
    t = title.lower()
    hits = []
    for _, row in silhouettes.iterrows():
        terms = [x.strip().lower() for x in str(row["reddit_terms"]).split("|")]
        for term in terms:
            if term and term in t:
                hits.append(row["silhouette_key"])
                break
    return hits


def main():
    print("── Arctic Shift Reddit backfill (2023-01-01 → today) ──────────")
    silhouettes = load_silhouettes()
    all_rows = []

    # PASS 1: Per-silhouette title search on large subs (r/crocs, r/sneakers, etc.)
    for _, sil in silhouettes.iterrows():
        if sil["brand"] == "HeyDude":
            continue   # HeyDude handled in Pass 2 (brand-level sweep)
        key = sil["silhouette_key"]
        terms = [t.strip() for t in str(sil["reddit_terms"]).split("|") if t.strip()]
        primary = terms[0] if terms else sil["search_term"]

        subs = ["crocs", "sneakers", "femalefashionadvice", "malefashionadvice"]
        for sub in subs:
            print(f"  r/{sub} × {key} ({primary!r}) …", end=" ", flush=True)
            posts = fetch_keyword(sub, primary)
            for p in posts:
                p["silhouette_key"] = key
                p["keyword"] = primary
                all_rows.append(p)
            print(f"{len(posts)} posts")

    # PASS 2: Brand-level sweep of r/heydude — per-silhouette title search returns
    # near-zero because posters use natural language. Fetch all posts, match post-hoc.
    print("  [heydude brand sweep] r/heydude all posts ...", end=" ", flush=True)
    hd_posts = fetch_subreddit_all("heydude")
    print(f"{len(hd_posts)} total posts")
    # Also sweep for HeyDude mentions in r/sneakers (narrow filter since big sub)
    print("  [heydude brand sweep] r/sneakers title=HeyDude ...", end=" ", flush=True)
    hd_posts_sn = fetch_keyword("sneakers", "HeyDude")
    print(f"{len(hd_posts_sn)} posts")
    hd_all = hd_posts + hd_posts_sn

    # Match each HeyDude-sub post to silhouette keys via title scan
    for p in hd_all:
        hits = match_silhouettes_in_title(p.get("title", ""), silhouettes)
        # Prefer HeyDude-brand hits; fall back to generic "heydude_other" if post is
        # clearly HeyDude-related (in r/heydude sub) but matches no specific silhouette
        hd_hits = [h for h in hits if silhouettes[silhouettes["silhouette_key"] == h].iloc[0]["brand"] == "HeyDude"] if hits else []
        if hd_hits:
            for h in hd_hits:
                row = {**p, "silhouette_key": h, "keyword": "brand_sweep"}
                all_rows.append(row)
        elif p.get("subreddit", "").lower() == "heydude":
            # Posted to r/heydude but no specific silhouette matched → bucket
            row = {**p, "silhouette_key": "heydude_other", "keyword": "brand_sweep"}
            all_rows.append(row)

    if not all_rows:
        print("No data fetched.")
        return

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["post_id", "silhouette_key"], keep="first")
    df = df.sort_values("created_ts", ascending=False)
    df.to_csv(RAW_OUT, index=False)
    print(f"  → {RAW_OUT} ({len(df)} rows spanning {df['date'].min()} → {df['date'].max()})")

    # Monthly counts per silhouette
    monthly = (df.groupby(["month", "silhouette_key"])
                 .agg(mentions=("post_id", "count"),
                      total_score=("score", "sum"),
                      total_comments=("num_comments", "sum"))
                 .reset_index())
    monthly.to_csv(MONTHLY_OUT, index=False)
    print(f"  → {MONTHLY_OUT} ({len(monthly)} month×silhouette rows)")

    # YoY: rolling-30d current vs same 30d one year ago
    today = datetime.now(timezone.utc)
    win30_start   = today - timedelta(days=30)
    prior30_start = today - timedelta(days=60)
    yoy_start     = today - timedelta(days=365) - timedelta(days=15)
    yoy_end       = today - timedelta(days=365) + timedelta(days=15)

    def count_in(sub_df, start, end):
        return int(((sub_df["created_ts"] >= int(start.timestamp())) &
                    (sub_df["created_ts"] <  int(end.timestamp()))).sum())

    yoy_rows = []
    for key, grp in df.groupby("silhouette_key"):
        curr_30d     = count_in(grp, win30_start, today)
        prior_30d    = count_in(grp, prior30_start, win30_start)
        yoy_30d      = count_in(grp, yoy_start, yoy_end)
        total_12mo   = count_in(grp, today - timedelta(days=365), today)
        total_24mo   = count_in(grp, today - timedelta(days=730), today)
        prior_12mo   = count_in(grp, today - timedelta(days=730), today - timedelta(days=365))

        def pct(curr, prev):
            if prev > 0:
                return round((curr - prev) / prev * 100, 1)
            return None

        yoy_rows.append({
            "silhouette_key":  key,
            "curr_30d":        curr_30d,
            "prior_30d":       prior_30d,
            "delta_30d_pct":   pct(curr_30d, prior_30d),
            "yoy_30d":         yoy_30d,
            "yoy_30d_pct":     pct(curr_30d, yoy_30d),
            "total_12mo":      total_12mo,
            "prior_12mo":      prior_12mo,
            "yoy_12mo_pct":    pct(total_12mo, prior_12mo),
            "total_24mo":      total_24mo,
        })
    pd.DataFrame(yoy_rows).to_csv(YOY_OUT, index=False)
    print(f"  → {YOY_OUT} ({len(yoy_rows)} silhouettes with YoY stats)")


if __name__ == "__main__":
    main()
