"""
YouTube Data API — per-silhouette review/coverage tracking, 2+ years back.

Uses the YouTube Data API v3 search.list endpoint to find videos mentioning
each silhouette, then videos.list to pull view/like/comment stats. Produces
a historical record going back to 2023-01-01.

API quota: search.list costs 100 units/call, videos.list costs 1/call.
Free daily quota is 10,000 — so ~60 searches + their video-stat calls fit
comfortably. Re-running daily is safe.

Uses the YOUTUBE_API_KEY already set up for the Warhammer dashboard
(see .env). If missing, script errors cleanly.

Outputs:
  data/youtube_raw.csv          — every matched video with stats
  data/youtube_monthly.csv      — monthly upload counts + views per silhouette
  data/youtube_yoy.csv          — YoY comparisons per silhouette

Run:
  python3 scripts/fetch_youtube.py
"""
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta

import pandas as pd
import requests

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

API_KEY = os.environ.get("YOUTUBE_API_KEY")
if not API_KEY:
    print("ERROR: YOUTUBE_API_KEY not set. Copy from warhammer_demand/.env or set in crocs_demand/.env")
    sys.exit(1)

SEARCH_URL = "https://www.googleapis.com/youtube/v3/search"
VIDEOS_URL = "https://www.googleapis.com/youtube/v3/videos"

# Backfill window
START_DT    = datetime(2023, 1, 1, tzinfo=timezone.utc)
START_RFC3  = START_DT.strftime("%Y-%m-%dT%H:%M:%SZ")

RAW_OUT     = DATA_DIR / "youtube_raw.csv"
MONTHLY_OUT = DATA_DIR / "youtube_monthly.csv"
YOY_OUT     = DATA_DIR / "youtube_yoy.csv"

MAX_RESULTS = 50       # per search.list call
MAX_PAGES   = 3        # up to 150 videos per silhouette
SLEEP_S     = 0.3


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def search_videos(query: str) -> list:
    video_ids = []
    page_token = None
    for _ in range(MAX_PAGES):
        params = {
            "part":        "id,snippet",
            "q":           query,
            "type":        "video",
            "maxResults":  MAX_RESULTS,
            "publishedAfter": START_RFC3,
            "key":         API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token
        try:
            r = requests.get(SEARCH_URL, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"      search err: {e}")
            break
        items = data.get("items", [])
        for it in items:
            vid = it.get("id", {}).get("videoId")
            if vid:
                sn = it.get("snippet", {})
                video_ids.append({
                    "video_id":      vid,
                    "title":         sn.get("title", ""),
                    "channel_title": sn.get("channelTitle", ""),
                    "channel_id":    sn.get("channelId", ""),
                    "published_at":  sn.get("publishedAt", ""),
                })
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(SLEEP_S)
    return video_ids


def fetch_video_stats(video_ids: list) -> dict:
    """Batch fetch statistics for up to 50 IDs at a time."""
    out = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        params = {
            "part": "statistics",
            "id":   ",".join(batch),
            "key":  API_KEY,
        }
        try:
            r = requests.get(VIDEOS_URL, params=params, timeout=20)
            r.raise_for_status()
            for it in r.json().get("items", []):
                s = it.get("statistics", {})
                out[it["id"]] = {
                    "views":    int(s.get("viewCount", 0) or 0),
                    "likes":    int(s.get("likeCount", 0) or 0),
                    "comments": int(s.get("commentCount", 0) or 0),
                }
        except Exception as e:
            print(f"      stats err: {e}")
        time.sleep(SLEEP_S)
    return out


def is_relevant(title: str, silhouette_row) -> bool:
    """
    Strict relevance: title must contain BOTH brand word AND a silhouette term.
    Filters out viral tangentially-related videos ("girl stomps in her Crocs")
    that previously inflated view counts.
    """
    t = (title or "").lower()
    brand = "crocs" if silhouette_row["brand"] == "Crocs" else "heydude"
    has_brand = brand in t or "hey dude" in t  # accept "Hey Dude" spacing
    if not has_brand:
        return False
    terms = [x.strip().lower() for x in str(silhouette_row["reddit_terms"]).split("|") if x.strip()]
    has_sil = any(term in t for term in terms)
    return has_sil


def main():
    print("── YouTube Data API (2023-01-01 → today, strict relevance) ───")
    silhouettes = load_silhouettes()
    all_rows = []

    for _, sil in silhouettes.iterrows():
        if sil["tracking_tier"] > 2:
            continue
        yt_q = sil.get("youtube_terms") or sil["search_term"]
        key  = sil["silhouette_key"]
        print(f"  {key} ({yt_q!r}) …", end=" ", flush=True)

        videos = search_videos(yt_q)
        if not videos:
            print("0 videos")
            continue

        # RELEVANCE FILTER — drop videos whose titles don't have brand + silhouette
        relevant = [v for v in videos if is_relevant(v["title"], sil)]
        if not relevant:
            print(f"{len(videos)} fetched / 0 relevant (filtered out as tangential)")
            continue

        stats = fetch_video_stats([v["video_id"] for v in relevant])
        for v in relevant:
            st = stats.get(v["video_id"], {})
            pub = v.get("published_at", "")
            try:
                pub_dt = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                month = pub_dt.strftime("%Y-%m")
            except Exception:
                pub_dt = None
                month = ""
            all_rows.append({
                "silhouette_key": key,
                "video_id":       v["video_id"],
                "title":          v["title"],
                "channel_title":  v["channel_title"],
                "channel_id":     v["channel_id"],
                "published_at":   pub,
                "published_month": month,
                "views":          st.get("views", 0),
                "likes":          st.get("likes", 0),
                "comments":       st.get("comments", 0),
                "url":            f"https://youtu.be/{v['video_id']}",
            })
        total_views = sum(st.get("views", 0) for st in stats.values())
        print(f"{len(videos)} fetched / {len(relevant)} relevant ({total_views:,} views)")

    if not all_rows:
        print("No data fetched.")
        return

    new_df = pd.DataFrame(all_rows).drop_duplicates(subset=["video_id", "silhouette_key"], keep="first")
    # MERGE with any existing data — quota hits shouldn't destroy prior runs.
    # Keep=last means we refresh stats if same (video_id, silhouette_key) re-appears.
    if RAW_OUT.exists():
        try:
            existing = pd.read_csv(RAW_OUT)
            df = pd.concat([existing, new_df], ignore_index=True).drop_duplicates(
                subset=["video_id", "silhouette_key"], keep="last"
            )
            print(f"  merged: {len(existing)} existing + {len(new_df)} new → {len(df)} total")
        except Exception:
            df = new_df
    else:
        df = new_df
    df.to_csv(RAW_OUT, index=False)
    print(f"  → {RAW_OUT} ({len(df)} videos)")

    # Monthly stats
    monthly = (df[df["published_month"].astype(str).str.len() >= 7]
                 .groupby(["published_month", "silhouette_key"])
                 .agg(uploads=("video_id", "count"),
                      total_views=("views", "sum"),
                      total_likes=("likes", "sum"))
                 .reset_index()
                 .rename(columns={"published_month": "month"}))
    monthly.to_csv(MONTHLY_OUT, index=False)
    print(f"  → {MONTHLY_OUT} ({len(monthly)} month×silhouette rows)")

    # YoY per silhouette
    today = datetime.now(timezone.utc)
    def in_window(sub, start, end):
        mask = pd.to_datetime(sub["published_at"], errors="coerce", utc=True)
        return int(((mask >= start) & (mask < end)).sum()), int(sub.loc[(mask >= start) & (mask < end), "views"].sum() if not sub.empty else 0)

    yoy = []
    for key, grp in df.groupby("silhouette_key"):
        up12, v12 = in_window(grp, today - timedelta(days=365), today)
        up_prev12, v_prev12 = in_window(grp, today - timedelta(days=730), today - timedelta(days=365))
        up30, _ = in_window(grp, today - timedelta(days=30), today)
        upyoy30, _ = in_window(grp, today - timedelta(days=395), today - timedelta(days=335))

        def pct(c, p):
            if p > 0: return round((c - p) / p * 100, 1)
            return None
        yoy.append({
            "silhouette_key": key,
            "uploads_30d":    up30,
            "uploads_yoy_30d":upyoy30,
            "uploads_yoy_30d_pct": pct(up30, upyoy30),
            "uploads_12mo":   up12,
            "uploads_prev_12mo": up_prev12,
            "uploads_12mo_yoy_pct": pct(up12, up_prev12),
            "views_12mo":     v12,
            "views_prev_12mo":v_prev12,
            "views_12mo_yoy_pct": pct(v12, v_prev12),
        })
    pd.DataFrame(yoy).to_csv(YOY_OUT, index=False)
    print(f"  → {YOY_OUT} ({len(yoy)} silhouettes with YoY stats)")


if __name__ == "__main__":
    main()
