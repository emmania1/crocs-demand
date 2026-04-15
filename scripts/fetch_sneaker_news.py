"""
Sneaker news RSS → Crocs/HeyDude mentions.

Pulls from SneakerNews, Hypebeast, and Sole Retriever tag feeds, filters for
Crocs/HeyDude, matches each entry to the silhouette universe in
config/silhouettes.csv, and writes:

  data/sneaker_news_raw.csv       — every matched entry, one row per article
  data/sneaker_news_upcoming.csv  — entries that mention a future release date
  data/sneaker_news_monthly.csv   — monthly mention counts per silhouette

Status: ROCK-SOLID — RSS is public, stable.
No auth required.

Run:
  python3 scripts/fetch_sneaker_news.py
"""
import os
import re
import sys
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

try:
    import feedparser
except ImportError:
    print("ERROR: feedparser not installed. Run: pip install -r requirements.txt")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
DATA_DIR.mkdir(exist_ok=True)

FEEDS = [
    # SneakerNews — only RSS feed confirmed working for sneaker-specific coverage.
    # Hypebeast / Highsnobiety / SoleCollector RSS paths are all broken as of 2026-04;
    # they return HTML or invalid-token errors. Removed rather than spam warnings —
    # those publishers are still captured aggregately via fetch_google_news.py
    # (Google News RSS indexes all of them).
    ("sneakernews_crocs",   "https://sneakernews.com/tag/crocs/feed/"),
]

RAW_OUT       = DATA_DIR / "sneaker_news_raw.csv"
UPCOMING_OUT  = DATA_DIR / "sneaker_news_upcoming.csv"
MONTHLY_OUT   = DATA_DIR / "sneaker_news_monthly.csv"


def load_silhouettes() -> pd.DataFrame:
    df = pd.read_csv(CONFIG_DIR / "silhouettes.csv")
    return df


def match_silhouettes(text: str, silhouettes: pd.DataFrame) -> list:
    """Return list of silhouette_keys mentioned in text (case-insensitive)."""
    text_lc = text.lower()
    hits = []
    for _, row in silhouettes.iterrows():
        # reddit_terms is a pipe-delimited search term list — reuse for matching
        terms = [t.strip().lower() for t in str(row["reddit_terms"]).split("|")]
        for term in terms:
            if term and term in text_lc:
                hits.append(row["silhouette_key"])
                break
    return hits


# Rough date extractors — sneaker news frequently says "releases on Mon DD" or "drops Mon DD, YYYY"
MONTH = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
DATE_PATTERNS = [
    rf"(?:release[s]?|drop[s]?|launche[s]?|available)\s+(?:on\s+)?({MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*\d{{4}})?)",
    rf"({MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?,\s*\d{{4}})\s+(?:release|drop|launch)",
]

def extract_release_date(text: str) -> str | None:
    for pat in DATE_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            raw = m.group(1)
            # Try to parse — assume current year if year missing
            raw_clean = re.sub(r"(?:st|nd|rd|th)", "", raw)
            for fmt in ["%b %d, %Y", "%B %d, %Y", "%b %d", "%B %d"]:
                try:
                    dt = datetime.strptime(raw_clean, fmt)
                    if dt.year == 1900:  # no year → assume this year
                        dt = dt.replace(year=datetime.now().year)
                        if dt < datetime.now():  # past → probably next year
                            dt = dt.replace(year=dt.year + 1)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return None


def fetch_all() -> pd.DataFrame:
    silhouettes = load_silhouettes()
    rows = []
    for feed_key, url in FEEDS:
        print(f"  {feed_key} …", end=" ", flush=True)
        try:
            d = feedparser.parse(url)
            if d.bozo and not d.entries:
                print(f"skip ({d.bozo_exception})")
                continue
            for e in d.entries:
                title   = getattr(e, "title", "") or ""
                summary = getattr(e, "summary", "") or getattr(e, "description", "") or ""
                link    = getattr(e, "link", "") or ""
                pub     = getattr(e, "published", "") or getattr(e, "updated", "") or ""
                try:
                    pub_dt = datetime(*e.published_parsed[:6]) if getattr(e, "published_parsed", None) else None
                except Exception:
                    pub_dt = None

                full_text = f"{title} {summary}"
                hits = match_silhouettes(full_text, silhouettes)
                rel_date = extract_release_date(full_text)

                # only keep entries that at least mention Crocs or HeyDude
                if "crocs" not in full_text.lower() and "heydude" not in full_text.lower() and "hey dude" not in full_text.lower():
                    continue

                rows.append({
                    "feed":             feed_key,
                    "published":        pub_dt.isoformat() if pub_dt else pub,
                    "published_month":  pub_dt.strftime("%Y-%m") if pub_dt else "",
                    "title":            title,
                    "link":             link,
                    "silhouette_hits":  "|".join(hits) if hits else "",
                    "brand":            "HeyDude" if "heydude" in feed_key or "hey-dude" in full_text.lower() else "Crocs",
                    "extracted_release_date": rel_date or "",
                    "is_upcoming":      bool(rel_date and rel_date >= datetime.now().strftime("%Y-%m-%d")),
                })
            print(f"{len(d.entries)} entries")
        except Exception as ex:
            print(f"error: {ex}")
    return pd.DataFrame(rows)


def main():
    print("── Sneaker News RSS ────────────────────────────────────────────")
    df = fetch_all()
    if df.empty:
        print("No entries fetched.")
        return
    df = df.drop_duplicates(subset=["link"], keep="first")
    df.sort_values("published", ascending=False).to_csv(RAW_OUT, index=False)
    print(f"  → {RAW_OUT} ({len(df)} rows)")

    upcoming = df[df["is_upcoming"]].copy()
    upcoming.sort_values("extracted_release_date").to_csv(UPCOMING_OUT, index=False)
    print(f"  → {UPCOMING_OUT} ({len(upcoming)} upcoming)")

    # Monthly mention counts per silhouette
    monthly_rows = []
    for _, r in df.iterrows():
        if not r["published_month"] or not r["silhouette_hits"]:
            continue
        for k in r["silhouette_hits"].split("|"):
            monthly_rows.append({"month": r["published_month"], "silhouette_key": k})
    if monthly_rows:
        m = pd.DataFrame(monthly_rows)
        counts = m.groupby(["month", "silhouette_key"]).size().reset_index(name="mentions")
        counts.to_csv(MONTHLY_OUT, index=False)
        print(f"  → {MONTHLY_OUT} ({len(counts)} month×silhouette rows)")


if __name__ == "__main__":
    main()
