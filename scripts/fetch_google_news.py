"""
Google News RSS — comprehensive news coverage across the ENTIRE internet.

Google News aggregates every publisher (sneaker blogs, mainstream press,
trade pubs, local outlets). We query per-silhouette + brand-level queries
and merge into a unified news stream.

This replaces the single-RSS SneakerNews approach. Keeps SneakerNews as a
supplemental feed via fetch_sneaker_news.py but this is the primary.

Outputs:
  data/google_news_raw.csv      — every matched article
  data/google_news_upcoming.csv — articles mentioning a future release date
  data/google_news_monthly.csv  — monthly article counts per silhouette
  data/google_news_publishers.csv — article count by publisher (source diversity check)

No auth required. Rate-limited: 2s between queries.

Run:
  python3 scripts/fetch_google_news.py
"""
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from urllib.parse import quote_plus

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

GN_BASE = "https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"

RAW_OUT        = DATA_DIR / "google_news_raw.csv"
UPCOMING_OUT   = DATA_DIR / "google_news_upcoming.csv"
MONTHLY_OUT    = DATA_DIR / "google_news_monthly.csv"
PUBLISHERS_OUT = DATA_DIR / "google_news_publishers.csv"

# Brand-level queries (catch everything, not silhouette-specific)
BRAND_QUERIES = [
    ("crocs_releases",   '"Crocs" (release OR drop OR launch OR collab)'),
    ("crocs_partnership",'"Crocs" (partnership OR collaboration OR announce)'),
    ("heydude_releases", '"HeyDude" (release OR drop OR launch OR collab)'),
    ("crocs_designer",   '"Crocs" (Steven Smith OR Salehe Bembury OR designer)'),
]

SLEEP_S = 2.0

MONTH = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t)?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
DATE_PATTERNS = [
    rf"(?:releas[es]{{0,2}}|drop[s]?|launch[es]{{0,2}}|available|coming)\s+(?:on\s+)?({MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?(?:,\s*\d{{4}})?)",
    rf"({MONTH}\s+\d{{1,2}}(?:st|nd|rd|th)?,\s*\d{{4}})\s+(?:release|drop|launch)",
]


def load_silhouettes() -> pd.DataFrame:
    return pd.read_csv(CONFIG_DIR / "silhouettes.csv")


def match_silhouettes(text: str, silhouettes: pd.DataFrame) -> list:
    text_lc = text.lower()
    hits = []
    for _, row in silhouettes.iterrows():
        terms = [t.strip().lower() for t in str(row["reddit_terms"]).split("|")]
        for term in terms:
            if term and term in text_lc:
                hits.append(row["silhouette_key"])
                break
    return hits


def extract_release_date(text: str, pub_year: int = None) -> str | None:
    """
    Extract a release date from article text. When no year is explicitly
    mentioned in the text, default to the article's PUBLISH year (if known),
    not the current year — otherwise articles from 2023/2024 that say
    'releases on May 20' get parsed as May 20, 2026 which is wrong and makes
    stale articles show up in the Drop Calendar as upcoming.
    """
    default_year = pub_year if pub_year else datetime.now().year
    for pat in DATE_PATTERNS:
        m = re.search(pat, text, flags=re.IGNORECASE)
        if m:
            raw = re.sub(r"(?:st|nd|rd|th)", "", m.group(1))
            for fmt in ["%b %d, %Y", "%B %d, %Y", "%b %d", "%B %d"]:
                try:
                    dt = datetime.strptime(raw, fmt)
                    if dt.year == 1900:
                        dt = dt.replace(year=default_year)
                        # Only bump to next year if we're using the CURRENT year
                        # as default and the date is already past — this avoids
                        # treating "May 20" in a 2024 article as 2024-05-20 (correct)
                        # while keeping "May 20" in a 2026 article as 2027-05-20 (upcoming)
                        if pub_year is None and dt < datetime.now():
                            dt = dt.replace(year=dt.year + 1)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return None


def extract_publisher(entry) -> str:
    # Google News puts publisher in source.title or at end of title after " - "
    src = getattr(entry, "source", None)
    if src and getattr(src, "title", None):
        return src.title
    title = getattr(entry, "title", "")
    if " - " in title:
        return title.rsplit(" - ", 1)[-1].strip()
    return ""


def run_query(label: str, query: str, silhouettes: pd.DataFrame) -> list:
    url = GN_BASE.format(q=quote_plus(query))
    try:
        d = feedparser.parse(url)
    except Exception as e:
        print(f"    error: {e}")
        return []
    rows = []
    for e in d.entries:
        title   = getattr(e, "title", "") or ""
        summary = getattr(e, "summary", "") or ""
        link    = getattr(e, "link", "") or ""
        try:
            pub_dt = datetime(*e.published_parsed[:6]) if getattr(e, "published_parsed", None) else None
        except Exception:
            pub_dt = None
        publisher = extract_publisher(e)
        full_text = f"{title} {summary}"
        hits = match_silhouettes(full_text, silhouettes)
        rel_date = extract_release_date(full_text, pub_year=pub_dt.year if pub_dt else None)
        brand = "HeyDude" if "heydude" in full_text.lower() or "hey dude" in full_text.lower() else "Crocs"
        rows.append({
            "query_label":          label,
            "query":                query,
            "published":            pub_dt.isoformat() if pub_dt else "",
            "published_month":      pub_dt.strftime("%Y-%m") if pub_dt else "",
            "title":                title,
            "publisher":            publisher,
            "link":                 link,
            "silhouette_hits":      "|".join(hits) if hits else "",
            "brand":                brand,
            "extracted_release_date": rel_date or "",
            "is_upcoming":          bool(rel_date and rel_date >= datetime.now().strftime("%Y-%m-%d")),
        })
    return rows


def main():
    print("── Google News RSS (broad internet coverage) ─────────────────")
    silhouettes = load_silhouettes()

    all_rows = []

    # Brand-level queries
    for label, q in BRAND_QUERIES:
        print(f"  [brand] {label} …", end=" ", flush=True)
        rows = run_query(label, q, silhouettes)
        all_rows.extend(rows)
        print(f"{len(rows)} articles")
        time.sleep(SLEEP_S)

    # Per-silhouette queries (Tier 1 + 2 only, keeps rate usage reasonable)
    # Use news_terms as-is (don't force-quote); simpler queries match more broadly.
    # If user wants exact-phrase match, they put quotes inside the news_terms cell.
    t12 = silhouettes[silhouettes["tracking_tier"] <= 2].copy()
    for _, sil in t12.iterrows():
        news_q = sil.get("news_terms") or sil["search_term"]
        print(f"  [silhouette] {sil['silhouette_key']} ({news_q!r}) …", end=" ", flush=True)
        rows = run_query(f"sil_{sil['silhouette_key']}", news_q, silhouettes)
        all_rows.extend(rows)
        print(f"{len(rows)} articles")
        time.sleep(SLEEP_S)

    if not all_rows:
        print("No articles fetched.")
        return

    df = pd.DataFrame(all_rows).drop_duplicates(subset=["link"], keep="first")
    df.to_csv(RAW_OUT, index=False)
    print(f"  → {RAW_OUT} ({len(df)} unique articles)")

    # Upcoming
    upcoming = df[df["is_upcoming"]].copy()
    if not upcoming.empty:
        upcoming = upcoming.sort_values("extracted_release_date")
    upcoming.to_csv(UPCOMING_OUT, index=False)
    print(f"  → {UPCOMING_OUT} ({len(upcoming)} upcoming)")

    # Monthly per silhouette
    monthly_rows = []
    for _, r in df.iterrows():
        if not r["published_month"] or not r["silhouette_hits"]:
            continue
        for k in r["silhouette_hits"].split("|"):
            monthly_rows.append({"month": r["published_month"], "silhouette_key": k})
    if monthly_rows:
        m = pd.DataFrame(monthly_rows)
        counts = m.groupby(["month", "silhouette_key"]).size().reset_index(name="articles")
        counts.to_csv(MONTHLY_OUT, index=False)
        print(f"  → {MONTHLY_OUT} ({len(counts)} month×silhouette rows)")

    # Publisher diversity
    pubs = df["publisher"].fillna("").replace("", "Unknown")
    pub_counts = pubs.value_counts().reset_index()
    pub_counts.columns = ["publisher", "article_count"]
    pub_counts.to_csv(PUBLISHERS_OUT, index=False)
    print(f"  → {PUBLISHERS_OUT} ({len(pub_counts)} distinct publishers)")


if __name__ == "__main__":
    main()
