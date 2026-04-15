"""
Crocs Demand Acceleration Intelligence — Dashboard v4

Layout philosophy: most substantial → least substantial.
Outsider-friendly: clear data-source attribution on every chart; neutral
descriptive language (no investment-thesis framing in UI text). "What to do
next" kept in chat sessions only, not on the dashboard.

Sections:
  1. OVERVIEW            — 24-month category trajectory + hero tiles
  2. WHERE THE HEAT IS   — YoY growth + absolute volume + engagement
  3. RELEASE RADAR       — upcoming drops + recent coverage + manual log explainer
  4. NEWS COVERAGE       — cadence + topic breakdown + publisher diversity
  5. DEMAND vs STOCK     — CROX price line with past-drop event markers
  6. FULL DETAIL         — per-silhouette heat map (hidden behind button)

Reads (all optional — missing files degrade gracefully):
  config/silhouettes.csv                    REQUIRED
  config/crocs_releases.csv                 manual log
  data/google_news_{raw,upcoming,publishers}.csv
  data/reddit_arctic_{raw,monthly,yoy}.csv
  data/youtube_{raw,yoy}.csv
  data/crox_stock.csv
  data/crox_stock_monthly.csv
"""
import os
import sys
import json
import webbrowser
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
CONFIG_DIR   = PROJECT_ROOT / "config"
OUTPUT_HTML  = PROJECT_ROOT / "index.html"

BRAND_NAME    = "Crocs"
BRAND_ACCENT  = "#06A550"
BRAND_ACCENT2 = "#F58025"

CATEGORY_COLORS = {
    "legacy":          "#e74c3c",
    "non_legacy_core": "#06A550",
    "sandal":          "#27ae60",
    "premium":         "#8e44ad",
    "designer":        "#2980b9",
    "collab":          "#F58025",
    "heydude":         "#95a5a6",
}
CATEGORY_LABELS = {
    "legacy":          "Legacy",
    "non_legacy_core": "Non-Legacy Core",
    "sandal":          "Sandal",
    "premium":         "Premium/Platform",
    "designer":        "Designer",
    "collab":          "Collab",
    "heydude":         "HeyDude",
}
MIN_SIGNAL_POSTS   = 5
MIN_VOLUME_FOR_YOY = 4

# News topic classification — keyword-based, checked in dict order (first match wins).
# Collab checked BEFORE release so a "Xbox x Crocs launch" article tags as collab (more informative).
# IP/brand/partner names caught explicitly — audit showed 47 legacy collab articles ("Xbox and Crocs",
# "Pokémon Classic Clog", "SpongeBob", "Krispy Kreme") were buried in "other" with the narrower list.
TOPIC_KEYWORDS = {
    "collab": [
        "collab", "collaboration", "partnership", "teams up", "limited edition", " x ",
        # Named collab partners we track
        "salehe bembury", "steven smith", "mschf", "balenciaga", "post malone",
        "bad bunny", "justin bieber", "drake", "lindsay lohan", "mcm", "kith",
        "palace", "takashi murakami", "drew house",
        # Entertainment / gaming IP collabs that turn up in legacy clog news
        "xbox", "playstation", "nintendo", "spongebob", "patrick star", "squidward",
        "doraemon", "pokémon", "pokemon", "one piece", "dragon ball", "naruto",
        "shrek", "lightning mcqueen", "super mario", "sesame street",
        # Food/brand collabs
        "krispy kreme", "mcdonalds", "mcdonald's", "kfc", "7-eleven", "7 eleven",
        "dunkin", "chipotle", "taco bell",
    ],
    "release":   ["release", "drops", "launches", "launching", "available now", "coming soon",
                  "unveils", "debut", "hits shelves", "restocking", "restock", "back in stock",
                  "adding a", "drop coming", "unveil", "pack releases"],
    "review":    ["review", "hands-on", "first look", "on foot", "on feet", "unboxing",
                  "worth it", "compared", "honest review"],
    "financial": ["earnings", "revenue", "quarter", "guidance", "ceo", "cfo", "stock price",
                  "shares", "analyst", "upgrade", "downgrade", "beats estimates", "misses estimates"],
    "culture":   ["viral", "trend", "celebrity", "spotted", "wearing", "outfit", "style guide",
                  "tiktok", "instagram", "red carpet", "street style"],
}
TOPIC_COLORS = {
    "release":   "#06A550",
    "collab":    "#F58025",
    "review":    "#58a6ff",
    "financial": "#e67e22",
    "culture":   "#e84393",
    "other":     "#6e7781",
}
TOPIC_LABELS = {
    "release":   "Release / Drop",
    "collab":    "Collab",
    "review":    "Review",
    "financial": "Financial",
    "culture":   "Culture / Celebrity",
    "other":     "Other",
}


# ─────────────────────────────────────────────────────────────────────────────
# Loading
# ─────────────────────────────────────────────────────────────────────────────
def safe_read(path: Path) -> pd.DataFrame:
    if path.exists():
        try: return pd.read_csv(path)
        except Exception as e: print(f"  [warn] {path.name}: {e}")
    return pd.DataFrame()


def load_all():
    s = safe_read(CONFIG_DIR / "silhouettes.csv")
    if s.empty:
        print("[ERROR] config/silhouettes.csv missing — required.")
        sys.exit(1)
    return {
        "silhouettes":      s,
        "manual_releases":  safe_read(CONFIG_DIR / "crocs_releases.csv"),
        "news_raw":         safe_read(DATA_DIR / "google_news_raw.csv"),
        "news_upcoming":    safe_read(DATA_DIR / "google_news_upcoming.csv"),
        "news_publishers":  safe_read(DATA_DIR / "google_news_publishers.csv"),
        "reddit_raw":       safe_read(DATA_DIR / "reddit_arctic_raw.csv"),
        "reddit_monthly":   safe_read(DATA_DIR / "reddit_arctic_monthly.csv"),
        "reddit_yoy":       safe_read(DATA_DIR / "reddit_arctic_yoy.csv"),
        "youtube_raw":      safe_read(DATA_DIR / "youtube_raw.csv"),
        "youtube_yoy":      safe_read(DATA_DIR / "youtube_yoy.csv"),
        "stock_daily":      safe_read(DATA_DIR / "crox_stock.csv"),
        "stock_monthly":    safe_read(DATA_DIR / "crox_stock_monthly.csv"),
    }


def _is_nan_or_none(v):
    return v is None or (isinstance(v, float) and pd.isna(v))


# ─────────────────────────────────────────────────────────────────────────────
# Computations
# ─────────────────────────────────────────────────────────────────────────────
def classify_topic(title: str) -> str:
    t = (title or "").lower()
    for topic, kws in TOPIC_KEYWORDS.items():
        if any(kw in t for kw in kws):
            return topic
    return "other"


def compute_release_radar(d: dict) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    upcoming = []

    if not d["manual_releases"].empty:
        m = d["manual_releases"].copy()
        m = m[m["release_date"].astype(str) >= today]
        for _, r in m.iterrows():
            upcoming.append({
                "release_date":   str(r.get("release_date", "")),
                "brand":          r.get("brand", ""),
                "display_name":   r.get("display_name", ""),
                "collab_partner": r.get("collab_partner") or "",
                "source":         "manual",
                "source_detail":  r.get("source_type", "manual"),
                "link":           "",
            })

    if not d["news_upcoming"].empty:
        # Filter out stale entries — articles published in a past year that
        # reference "May 20" shouldn't be rendered as upcoming on May 20 of the
        # current year. Heuristic: article must be published within last 60 days.
        nu = d["news_upcoming"].copy()
        nu["published_dt"] = pd.to_datetime(nu["published"], errors="coerce")
        if nu["published_dt"].dt.tz is not None:
            nu["published_dt"] = nu["published_dt"].dt.tz_localize(None)
        cutoff = pd.Timestamp(datetime.now() - timedelta(days=60))
        nu = nu[nu["published_dt"] >= cutoff]
        for _, r in nu.iterrows():
            upcoming.append({
                "release_date":   str(r.get("extracted_release_date", "")),
                "brand":          r.get("brand", "Crocs"),
                "display_name":   str(r.get("title", ""))[:70],
                "collab_partner": "",
                "source":         "news",
                "source_detail":  r.get("publisher") or "Google News",
                "link":           r.get("link", ""),
            })

    # Recent drops (last 30 days) — release/collab topic articles from the news
    # corpus. These are the real "drops" that just landed. Useful to show in
    # left column of Drop Calendar alongside upcoming.
    recent_drops = []
    if not d["news_raw"].empty:
        s = d["news_raw"].copy()
        s["published"] = pd.to_datetime(s["published"], errors="coerce")
        if s["published"].dt.tz is not None:
            s["published"] = s["published"].dt.tz_localize(None)
        s["topic"] = s["title"].apply(classify_topic)
        cut30 = pd.Timestamp(datetime.now() - timedelta(days=30))
        s = s[(s["published"] >= cut30) & (s["topic"].isin(["release", "collab"]))]
        s = s.sort_values("published", ascending=False)
        # Dedupe by title to avoid 5 articles covering the same drop
        s = s.drop_duplicates(subset=["title"], keep="first").head(20)
        for _, r in s.iterrows():
            recent_drops.append({
                "date":       r["published"].strftime("%Y-%m-%d") if pd.notna(r["published"]) else "",
                "brand":      r.get("brand", "Crocs"),
                "title":      str(r.get("title", ""))[:80],
                "topic":      r.get("topic", "release"),
                "publisher":  r.get("publisher") or "Unknown",
                "link":       r.get("link", ""),
            })

    up_df = pd.DataFrame(upcoming)
    if not up_df.empty:
        up_df = up_df.sort_values("release_date")

    recent = []
    topic_by_sil = {}
    topic_totals = {t: 0 for t in list(TOPIC_KEYWORDS.keys()) + ["other"]}

    if not d["news_raw"].empty:
        s = d["news_raw"].copy()
        s["published"] = pd.to_datetime(s["published"], errors="coerce")
        if s["published"].dt.tz is not None:
            s["published"] = s["published"].dt.tz_localize(None)
        cut = pd.Timestamp(datetime.now() - timedelta(days=90))

        # Classify all articles
        s["topic"] = s["title"].apply(classify_topic)
        for _, r in s.iterrows():
            topic_totals[r["topic"]] = topic_totals.get(r["topic"], 0) + 1
            hits = str(r.get("silhouette_hits", "") or "").split("|")
            for k in hits:
                if k:
                    topic_by_sil.setdefault(k, {t: 0 for t in TOPIC_KEYWORDS.keys() | {"other"}})
                    topic_by_sil[k][r["topic"]] = topic_by_sil[k].get(r["topic"], 0) + 1

        s_recent = s[s["published"] >= cut].sort_values("published", ascending=False).head(25)
        for _, r in s_recent.iterrows():
            recent.append({
                "date":      r["published"].strftime("%Y-%m-%d") if pd.notna(r["published"]) else "",
                "brand":     r.get("brand", "Crocs"),
                "title":     str(r.get("title", ""))[:90],
                "publisher": r.get("publisher") or "Unknown",
                "topic":     r.get("topic", "other"),
                "link":      r.get("link", ""),
            })

    cadence = {"labels": [], "crocs": [], "heydude": []}
    if not d["news_raw"].empty:
        s = d["news_raw"].copy()
        s = s[s["published_month"].astype(str).str.len() >= 7]
        last24 = sorted(s["published_month"].unique())[-24:]
        for m in last24:
            sub = s[s["published_month"] == m]
            cadence["labels"].append(m)
            cadence["crocs"].append(int((sub["brand"] == "Crocs").sum()))
            cadence["heydude"].append(int((sub["brand"] == "HeyDude").sum()))

    publishers = d["news_publishers"].head(15).to_dict(orient="records") if not d["news_publishers"].empty else []

    upc_30d = 0
    if not up_df.empty:
        cut = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        upc_30d = int((up_df["release_date"] <= cut).sum())
    recent_30d = sum(1 for r in recent if r["date"] >= (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"))

    return {
        "upcoming":           up_df.to_dict(orient="records") if not up_df.empty else [],
        "recent":             recent,
        "recent_drops":       recent_drops,
        "cadence":            cadence,
        "publishers":         publishers,
        "upcoming_count_30d": upc_30d,
        "recent_count_30d":   recent_30d,
        "recent_drops_count": len(recent_drops),
        "total_upcoming":     len(up_df) if not up_df.empty else 0,
        "topic_totals":       topic_totals,
        "topic_by_sil":       topic_by_sil,
    }


def compute_heat_map(d: dict) -> list:
    sils = d["silhouettes"].copy()
    rows = []

    reddit_yoy = d["reddit_yoy"].set_index("silhouette_key").to_dict(orient="index") if not d["reddit_yoy"].empty else {}
    yt_yoy     = d["youtube_yoy"].set_index("silhouette_key").to_dict(orient="index") if not d["youtube_yoy"].empty else {}

    # Engagement — total score + comments per silhouette
    engagement = {}
    if not d["reddit_raw"].empty:
        r = d["reddit_raw"].copy()
        r["score"] = pd.to_numeric(r["score"], errors="coerce").fillna(0).astype(int)
        r["num_comments"] = pd.to_numeric(r["num_comments"], errors="coerce").fillna(0).astype(int)
        for key, grp in r.groupby("silhouette_key"):
            engagement[key] = {
                "total_score":    int(grp["score"].sum()),
                "total_comments": int(grp["num_comments"].sum()),
                "avg_score":      round(grp["score"].mean(), 1) if len(grp) else 0,
                "avg_comments":   round(grp["num_comments"].mean(), 1) if len(grp) else 0,
                "post_count":     len(grp),
            }

    # Reddit raw counts (30d)
    reddit_30d = {}
    if not d["reddit_raw"].empty:
        r = d["reddit_raw"].copy()
        r["created_ts"] = pd.to_numeric(r["created_ts"], errors="coerce")
        ts30 = datetime.now().timestamp() - 30 * 86400
        for key, grp in r.groupby("silhouette_key"):
            reddit_30d[key] = int((grp["created_ts"] >= ts30).sum())

    # News 12mo
    news_12mo = {}
    news_prev_12mo = {}
    news_monthly = safe_read(DATA_DIR / "google_news_monthly.csv")
    if not news_monthly.empty:
        nm = news_monthly.copy()
        nm["month_dt"] = pd.to_datetime(nm["month"] + "-01", errors="coerce")
        cut12 = pd.Timestamp(datetime.now() - timedelta(days=365))
        cut24 = pd.Timestamp(datetime.now() - timedelta(days=730))
        for key, grp in nm.groupby("silhouette_key"):
            news_12mo[key]      = int(grp[grp["month_dt"] >= cut12]["articles"].sum())
            news_prev_12mo[key] = int(grp[(grp["month_dt"] >= cut24) & (grp["month_dt"] < cut12)]["articles"].sum())

    def pct(c, p):
        if p > 0: return round((c - p) / p * 100, 1)
        return None

    for _, s in sils.iterrows():
        k = s["silhouette_key"]
        ry = reddit_yoy.get(k, {})
        yy = yt_yoy.get(k, {})
        eg = engagement.get(k, {})
        rows.append({
            "silhouette_key":   k,
            "display_name":     s["display_name"],
            "category":         s["category"],
            "brand":            s["brand"],
            "tier":             int(s["tracking_tier"]),
            "reddit_30d":       reddit_30d.get(k, ry.get("curr_30d", 0)),
            "reddit_12mo":      ry.get("total_12mo", 0),
            "reddit_yoy12_pct": ry.get("yoy_12mo_pct"),
            "reddit_24mo":      ry.get("total_24mo", 0),
            "total_score":      eg.get("total_score", 0),
            "total_comments":   eg.get("total_comments", 0),
            "avg_score":        eg.get("avg_score", 0),
            "avg_comments":     eg.get("avg_comments", 0),
            "yt_uploads_12mo":  yy.get("uploads_12mo", 0),
            "yt_views_12mo":    yy.get("views_12mo", 0),
            "yt_views_yoy_pct": yy.get("views_12mo_yoy_pct"),
            "news_12mo":        news_12mo.get(k, 0),
            "news_yoy_pct":     pct(news_12mo.get(k, 0), news_prev_12mo.get(k, 0)),
        })

    # Composite heat
    def rank_asc(vals, reverse=True):
        def valid(v): return not _is_nan_or_none(v)
        clean = [v for v in vals if valid(v)]
        if not clean: return [0] * len(vals)
        sv = sorted(set(clean), reverse=reverse)
        lut = {v: len(sv) - i for i, v in enumerate(sv)}
        return [lut.get(v, 0) if valid(v) else 0 for v in vals]

    for i, row in enumerate(rows):
        pass
    r_r_yoy = rank_asc([r["reddit_yoy12_pct"] for r in rows])
    r_r_12  = rank_asc([r["reddit_12mo"]       for r in rows])
    r_eng   = rank_asc([r["total_comments"]    for r in rows])
    r_y_v   = rank_asc([r["yt_views_12mo"]     for r in rows])
    r_n_12  = rank_asc([r["news_12mo"]         for r in rows])
    for i, row in enumerate(rows):
        row["heat_score"] = r_r_yoy[i] + r_r_12[i] + r_eng[i] + r_y_v[i] + r_n_12[i]

    rows.sort(key=lambda x: -x["heat_score"])
    return rows


def compute_trajectory(d: dict) -> dict:
    sils = d["silhouettes"].set_index("silhouette_key")["category"].to_dict()
    trend = {"labels": [], "legacy": [], "non_legacy": [], "heydude": []}
    if d["reddit_monthly"].empty:
        return trend
    rm = d["reddit_monthly"].copy()
    rm["category"] = rm["silhouette_key"].map(sils)
    rm["bucket"] = rm["category"].apply(
        lambda c: "legacy" if c == "legacy" else ("heydude" if c == "heydude" else "non_legacy")
    )
    months = sorted(rm["month"].unique())[-24:]
    for m in months:
        sub = rm[rm["month"] == m]
        trend["labels"].append(m)
        for b in ["legacy", "non_legacy", "heydude"]:
            trend[b].append(int(sub[sub["bucket"] == b]["mentions"].sum()))
    return trend


def compute_bear_case(d: dict, heat: list) -> dict:
    buckets = {"legacy": [], "non_legacy": [], "heydude": []}
    for r in heat:
        c = r["category"]
        b = "legacy" if c == "legacy" else ("heydude" if c == "heydude" else "non_legacy")
        buckets[b].append(r)

    def agg(rows):
        if not rows: return {"curr_30d": 0, "total_12mo": 0, "yoy_12mo_pct": None, "n_sils": 0}
        curr = sum(x.get("reddit_30d", 0) for x in rows)
        t12  = sum(x.get("reddit_12mo", 0) for x in rows)
        good12 = [(x.get("reddit_yoy12_pct"), x.get("reddit_12mo", 0))
                  for x in rows if not _is_nan_or_none(x.get("reddit_yoy12_pct")) and x.get("reddit_12mo", 0) > 0]
        yoy_12 = None
        if good12:
            tot_w = sum(w for _, w in good12)
            yoy_12 = round(sum(p * w for p, w in good12) / tot_w, 1) if tot_w > 0 else None
        return {"curr_30d": curr, "total_12mo": t12, "yoy_12mo_pct": yoy_12, "n_sils": len(rows)}

    return {
        "legacy":           agg(buckets["legacy"]),
        "non_legacy":       agg(buckets["non_legacy"]),
        "heydude":          agg(buckets["heydude"]),
    }


def compute_growth_leaders(heat: list) -> dict:
    sig = [r for r in heat
           if r["reddit_12mo"] >= MIN_VOLUME_FOR_YOY
           and not _is_nan_or_none(r["reddit_yoy12_pct"])]
    sig.sort(key=lambda x: -x["reddit_yoy12_pct"])
    return {
        "labels":     [r["display_name"] for r in sig],
        "yoy":        [r["reddit_yoy12_pct"] for r in sig],
        "volumes":    [r["reddit_12mo"] for r in sig],
        "categories": [r["category"] for r in sig],
        "colors":     [CATEGORY_COLORS.get(r["category"], "#95a5a6") for r in sig],
    }


def compute_volume_leaders(heat: list, top_n: int = 12) -> dict:
    sig = sorted([r for r in heat if r["reddit_12mo"] > 0],
                 key=lambda x: -x["reddit_12mo"])[:top_n]
    return {
        "labels":    [r["display_name"] for r in sig],
        "volumes":   [r["reddit_12mo"] for r in sig],
        "comments":  [r["total_comments"] for r in sig],
        "colors":    [CATEGORY_COLORS.get(r["category"], "#95a5a6") for r in sig],
    }


def compute_stock_overlay(d: dict) -> dict:
    """
    CROX daily price + monthly Reddit mentions + past drop event markers.
    Drops come from: manual log (past dates) + news articles with extracted
    past release dates. Each drop event is attached to the closest stock close.
    """
    out = {"dates": [], "closes": [], "reddit_labels": [], "reddit_counts": [],
           "events": []}
    if d["stock_daily"].empty:
        return out

    sd = d["stock_daily"].copy()
    sd = sd.sort_values("date")
    out["dates"]  = sd["date"].tolist()
    out["closes"] = [float(x) for x in sd["close"].tolist()]

    # Reddit monthly mentions for stacked bar context
    if not d["reddit_monthly"].empty:
        rm = d["reddit_monthly"].copy()
        monthly_tot = rm.groupby("month")["mentions"].sum().reset_index()
        out["reddit_labels"] = monthly_tot["month"].tolist()
        out["reddit_counts"] = [int(x) for x in monthly_tot["mentions"].tolist()]

    # Past drop events
    events = []
    today = datetime.now().strftime("%Y-%m-%d")
    # From manual log
    if not d["manual_releases"].empty:
        for _, r in d["manual_releases"].iterrows():
            rd = str(r.get("release_date") or "")
            if rd and rd < today:
                events.append({
                    "date":   rd,
                    "label":  str(r.get("display_name") or "Manual drop"),
                    "type":   "manual",
                    "brand":  r.get("brand", "Crocs"),
                })
    # From news articles classified as "release"-themed and published in the past:
    # use the article's published date as a proxy for when the drop happened.
    if not d["news_raw"].empty:
        # Build silhouette_key → category lookup for drop categorization
        sil_cat = d["silhouettes"].set_index("silhouette_key")["category"].to_dict()

        nr = d["news_raw"].copy()
        nr["published"] = pd.to_datetime(nr["published"], errors="coerce")
        if nr["published"].dt.tz is not None:
            nr["published"] = nr["published"].dt.tz_localize(None)
        nr["topic"] = nr["title"].apply(classify_topic)
        nr = nr[(nr["topic"].isin(["release", "collab"])) &
                (nr["silhouette_hits"].astype(str).str.len() > 0)]
        nr = nr.dropna(subset=["published"])
        nr["date"] = nr["published"].dt.strftime("%Y-%m-%d")
        nr = nr.sort_values("published", ascending=False)
        nr = nr.drop_duplicates(subset=["date", "brand"], keep="first")
        for _, r in nr.iterrows():
            # First silhouette_key match → get its category for drop-type analysis
            first_hit = str(r.get("silhouette_hits", "") or "").split("|")[0]
            category = sil_cat.get(first_hit, "unknown")
            events.append({
                "date":           r["date"],
                "label":          str(r.get("title", ""))[:65],
                "type":           "news",
                "brand":          r.get("brand", "Crocs"),
                "topic":          r["topic"],
                "silhouette_key": first_hit,
                "category":       category,
            })
    events.sort(key=lambda e: e["date"])

    # Attach each event to nearest stock trading-day close.
    # Normalize event['date'] to the matched trading day so Chart.js can
    # place markers at the correct category position (stock market is closed
    # on weekends — weekend drop dates otherwise wouldn't render).
    price_by_date = dict(zip(out["dates"], out["closes"]))
    trading_days = sorted(out["dates"])
    for ev in events:
        dt = ev["date"]
        ev["original_date"] = dt
        if dt in price_by_date:
            ev["price"] = price_by_date[dt]
        else:
            prior = [d_ for d_ in trading_days if d_ <= dt]
            if prior:
                ev["date"]  = prior[-1]   # snap to prior trading day
                ev["price"] = price_by_date[prior[-1]]
            else:
                ev["price"] = None

    # Trim to stock window
    if out["dates"]:
        dmin, dmax = out["dates"][0], out["dates"][-1]
        events = [e for e in events if e["price"] is not None and dmin <= e["date"] <= dmax]

    # Dedupe events landing on same trading day (pick one with longest label)
    dedup = {}
    for e in events:
        k = (e["date"], e["brand"])
        if k not in dedup or len(e["label"]) > len(dedup[k]["label"]):
            dedup[k] = e
    out["events"] = sorted(dedup.values(), key=lambda x: x["date"])
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Drop Effect Study — per-drop lift in Reddit / YouTube / News + CROX return
# ─────────────────────────────────────────────────────────────────────────────
def compute_drop_effects(d: dict, stock: dict) -> list:
    """
    For each past drop event: measure the 14d-before vs 14d-after lift in
    Reddit mentions, News articles, YouTube uploads, and the CROX stock move
    1 / 7 / 30 days later. Answers: did drops move demand, and did that
    demand move the stock?
    """
    if not stock.get("events") or d["reddit_raw"].empty:
        return []

    reddit = d["reddit_raw"].copy()
    reddit["created_ts"] = pd.to_numeric(reddit["created_ts"], errors="coerce")

    news = pd.DataFrame()
    if not d["news_raw"].empty:
        news = d["news_raw"].copy()
        news["published_dt"] = pd.to_datetime(news["published"], errors="coerce")
        if news["published_dt"].dt.tz is not None:
            news["published_dt"] = news["published_dt"].dt.tz_localize(None)

    yt = pd.DataFrame()
    if not d["youtube_raw"].empty:
        yt = d["youtube_raw"].copy()
        yt["published_dt"] = pd.to_datetime(yt["published_at"], errors="coerce")
        if yt["published_dt"].dt.tz is not None:
            yt["published_dt"] = yt["published_dt"].dt.tz_localize(None)

    price_by_date = dict(zip(stock["dates"], stock["closes"]))
    trading_days  = sorted(price_by_date.keys())

    def nearest_close_on_or_after(target_str):
        for d_ in trading_days:
            if d_ >= target_str:
                return price_by_date[d_]
        return None

    results = []
    for ev in stock["events"]:
        dt_str = ev["date"]
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%d")
        except ValueError:
            continue

        pre_start_ts = (dt - timedelta(days=14)).timestamp()
        pre_end_ts   = dt.timestamp()
        post_start_ts = dt.timestamp()
        post_end_ts   = (dt + timedelta(days=14)).timestamp()

        pre_r  = int(((reddit["created_ts"] >= pre_start_ts)  & (reddit["created_ts"] <  pre_end_ts )).sum())
        post_r = int(((reddit["created_ts"] >= post_start_ts) & (reddit["created_ts"] <= post_end_ts)).sum())
        reddit_lift = round(post_r / pre_r, 2) if pre_r > 0 else (None if post_r == 0 else float("inf"))

        pre_n = post_n = 0
        news_lift = None
        if not news.empty:
            pre_dt  = pd.Timestamp(dt - timedelta(days=14))
            post_dt = pd.Timestamp(dt + timedelta(days=14))
            dt_ts   = pd.Timestamp(dt)
            pre_n   = int(((news["published_dt"] >= pre_dt)  & (news["published_dt"] <  dt_ts  )).sum())
            post_n  = int(((news["published_dt"] >= dt_ts)   & (news["published_dt"] <= post_dt)).sum())
            news_lift = round(post_n / pre_n, 2) if pre_n > 0 else (None if post_n == 0 else float("inf"))

        yt_lift = None
        pre_y = post_y = 0
        if not yt.empty:
            pre_dt  = pd.Timestamp(dt - timedelta(days=14))
            post_dt = pd.Timestamp(dt + timedelta(days=14))
            dt_ts   = pd.Timestamp(dt)
            pre_y   = int(((yt["published_dt"] >= pre_dt) & (yt["published_dt"] <  dt_ts  )).sum())
            post_y  = int(((yt["published_dt"] >= dt_ts)  & (yt["published_dt"] <= post_dt)).sum())
            yt_lift = round(post_y / pre_y, 2) if pre_y > 0 else (None if post_y == 0 else float("inf"))

        # CROX return 1d / 7d / 30d after drop
        start_price = price_by_date.get(dt_str) or ev.get("price")
        returns = {}
        for days in (1, 7, 30):
            target = (dt + timedelta(days=days)).strftime("%Y-%m-%d")
            tp = nearest_close_on_or_after(target)
            if start_price and tp:
                returns[f"r{days}"] = round((tp - start_price) / start_price * 100, 2)
            else:
                returns[f"r{days}"] = None

        # Composite lift = avg of finite, non-None lift ratios
        lifts = [x for x in (reddit_lift, news_lift, yt_lift)
                 if x is not None and not (isinstance(x, float) and (pd.isna(x) or x == float("inf")))]
        composite = round(sum(lifts) / len(lifts), 2) if lifts else None

        results.append({
            "date":           dt_str,
            "original_date":  ev.get("original_date", dt_str),
            "label":          ev["label"],
            "brand":          ev["brand"],
            "type":           ev["type"],
            "topic":          ev.get("topic", ""),
            "category":       ev.get("category", "unknown"),
            "silhouette_key": ev.get("silhouette_key", ""),
            "drop_price":     ev.get("price"),
            "reddit_pre":     pre_r,
            "reddit_post":    post_r,
            "reddit_lift":    reddit_lift if isinstance(reddit_lift, (int, float)) and not (isinstance(reddit_lift, float) and reddit_lift == float("inf")) else None,
            "news_pre":       pre_n,
            "news_post":      post_n,
            "news_lift":      news_lift if isinstance(news_lift, (int, float)) and not (isinstance(news_lift, float) and news_lift == float("inf")) else None,
            "yt_lift":        yt_lift if isinstance(yt_lift, (int, float)) and not (isinstance(yt_lift, float) and yt_lift == float("inf")) else None,
            "composite_lift": composite,
            "r1":             returns["r1"],
            "r7":             returns["r7"],
            "r30":            returns["r30"],
        })

    results.sort(key=lambda x: -(x["composite_lift"] or 0))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Auto-generated dashboard summary — shown in modal via top-of-page button
# ─────────────────────────────────────────────────────────────────────────────
def compute_summary(d, bear, heat, radar, effects, stock):
    """Build a structured dict of dashboard-wide highlights for the summary modal."""
    import statistics as stats_mod

    # Category deltas
    legacy_yoy     = bear["legacy"]["yoy_12mo_pct"]
    non_legacy_yoy = bear["non_legacy"]["yoy_12mo_pct"]
    heydude_yoy    = bear["heydude"]["yoy_12mo_pct"]

    # Thesis one-liner
    if legacy_yoy is not None and non_legacy_yoy is not None:
        gap = non_legacy_yoy - legacy_yoy
        if gap > 15:
            thesis_line = f"Non-legacy Reddit demand grew <strong>{non_legacy_yoy:+.1f}%</strong> YoY vs legacy <strong>{legacy_yoy:+.1f}%</strong>. Non-legacy is outpacing legacy by <strong>{gap:+.1f}pp</strong> — the mix-shift story is showing up in leading signals."
        elif gap > 0:
            thesis_line = f"Non-legacy Reddit demand grew <strong>{non_legacy_yoy:+.1f}%</strong> YoY vs legacy <strong>{legacy_yoy:+.1f}%</strong>. Non-legacy outpaces legacy by <strong>{gap:+.1f}pp</strong>, but the gap is narrower than a clean mix-shift story would want."
        else:
            thesis_line = f"Non-legacy Reddit demand grew <strong>{non_legacy_yoy:+.1f}%</strong> YoY vs legacy <strong>{legacy_yoy:+.1f}%</strong>. Legacy is matching or outpacing non-legacy — the mix-shift story is not visible in leading signals."
    else:
        thesis_line = "Thesis read inconclusive — missing YoY data on one or both sides."

    # Stock stats
    stock_return = None
    if stock.get("closes") and len(stock["closes"]) > 1:
        stock_return = (stock["closes"][-1] - stock["closes"][0]) / stock["closes"][0] * 100

    # Drop stats
    median_30d = median_lift = None
    pos_30d_pct = 0
    top_drop = bottom_drop = None
    n_drops = len(effects)
    if effects:
        r30s  = [e["r30"] for e in effects if e["r30"] is not None]
        lifts = [e["composite_lift"] for e in effects if e["composite_lift"] is not None]
        if r30s:
            median_30d = stats_mod.median(r30s)
            pos_30d_pct = sum(1 for r in r30s if r > 0) / len(r30s) * 100
        if lifts:
            median_lift = stats_mod.median(lifts)
        top_drop = effects[0] if effects[0]["composite_lift"] else None
        flops = [e for e in effects if e["composite_lift"] is not None and e["composite_lift"] < 1]
        bottom_drop = sorted(flops, key=lambda x: x["composite_lift"])[0] if flops else None

    # Top heat silhouettes (exclude legacy for "growth leaders")
    top_non_legacy = [h for h in heat if h["category"] != "legacy"][:3]
    top_legacy     = [h for h in heat if h["category"] == "legacy"][:2]

    # Biggest growth / decline in silhouettes with real volume
    def _good(v): return v is not None and not (isinstance(v, float) and pd.isna(v))
    sig_yoy = [h for h in heat if h["reddit_12mo"] >= 4 and _good(h["reddit_yoy12_pct"])]
    top_growers = sorted(sig_yoy, key=lambda x: -x["reddit_yoy12_pct"])[:3]
    top_decliners = sorted(sig_yoy, key=lambda x: x["reddit_yoy12_pct"])[:3]

    # News stats
    news_count = len(d["news_raw"]) if not d["news_raw"].empty else 0
    pub_count  = len(d["news_publishers"]) if not d["news_publishers"].empty else 0

    # Reddit stats
    reddit_count = len(d["reddit_raw"]) if not d["reddit_raw"].empty else 0

    # YouTube stats
    yt_count = len(d["youtube_raw"]) if not d["youtube_raw"].empty else 0

    # Data coverage assessment
    coverage_strong = []
    coverage_missing = []
    if reddit_count >= 500: coverage_strong.append(f"Reddit — 3-year archive, {reddit_count:,} user posts across 5 subreddits")
    if news_count >= 500:   coverage_strong.append(f"News — {news_count:,} articles from {pub_count} distinct publishers, topic-classified")
    if yt_count >= 500:     coverage_strong.append(f"YouTube — {yt_count:,} relevance-filtered videos across 30 silhouettes with YoY")
    if stock.get("dates"):  coverage_strong.append(f"CROX — {len(stock['dates'])} daily closes, 3-year window")
    if effects:             coverage_strong.append(f"Drop Effects — {n_drops} past drops with demand-lift + stock-return analysis")

    if not (DATA_DIR / "amazon_ranks.csv").exists() or safe_read(DATA_DIR / "amazon_ranks.csv").empty:
        coverage_missing.append("Amazon Best Sellers rank — scraper blocked, needs Keepa API (~$20/mo) or manual snapshots")
    if not (DATA_DIR / "stockx_premiums.csv").exists() or safe_read(DATA_DIR / "stockx_premiums.csv").empty:
        coverage_missing.append("StockX resale premiums — Algolia endpoint broken, manual CSV fallback available")
    if not (DATA_DIR / "store_review_latest.csv").exists():
        coverage_missing.append("Retail foot-traffic (Google Places review velocity) — needs user API key")
    if d["manual_releases"].empty:
        coverage_missing.append("Manual info-edge release log — user-curated upcoming drops from PLC expert calls / Slack")

    return {
        "thesis_line":        thesis_line,
        "non_legacy_yoy":     non_legacy_yoy,
        "legacy_yoy":         legacy_yoy,
        "heydude_yoy":        heydude_yoy,
        "stock_return_3y":    stock_return,
        "stock_latest":       stock["closes"][-1] if stock.get("closes") else None,
        "stock_latest_date":  stock["dates"][-1]  if stock.get("dates")  else None,
        "n_drops":            n_drops,
        "median_30d_return":  median_30d,
        "pos_30d_hitrate":    pos_30d_pct,
        "median_drop_lift":   median_lift,
        "top_drop":           top_drop,
        "bottom_drop":        bottom_drop,
        "top_growers":        top_growers,
        "top_decliners":      top_decliners,
        "top_non_legacy":     top_non_legacy,
        "top_legacy":         top_legacy,
        "news_count":         news_count,
        "pub_count":          pub_count,
        "reddit_count":       reddit_count,
        "yt_count":           yt_count,
        "coverage_strong":    coverage_strong,
        "coverage_missing":   coverage_missing,
    }


def render_summary_modal(s: dict) -> str:
    """HTML for the summary modal — shown when user clicks the Generate Summary button."""
    def pct(v, fallback="—"):
        if v is None or (isinstance(v, float) and pd.isna(v)): return fallback
        return f"{v:+.1f}%"
    def money(v):
        if v is None: return "—"
        return f"${v:,.2f}"

    # Stat tiles
    tiles = [
        ("Non-Legacy 12mo YoY", pct(s["non_legacy_yoy"]), "#3F9C35"),
        ("Legacy 12mo YoY",     pct(s["legacy_yoy"]),     "#d9715a"),
        ("HeyDude 12mo YoY",    pct(s["heydude_yoy"]),    "#9c9383"),
        ("CROX 3yr return",     pct(s["stock_return_3y"]),"#3F9C35" if (s["stock_return_3y"] or 0) > 0 else "#d9715a"),
        ("Drops analyzed",      f"{s['n_drops']:,}",      "#8b6d3a"),
        ("Median 30d CROX return post-drop", pct(s["median_30d_return"]), "#d9715a" if (s["median_30d_return"] or 0) < 0 else "#3F9C35"),
    ]
    tiles_html = "".join(
        f'<div class="sm-tile" style="border-left:3px solid {color}"><div class="sm-tile-val">{val}</div><div class="sm-tile-lbl">{lbl}</div></div>'
        for lbl, val, color in tiles
    )

    # Top growers
    grow_items = "".join(
        f'<li><strong>{g["display_name"]}</strong> ({CATEGORY_LABELS.get(g["category"], g["category"])}) — {g["reddit_yoy12_pct"]:+.0f}% Reddit YoY, {g["reddit_12mo"]} posts/12mo</li>'
        for g in s["top_growers"]
    )
    decline_items = "".join(
        f'<li><strong>{g["display_name"]}</strong> ({CATEGORY_LABELS.get(g["category"], g["category"])}) — {g["reddit_yoy12_pct"]:+.0f}% Reddit YoY, {g["reddit_12mo"]} posts/12mo</li>'
        for g in s["top_decliners"]
    )

    # Drop highlights
    drop_items = ""
    if s.get("top_drop"):
        t = s["top_drop"]
        r30 = f"{t['r30']:+.1f}%" if t.get("r30") is not None else "n/a"
        drop_items += f'<li><strong>Top demand-lift drop:</strong> "{t["label"][:80]}" ({t["date"]}) — composite lift ×{t["composite_lift"]:.2f}, CROX 30d return {r30}</li>'
    if s.get("bottom_drop"):
        b = s["bottom_drop"]
        r30 = f"{b['r30']:+.1f}%" if b.get("r30") is not None else "n/a"
        drop_items += f'<li><strong>Biggest flop:</strong> "{b["label"][:80]}" ({b["date"]}) — composite lift ×{b["composite_lift"]:.2f} (demand fell post-drop), CROX 30d return {r30}</li>'
    drop_items += f'<li><strong>Drop hit rate:</strong> {s["pos_30d_hitrate"]:.0f}% of drops produced a positive CROX 30-day return (vs 50% coin flip).</li>'
    drop_items += f'<li><strong>Median drop 30d CROX return:</strong> {pct(s["median_30d_return"])} — individual drops are not reliable stock catalysts on 30-day windows.</li>'

    # Coverage
    strong_items  = "".join(f'<li>{x}</li>' for x in s["coverage_strong"])

    return f"""
<div class="summary-overlay" id="summary-overlay" onclick="if(event.target===this) closeSummary()"></div>
<div class="summary-modal" id="summary-modal" role="dialog" aria-labelledby="summary-title">
  <button class="summary-close" onclick="closeSummary()" aria-label="Close">×</button>
  <div class="summary-body">
    <div class="summary-eyebrow">Auto-generated · reads the live data in this dashboard</div>
    <h2 id="summary-title">Executive Summary</h2>

    <div class="summary-thesis">{s["thesis_line"]}</div>

    <h3>Headline metrics</h3>
    <div class="sm-tile-row">{tiles_html}</div>

    <h3>What's growing</h3>
    <ul class="summary-list">{grow_items or '<li>Insufficient data.</li>'}</ul>

    <h3>What's declining</h3>
    <ul class="summary-list">{decline_items or '<li>Insufficient data.</li>'}</ul>

    <h3>Drop effect highlights</h3>
    <ul class="summary-list">{drop_items}</ul>

    <h3>Data sources feeding this dashboard</h3>
    <ul class="summary-list">{strong_items}</ul>

    <div class="summary-footer">Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} from live data in <code>/Users/emmania/Desktop/crocs_demand/data/</code>. Numbers above are computed, not interpreted — full methodology is in each section's source caption.</div>
  </div>
</div>
"""


# ─────────────────────────────────────────────────────────────────────────────
# Findings — computed text snippets injected into chart-insight callouts
# ─────────────────────────────────────────────────────────────────────────────
def compute_findings(d, bear, heat, radar, effects, stock):
    """Returns a dict of pre-computed text snippets — one per big chart."""
    import statistics as stats_mod
    f = {}

    # ── Category trajectory ──────────────────────────────────────────
    nl = bear["non_legacy"]; lg = bear["legacy"]; hd = bear["heydude"]
    nl_yoy = nl["yoy_12mo_pct"]; lg_yoy = lg["yoy_12mo_pct"]; hd_yoy = hd["yoy_12mo_pct"]
    gap = None
    if nl_yoy is not None and lg_yoy is not None:
        gap = nl_yoy - lg_yoy
    f["category"] = (
        f"<strong>Current read:</strong> Non-Legacy <strong>{nl_yoy:+.1f}%</strong> YoY "
        f"vs Legacy <strong>{lg_yoy:+.1f}%</strong> vs HeyDude <strong>{hd_yoy:+.1f}%</strong>. "
        + (f"Non-Legacy leads Legacy by <strong>{gap:+.1f}pp</strong> — " +
           ("thesis is clearly playing out." if (gap or 0) > 15 else
            "small gap; mix shift is visible but not decisive yet." if (gap or 0) > 0 else
            "Legacy is growing at least as fast — the mix-shift story isn't showing up in user conversation.")
           if gap is not None else "Insufficient data to compute gap.")
    )

    # ── YoY growth bars — top grower, top decliner, count of positives/negatives ─
    def _good(v): return v is not None and not (isinstance(v, float) and pd.isna(v))
    sig_yoy = [h for h in heat if h["reddit_12mo"] >= 4 and _good(h["reddit_yoy12_pct"])]
    if sig_yoy:
        sorted_g = sorted(sig_yoy, key=lambda x: -x["reddit_yoy12_pct"])
        sorted_d = sorted(sig_yoy, key=lambda x: x["reddit_yoy12_pct"])
        pos = sum(1 for h in sig_yoy if h["reddit_yoy12_pct"] > 0)
        neg = len(sig_yoy) - pos
        top_grower = sorted_g[0]
        top_decliner = sorted_d[0]
        f["growth"] = (
            f"<strong>Current read:</strong> of {len(sig_yoy)} silhouettes with meaningful volume, "
            f"<strong>{pos} growing</strong> ({pos/len(sig_yoy)*100:.0f}%), "
            f"<strong>{neg} declining</strong>. "
            f"Top grower: <strong>{top_grower['display_name']}</strong> "
            f"({CATEGORY_LABELS.get(top_grower['category'], top_grower['category'])}) "
            f"at <strong>{top_grower['reddit_yoy12_pct']:+.0f}%</strong>. "
            f"Biggest decline: <strong>{top_decliner['display_name']}</strong> "
            f"({CATEGORY_LABELS.get(top_decliner['category'], top_decliner['category'])}) "
            f"at <strong>{top_decliner['reddit_yoy12_pct']:+.0f}%</strong>."
        )
    else:
        f["growth"] = "<strong>Current read:</strong> Insufficient YoY data."

    # ── Volume bars — who leads, top non-legacy vs legacy mix ─────
    vol_sorted = sorted([h for h in heat if h["reddit_12mo"] > 0], key=lambda x: -x["reddit_12mo"])[:10]
    if vol_sorted:
        leader = vol_sorted[0]
        non_legacy_in_top5 = sum(1 for h in vol_sorted[:5] if h["category"] != "legacy")
        top_non_legacy = next((h for h in vol_sorted if h["category"] != "legacy"), None)
        f["volume"] = (
            f"<strong>Current read:</strong> <strong>{leader['display_name']}</strong> "
            f"({CATEGORY_LABELS.get(leader['category'], leader['category'])}) "
            f"leads with <strong>{leader['reddit_12mo']}</strong> posts/12mo. "
            + (f"In the top 5 by volume, <strong>{non_legacy_in_top5}/5 are non-legacy</strong>. " if non_legacy_in_top5 else "")
            + (f"Top non-legacy silhouette: <strong>{top_non_legacy['display_name']}</strong> at {top_non_legacy['reddit_12mo']} posts." if top_non_legacy else "")
        )
    else:
        f["volume"] = ""

    # ── News cadence — recent vs prior, Crocs vs HeyDude ─────────
    cadence = radar.get("cadence", {})
    labels = cadence.get("labels", [])
    if len(labels) >= 6:
        crocs  = cadence["crocs"]
        heydude= cadence["heydude"]
        recent_c = sum(crocs[-3:]); prior_c = sum(crocs[-6:-3])
        recent_h = sum(heydude[-3:]); prior_h = sum(heydude[-6:-3])
        delta_c = (recent_c - prior_c) / prior_c * 100 if prior_c else 0
        delta_h = (recent_h - prior_h) / prior_h * 100 if prior_h else 0
        f["cadence"] = (
            f"<strong>Current read:</strong> Last 3 months averaged "
            f"<strong>{recent_c//3}</strong> Crocs articles/mo "
            f"(<strong>{delta_c:+.0f}%</strong> vs prior 3mo) and "
            f"<strong>{recent_h//3}</strong> HeyDude articles/mo "
            f"(<strong>{delta_h:+.0f}%</strong>). "
            + ("HeyDude coverage is accelerating faster — unexpected for a brand the 2024 bear case called broken." if delta_h > delta_c + 10 else
               "Crocs accelerating faster than HeyDude — consistent with the bull narrative." if delta_c > delta_h + 10 else
               "Both brands moving at similar pace.")
        )
    else:
        f["cadence"] = ""

    # ── Price vs Demand ─────────────────────────────────────────
    closes = stock.get("closes", [])
    if closes and len(closes) > 1:
        first_p = closes[0]; last_p = closes[-1]
        pct_3y = (last_p - first_p) / first_p * 100
        reddit_counts = stock.get("reddit_counts", [])
        if len(reddit_counts) >= 6:
            recent_r = sum(reddit_counts[-6:]) / 6
            early_r  = sum(reddit_counts[:6]) / 6
            reddit_delta = (recent_r - early_r) / early_r * 100 if early_r > 0 else 0
            # Interpretation logic — 6 cases based on demand direction × stock direction
            if reddit_delta > 10 and pct_3y < -5:
                interp = "Demand up, stock down — classic decoupling where mix shift hasn't been priced in."
            elif reddit_delta > 10 and pct_3y > 10:
                interp = "Stock and demand both positive — market and consumers aligned."
            elif reddit_delta < -20 and pct_3y < -5:
                interp = "Both demand and stock are declining over the 3-year window — the thesis is materially at risk on longer-dated signals, even though YoY category data (Section 02) shows recent re-acceleration."
            elif reddit_delta < -20 and pct_3y > 10:
                interp = "Stock rising while Reddit demand falls — market may be pricing something orthogonal (margins, buybacks) rather than demand."
            elif abs(reddit_delta) < 20 and pct_3y < -5:
                interp = "Demand roughly flat while stock is down — the market has re-rated without a demand catalyst to support a reversal."
            else:
                interp = "Demand and stock both roughly flat over the 3-year window — no clear directional read."
            f["price_demand"] = (
                f"<strong>Current read:</strong> CROX is <strong>${last_p:.0f}</strong>, "
                f"<strong>{pct_3y:+.0f}%</strong> over the 3yr window (${min(closes):.0f} low, ${max(closes):.0f} high). "
                f"Monthly Reddit mentions went from <strong>~{early_r:.0f}/mo</strong> in the first 6 months of data to "
                f"<strong>~{recent_r:.0f}/mo</strong> recently ({reddit_delta:+.0f}%). "
                f"{interp}"
            )
        else:
            f["price_demand"] = f"<strong>Current read:</strong> CROX {pct_3y:+.0f}% over 3yr window (${min(closes):.0f}–${max(closes):.0f} range)."
    else:
        f["price_demand"] = ""

    # ── Drop Effects by category ─────────────────────────────────
    if effects:
        by_cat = {}
        for e in effects:
            if e.get("composite_lift") is None: continue
            cat = e.get("category", "unknown")
            if cat == "unknown" or not cat: continue
            by_cat.setdefault(cat, {"lifts": [], "r30s": [], "n": 0})
            by_cat[cat]["lifts"].append(e["composite_lift"])
            if e.get("r30") is not None:
                by_cat[cat]["r30s"].append(e["r30"])
            by_cat[cat]["n"] += 1

        cat_summary = []
        for cat, vals in sorted(by_cat.items(), key=lambda kv: -kv[1]["n"]):
            if vals["n"] < 3: continue  # skip categories with too few drops
            med_lift = stats_mod.median(vals["lifts"])
            med_r30  = stats_mod.median(vals["r30s"]) if vals["r30s"] else None
            cat_summary.append({
                "cat": cat, "n": vals["n"],
                "med_lift": med_lift,
                "med_r30":  med_r30,
            })

        lines = []
        lines.append(f"<strong>Overall pattern across {len(effects)} drops:</strong> median demand lift ×{stats_mod.median([e['composite_lift'] for e in effects if e['composite_lift'] is not None]):.2f}, median CROX 30d return after drop "
                     f"<strong>{stats_mod.median([e['r30'] for e in effects if e['r30'] is not None]):+.1f}%</strong>. "
                     f"Individual drops aren't reliable stock catalysts.")
        lines.append("<strong>Drop performance varies by category:</strong>")
        for c in cat_summary[:5]:
            r30_str = f"median 30d CROX {c['med_r30']:+.1f}%" if c["med_r30"] is not None else "30d return n/a"
            lines.append(f"&nbsp;&nbsp;&nbsp;• <strong>{CATEGORY_LABELS.get(c['cat'], c['cat'])}</strong> "
                         f"drops ({c['n']} tracked): median demand lift ×{c['med_lift']:.2f}, {r30_str}")
        f["drops_by_category"] = "<br>".join(lines)
    else:
        f["drops_by_category"] = ""

    return f


# ─────────────────────────────────────────────────────────────────────────────
# Formatters
# ─────────────────────────────────────────────────────────────────────────────
def fmt_num(v, default="—"):
    if _is_nan_or_none(v) or v == 0: return default
    if isinstance(v, float): return f"{v:,.1f}"
    return f"{v:,}"


def fmt_delta(v, bigger_is_better=True):
    if _is_nan_or_none(v): return '<span class="badge badge-na">—</span>'
    pos = v > 0
    good = pos if bigger_is_better else not pos
    cls = "badge-pos" if good else "badge-neg" if v != 0 else "badge-na"
    sign = "+" if pos else ""
    return f'<span class="badge {cls}">{sign}{v:.1f}%</span>'


def source_caption(text: str) -> str:
    return f'<div class="source-caption">{text}</div>'


def chart_insight(bullets: list) -> str:
    """Inline 'what to watch for' callout under a chart title — bullets format."""
    items = "".join(f"<li>{b}</li>" for b in bullets)
    return f'<div class="chart-insight"><span class="insight-lbl">What to watch for</span><ul>{items}</ul></div>'


# ─────────────────────────────────────────────────────────────────────────────
# Section renderers
# ─────────────────────────────────────────────────────────────────────────────
def render_overview(bear: dict, findings: dict = None) -> str:
    def tile(label, bucket_key, invert_color=False):
        b = bear[bucket_key]
        yoy = b["yoy_12mo_pct"]
        curr = b["total_12mo"]
        if curr < MIN_SIGNAL_POSTS:
            return f"""<div class="hero-tile" style="border-left:3px solid #30363d">
                <div class="hero-label">{label}</div>
                <div class="hero-val" style="color:var(--muted);font-size:18px">Thin coverage</div>
                <div class="hero-sub">{curr} posts / 12mo (&lt; {MIN_SIGNAL_POSTS} threshold)</div>
            </div>"""
        if yoy is None:
            color, badge = "#8b949e", "n/a"
        else:
            pos = yoy > 0
            # Color inversion purely for visual: red line going UP in legacy IS bad
            # for mix-shift, but we describe neutrally in the numbers.
            good_visual = (not pos) if invert_color else pos
            color = "#27ae60" if good_visual else "#e74c3c"
            badge = f"{'+' if pos else ''}{yoy:.1f}%"
        return f"""<div class="hero-tile" style="border-left:3px solid {color}">
            <div class="hero-label">{label}</div>
            <div class="hero-val" style="color:{color}">{badge}<span class="hero-val-suffix">YoY</span></div>
            <div class="hero-sub">{fmt_num(curr)} posts / 12mo · {b['n_sils']} silhouettes</div>
        </div>"""

    legacy_yoy = bear["legacy"]["yoy_12mo_pct"]
    nl_yoy     = bear["non_legacy"]["yoy_12mo_pct"]
    if nl_yoy is not None and legacy_yoy is not None:
        gap = nl_yoy - legacy_yoy
        takeaway = (f"Non-legacy Reddit volume grew <strong>{nl_yoy:+.1f}%</strong> YoY vs legacy "
                    f"<strong>{legacy_yoy:+.1f}%</strong>. Non-legacy outpaces legacy by "
                    f"<strong>{gap:+.1f}pp</strong>.")
    else:
        takeaway = "Insufficient YoY data on one or both sides."

    return f"""
<section>
  <div class="hero-row">
    {tile("Legacy demand", "legacy", invert_color=True)}
    {tile("Non-Legacy demand", "non_legacy")}
    {tile("HeyDude demand", "heydude")}
  </div>
  {source_caption("Source: Reddit post counts, trailing 12mo vs prior 12mo, volume-weighted across silhouettes. Archive: Arctic Shift (r/crocs, r/sneakers, r/heydude, r/femalefashionadvice, r/malefashionadvice). Legacy bucket = 3 silhouettes (Classic Clog, Crocband, Bayaband); non-legacy = 25 across collabs, Echo, Stomp, sandals, designer lines; HeyDude = 3.")}
  <div class="chart-card big">
    <h3>24-month category trajectory — Reddit posts per month</h3>
    {chart_insight([
        (findings or {}).get("category", ""),
        "Green line = Non-Legacy (25 silhouettes across collabs, Echo, Stomp, sandals, designer). Red line = Legacy (Classic Clog / Crocband / Bayaband). Gray = HeyDude.",
        "Spikes in the green line often align with major collab drops — cross-reference Section 06 to see which specific drops moved the needle.",
    ])}
    <div class="chart-wrap big"><canvas id="chart-traj"></canvas></div>
    {source_caption("Source: Arctic Shift archive, monthly aggregation from <code>data/reddit_arctic_monthly.csv</code>. Mentions = user posts in the 5 subreddits above whose titles match silhouette keywords.")}
  </div>
  <div class="takeaway">{takeaway}</div>
</section>"""


def render_heat(growth: dict, volume: dict, findings: dict = None) -> str:
    if not growth["labels"]:
        return '<section><div class="placeholder">No growth data yet.</div></section>'
    top3 = list(zip(growth["labels"], growth["yoy"]))[:3]
    bottom3 = list(zip(growth["labels"], growth["yoy"]))[-3:]
    top3_str = ", ".join(f"<strong>{n}</strong> {y:+.0f}%" for n, y in top3)
    bot3_str = ", ".join(f"<strong>{n}</strong> {y:+.0f}%" for n, y in bottom3)
    takeaway = (f"Biggest YoY gainers: {top3_str}. Biggest decliners: {bot3_str}.")

    return f"""
<section>
  <div class="chart-card">
    <h3>YoY growth — Reddit mentions (trailing 12mo vs prior 12mo)</h3>
    {chart_insight([
        (findings or {}).get("growth", ""),
        "<strong>How to read:</strong> bars to the right = growing YoY, bars to the left = declining. +100% = conversation doubled vs prior 12mo.",
        "<strong>Color code:</strong> green = Non-Legacy (core / sandal / premium / designer / collab). Red = Legacy. Gray = HeyDude.",
    ])}
    <div class="chart-wrap tall"><canvas id="chart-growth"></canvas></div>
    {source_caption(f"Source: Reddit post counts per silhouette via Arctic Shift archive. Only silhouettes with ≥{MIN_VOLUME_FOR_YOY} posts in the trailing 12mo are shown (filters noise from zero-baseline flips). Color = category.")}
  </div>

  <div class="chart-card">
    <h3>Where the conversation lives — absolute Reddit volume + comments (12mo)</h3>
    {chart_insight([
        (findings or {}).get("volume", ""),
        "<strong>How to read:</strong> bar length = number of user posts mentioning each silhouette in the last 12 months.",
        "Growth rate (prior chart) tells you who's <em>accelerating</em>; this tells you who has <em>scale</em>. A small silhouette doubling is less meaningful than a large one growing 20%.",
    ])}
    <div class="chart-wrap"><canvas id="chart-volume"></canvas></div>
    {source_caption("Source: Arctic Shift archive. Bar length = number of user posts matching the silhouette over the trailing 12 months. Hover for total comment count on those posts — higher comments ≈ higher engagement per post.")}
  </div>
  <div class="takeaway">{takeaway}</div>
</section>"""


def render_releases(radar: dict) -> str:
    upcoming     = radar["upcoming"][:15]
    recent_drops = radar.get("recent_drops", [])[:15]   # release/collab articles in last 30d
    recent_news  = radar["recent"][:12]                  # broader 90d coverage

    # --- LEFT COLUMN: confirmed upcoming + recent drops (actual drop events) ---
    drops_table_html = ""
    # Build unified rows: upcoming first (future-dated), then recent drops
    rows_html = ""
    for r in upcoming:
        src = r.get("source", "")
        src_badge = f'<span class="badge badge-{"pos" if src == "manual" else "na"}">{src}</span>'
        link = r.get("link", "")
        name = r.get("display_name", "")
        name_html = f'<a href="{link}" target="_blank">{name}</a>' if link else name
        rows_html += f"""<tr>
            <td class="num">{r.get('release_date') or '—'}</td>
            <td>{r.get('brand') or '—'}</td>
            <td>{name_html}</td>
            <td>{src_badge}</td>
        </tr>"""
    for r in recent_drops:
        link = r.get("link", "")
        title = r.get("title", "")
        title_html = f'<a href="{link}" target="_blank">{title}</a>' if link else title
        topic = r.get("topic", "release")
        topic_color = TOPIC_COLORS.get(topic, "#6e7781")
        topic_badge = f'<span class="topic-chip" style="background:{topic_color}22;color:{topic_color};border:1px solid {topic_color}55">{TOPIC_LABELS.get(topic, topic)}</span>'
        rows_html += f"""<tr>
            <td class="num">{r.get('date') or '—'}</td>
            <td>{r.get('brand') or '—'}</td>
            <td>{title_html}</td>
            <td>{topic_badge}</td>
        </tr>"""

    if rows_html:
        drops_table_html = f"""<div class="table-card"><table>
            <thead><tr><th class="num">Date</th><th>Brand</th><th>Drop / Title</th><th>Type</th></tr></thead>
            <tbody>{rows_html}</tbody></table></div>"""
    else:
        drops_table_html = '<div class="placeholder">No drops in the last 30 days.</div>'

    # --- RIGHT COLUMN: broader news (all topics, last 90d) ---
    if recent_news:
        news_body = ""
        for r in recent_news:
            link = r.get("link", "")
            title = r.get("title", "")
            title_html = f'<a href="{link}" target="_blank">{title}</a>' if link else title
            topic = r.get("topic", "other")
            topic_color = TOPIC_COLORS.get(topic, "#6e7781")
            topic_chip = f'<span class="topic-chip" style="background:{topic_color}22;color:{topic_color};border:1px solid {topic_color}55">{TOPIC_LABELS.get(topic, topic)}</span>'
            news_body += f"""<tr>
                <td class="num">{r.get('date') or '—'}</td>
                <td>{r.get('brand') or '—'}</td>
                <td>{title_html}</td>
                <td>{topic_chip}</td>
            </tr>"""
        news_html = f"""<div class="table-card"><table>
            <thead><tr><th class="num">Date</th><th>Brand</th><th>Headline</th><th>Topic</th></tr></thead>
            <tbody>{news_body}</tbody></table></div>"""
    else:
        news_html = '<div class="placeholder">No recent news.</div>'

    # Stat cards
    upcoming_manual = sum(1 for r in upcoming if r.get("source") == "manual")
    total_drops_in_window = len(upcoming) + len(recent_drops)

    takeaway = (f"<strong>{total_drops_in_window}</strong> drops in the last 30 days (plus any confirmed future-dated entries). "
                f"<strong>{len(recent_news)}</strong> total articles in the trailing 90 days across all topics. "
                f"Manual log currently has <strong>{upcoming_manual}</strong> upcoming entries — append to <code>config/crocs_releases.csv</code> to surface info-edge drops before public coverage.")

    return f"""
<section>
  <div class="stat-row">
    <div class="stat-card" style="border-left:3px solid {BRAND_ACCENT}">
      <div class="stat-val">{len(recent_drops)}</div>
      <div class="stat-lbl">Drops covered (last 30d)</div>
    </div>
    <div class="stat-card" style="border-left:3px solid {BRAND_ACCENT2}">
      <div class="stat-val">{len(upcoming)}</div>
      <div class="stat-lbl">Forward-dated upcoming</div>
    </div>
    <div class="stat-card" style="border-left:3px solid {BRAND_ACCENT}">
      <div class="stat-val">{radar['recent_count_30d']}</div>
      <div class="stat-lbl">All articles (trailing 30d)</div>
    </div>
  </div>

  <div class="info-box">
    <span class="info-lbl">How this section is built</span>
    <strong>Left column "Recent drops":</strong> release- and collab-topic news articles from the last 30 days, plus any confirmed future-dated drops (from your <code>config/crocs_releases.csv</code> manual log or news articles that explicitly mention a future release date). These are actual drop events.<br><br>
    <strong>Right column "All news coverage":</strong> every article about Crocs/HeyDude from the last 90 days regardless of topic — gives you the full media picture (reviews, culture, financial, not just drops).
  </div>

  <div class="dual-col">
    <div>
      <h4 class="mini-title">Recent drops (last 30 days) + confirmed upcoming</h4>
      {drops_table_html}
    </div>
    <div>
      <h4 class="mini-title">All news coverage (last 90 days, all topics)</h4>
      {news_html}
    </div>
  </div>
  <div class="takeaway">{takeaway}</div>
</section>"""


def render_news(radar: dict, findings: dict = None) -> str:
    topic_totals = radar["topic_totals"]
    total = sum(topic_totals.values()) or 1
    chips_topic = ""
    for t in ["release", "collab", "review", "culture", "financial", "other"]:
        n = topic_totals.get(t, 0)
        pct = n / total * 100
        c = TOPIC_COLORS[t]
        chips_topic += f'<span class="topic-chip" style="background:{c}22;color:{c};border:1px solid {c}55">{TOPIC_LABELS[t]} <span class="pub-count">{n:,} ({pct:.0f}%)</span></span>'

    pubs_html = ""
    if radar["publishers"]:
        pubs_html = "".join(
            f'<span class="pub-chip">{p["publisher"]} <span class="pub-count">{p["article_count"]}</span></span>'
            for p in radar["publishers"]
        )

    takeaway = (f"News coverage is {topic_totals.get('release',0)} release-themed, "
                f"{topic_totals.get('collab',0)} collab-themed, "
                f"{topic_totals.get('review',0)} review-themed, and "
                f"{topic_totals.get('culture',0)} culture/celebrity — auto-classified by headline keywords.")

    return f"""
<section>
  <div class="chart-card">
    <h3>News cadence — articles mentioning Crocs / HeyDude per month</h3>
    {chart_insight([
        (findings or {}).get("cadence", ""),
        "<strong>How to read:</strong> total news article volume per month across the entire internet (457+ publishers, aggregated via Google News). Slope matters more than absolute count.",
        "Spikes usually correspond to major drops — cross-check with the Drop Calendar section for specifics.",
    ])}
    <div class="chart-wrap"><canvas id="chart-cadence"></canvas></div>
    {source_caption("Source: Google News RSS (aggregates every publisher indexed by Google), via <code>fetch_google_news.py</code>. 24-month window.")}
  </div>
  <h4 class="mini-title">Coverage by topic</h4>
  <div class="pub-row">{chips_topic}</div>
  {source_caption("Topic classification is lightweight keyword-based on headline text. 'Release' = mentions of drop/launch/available/coming soon; 'collab' = collaboration/partnership; 'review' = hands-on/on-foot/unboxing; 'culture' = viral/spotted/trend; 'financial' = earnings/guidance/shares.")}
  <h4 class="mini-title" style="margin-top:22px">Top 15 publishers</h4>
  <div class="pub-row">{pubs_html}</div>
  <div class="takeaway">{takeaway}</div>
</section>"""


def render_stock(stock: dict, traj: dict, findings: dict = None) -> str:
    if not stock["dates"]:
        return '<section><div class="placeholder">No stock data loaded. Run <code>fetch_stock_price.py</code>.</div></section>'

    first, last = stock["closes"][0], stock["closes"][-1]
    pct_3y = (last - first) / first * 100 if first else 0
    high = max(stock["closes"])
    low  = min(stock["closes"])
    n_events = len(stock["events"])

    # Event legend chips
    crocs_manual = sum(1 for e in stock["events"] if e["brand"] == "Crocs" and e["type"] == "manual")
    crocs_news   = sum(1 for e in stock["events"] if e["brand"] == "Crocs" and e["type"] == "news")
    hd_events    = sum(1 for e in stock["events"] if e["brand"] == "HeyDude")

    takeaway = (f"CROX ${last:.2f} as of {stock['dates'][-1]} "
                f"({pct_3y:+.1f}% over 3yr window; range ${low:.2f}–${high:.2f}). "
                f"{n_events} past drops plotted: {crocs_manual} Crocs manual, {crocs_news} Crocs news-extracted, {hd_events} HeyDude.")

    return f"""
<section>
  <div class="chart-card big">
    <h3>CROX price + monthly Reddit demand + past drop events</h3>
    <div style="display:flex;gap:14px;flex-wrap:wrap;margin:0 0 14px;font-size:11px;color:var(--muted)">
      <span><span class="legend-dot" style="background:{BRAND_ACCENT}"></span> CROX close (left axis)</span>
      <span><span class="legend-dot" style="background:#58a6ff"></span> Reddit mentions / month (right axis)</span>
      <span><span class="legend-dot" style="background:{BRAND_ACCENT2};border:2px solid #fff"></span> Drop: manual log (Crocs)</span>
      <span><span class="legend-dot" style="background:#e84393;border:2px solid #fff"></span> Drop: news-extracted (Crocs)</span>
      <span><span class="legend-dot" style="background:#95a5a6;border:2px solid #fff"></span> Drop: HeyDude</span>
    </div>
    {chart_insight([
        (findings or {}).get("price_demand", ""),
        "<strong>How to read:</strong> green line = CROX daily close (left axis), blue line = monthly Reddit mentions (right axis), colored dots = past drop events. Hover any dot for the drop title.",
        "<strong>Look for:</strong> decoupling (demand rising, stock flat) = mix shift not yet priced in. Drop clusters at price troughs = hype wasn't enough. Tight correlation = market already sees what we see.",
    ])}
    <div class="chart-wrap big"><canvas id="chart-stock-combined"></canvas></div>
    {source_caption("Price: yfinance CROX daily close, 3yr window. Reddit: total monthly user-post volume across all tracked silhouettes (Arctic Shift archive, 5 subreddits). Drop events: past-date rows from <code>config/crocs_releases.csv</code> + news articles whose body text mentioned a release date that has now passed. Each drop marker is snapped to the nearest trading day, vertical line shows exact position. Hover any marker to see the drop title.")}
  </div>
  <div class="takeaway">{takeaway}</div>
</section>"""


def render_drop_effects(effects: list, findings: dict = None) -> str:
    """Top-lift drops + bottom-flops + CROX return correlation."""
    if not effects:
        return '<section><div class="placeholder">No drop-effect data yet. Run <code>fetch_google_news.py</code> + <code>fetch_reddit_arctic.py</code> + <code>fetch_stock_price.py</code> first.</div></section>'

    def row(e):
        def lift_cell(lift_val, pre, post):
            if lift_val is None:
                return '<td class="num"><span class="badge badge-na">—</span></td>'
            cls = "badge-pos" if lift_val > 1.05 else ("badge-neg" if lift_val < 0.95 else "badge-na")
            return (f'<td class="num"><span class="badge {cls}">×{lift_val:.2f}</span>'
                    f'<div class="cell-aux">{pre}→{post}</div></td>')
        def ret_cell(v):
            if v is None:
                return '<td class="num"><span class="badge badge-na">—</span></td>'
            cls = "badge-pos" if v > 0 else ("badge-neg" if v < 0 else "badge-na")
            sign = "+" if v > 0 else ""
            return f'<td class="num"><span class="badge {cls}">{sign}{v:.1f}%</span></td>'
        def comp_cell(v):
            if v is None: return '<td class="num"><span class="badge badge-na">—</span></td>'
            cls = "badge-pos" if v > 1.25 else ("badge-neg" if v < 0.75 else "badge-na")
            return f'<td class="num"><span class="badge {cls}" style="font-size:11px">×{v:.2f}</span></td>'
        brand_color = "#95a5a6" if e["brand"] == "HeyDude" else ("#F58025" if e["type"] == "manual" else "#e84393")
        title_clean = e['label'].replace('"', '&quot;')
        return f"""<tr>
            <td class="num">{e['date']}</td>
            <td><span class="dot" style="background:{brand_color}"></span>{e['brand']}</td>
            <td title="{title_clean}">{e['label'][:58]}{'…' if len(e['label']) > 58 else ''}</td>
            {comp_cell(e['composite_lift'])}
            {lift_cell(e['reddit_lift'], e['reddit_pre'], e['reddit_post'])}
            {lift_cell(e['news_lift'],   e['news_pre'],   e['news_post'])}
            {ret_cell(e['r7'])}
            {ret_cell(e['r30'])}
        </tr>"""

    # Split: top-lift (where composite > 1.0) sorted desc; flops (composite < 1.0) sorted asc
    with_lift = [e for e in effects if e["composite_lift"] is not None]
    top15 = with_lift[:15]
    flops = sorted([e for e in with_lift if e["composite_lift"] < 1.0], key=lambda x: x["composite_lift"])[:10]

    top_rows  = "".join(row(e) for e in top15)
    flop_rows = "".join(row(e) for e in flops) if flops else '<tr><td colspan="8" style="text-align:center;color:var(--muted);padding:14px">No drops with composite lift &lt; 1.0 — either all drops moved the needle, or the window is too short to see decay.</td></tr>'

    # Summary stats
    pos_7d    = sum(1 for e in with_lift if e["r7"] is not None and e["r7"] > 0)
    pos_30d   = sum(1 for e in with_lift if e["r30"] is not None and e["r30"] > 0)
    with_r7   = sum(1 for e in with_lift if e["r7"]  is not None)
    with_r30  = sum(1 for e in with_lift if e["r30"] is not None)
    hit_7d    = (pos_7d / with_r7 * 100) if with_r7 else 0
    hit_30d   = (pos_30d / with_r30 * 100) if with_r30 else 0

    # Did composite lift correlate with return? Simple: of top-quartile lift drops, what % had positive 30d return?
    top_q = sorted([e for e in with_lift if e["r30"] is not None], key=lambda x: -x["composite_lift"])[:max(len(with_lift)//4, 1)]
    top_q_pos_30d = sum(1 for e in top_q if e["r30"] > 0)
    top_q_hit = (top_q_pos_30d / len(top_q) * 100) if top_q else 0

    takeaway = (f"{len(with_lift)} drops with usable lift data. "
                f"<strong>{hit_7d:.0f}%</strong> had positive CROX 7d return, <strong>{hit_30d:.0f}%</strong> positive 30d return. "
                f"Top-quartile demand-lift drops: <strong>{top_q_hit:.0f}%</strong> positive 30d return. "
                f"Lift ratio ×1.00 = no change; ×2.00 = demand doubled post-drop.")

    # Simple scatter: composite lift vs 30d return
    scatter_points = [
        {"x": e["composite_lift"], "y": e["r30"], "label": e["label"][:50], "date": e["date"]}
        for e in with_lift
        if e["composite_lift"] is not None and e["r30"] is not None
    ]
    scatter_json = json.dumps(scatter_points)

    return f"""
<section>
  <div class="takeaway">{takeaway}</div>

  <div class="chart-card">
    <h3>Demand lift vs CROX 30-day return — each dot is one drop</h3>
    {chart_insight([
        (findings or {}).get("drops_by_category", ""),
        "<strong>How to read:</strong> X = demand lift (post/pre ratio, ±14d window). Y = CROX 30d return. Upper-right = drops that moved demand AND the stock. Lower-right = demand moved, stock didn't. Upper-left = stock moved without demand lift.",
    ])}
    <div class="chart-wrap"><canvas id="chart-drops-scatter"></canvas></div>
    {source_caption("X-axis: composite demand lift (avg of Reddit post, news article, YouTube upload post/pre ratios in ±14d window around drop). Y-axis: CROX % return from drop date to 30 days later. Top-right quadrant = drops that moved both demand and stock.")}
    <script id="drops-scatter-data" type="application/json">{scatter_json}</script>
  </div>

  <h4 class="mini-title">Top 15 drops by composite demand lift</h4>
  <div class="table-card"><table class="drop-effects-table">
    <thead>
      <tr>
        <th colspan="3" class="grp-header">DROP</th>
        <th colspan="3" class="grp-header grp-demand">DEMAND LIFT (post ÷ pre, ±14d window)</th>
        <th colspan="2" class="grp-header grp-stock">CROX RETURN AFTER DROP</th>
      </tr>
      <tr>
        <th class="num">Date</th><th>Brand</th><th>Title</th>
        <th class="num sub">Composite</th>
        <th class="num sub">Reddit</th>
        <th class="num sub">News</th>
        <th class="num sub">7 days</th>
        <th class="num sub">30 days</th>
      </tr>
    </thead>
    <tbody>{top_rows}</tbody>
  </table></div>
  {source_caption("<strong>How to read:</strong> Composite lift = average of Reddit and News pre→post ratios. ×1.00 means no change; ×2.00 means conversation doubled post-drop; ×0.50 means fell in half. Small gray number under each lift shows the raw post-drop / pre-drop counts. CROX return = % change from drop-day close to 7d and 30d later. Note: drops within 30 days of today may have null 30d return.")}

  <h4 class="mini-title" style="margin-top:24px">Bottom drops — where demand <em>fell</em> post-release</h4>
  <div class="table-card"><table class="drop-effects-table">
    <thead>
      <tr>
        <th colspan="3" class="grp-header">DROP</th>
        <th colspan="3" class="grp-header grp-demand">DEMAND LIFT (post ÷ pre, ±14d window)</th>
        <th colspan="2" class="grp-header grp-stock">CROX RETURN AFTER DROP</th>
      </tr>
      <tr>
        <th class="num">Date</th><th>Brand</th><th>Title</th>
        <th class="num sub">Composite</th>
        <th class="num sub">Reddit</th>
        <th class="num sub">News</th>
        <th class="num sub">7 days</th>
        <th class="num sub">30 days</th>
      </tr>
    </thead>
    <tbody>{flop_rows}</tbody>
  </table></div>
  {source_caption("Drops where 14-day post-drop demand was lower than the 14 days prior — either real flops, or the drop was already fully priced-in during pre-announcement coverage.")}
</section>"""


def render_full_heat(heat: list) -> str:
    body = ""
    for r in heat:
        cat = r["category"]
        cat_color = CATEGORY_COLORS.get(cat, "#95a5a6")
        cat_badge = f'<span class="dot" style="background:{cat_color}"></span>{CATEGORY_LABELS.get(cat, cat)}'
        body += f"""<tr>
            <td><strong>{r['display_name']}</strong></td>
            <td>{cat_badge}</td>
            <td class="num">{fmt_num(r['reddit_12mo'])}</td>
            <td class="num">{fmt_delta(r['reddit_yoy12_pct'])}</td>
            <td class="num">{fmt_num(r['total_score'])}</td>
            <td class="num">{fmt_num(r['total_comments'])}</td>
            <td class="num">{fmt_num(r['yt_views_12mo'])}</td>
            <td class="num">{fmt_num(r['news_12mo'])}</td>
            <td class="num">{fmt_delta(r['news_yoy_pct'])}</td>
            <td class="num" style="color:var(--muted)">{r['heat_score']}</td>
        </tr>"""
    return f"""<div class="table-card"><table>
        <thead><tr>
          <th>Silhouette</th><th>Category</th>
          <th class="num">Reddit 12mo</th><th class="num">Reddit YoY</th>
          <th class="num">Total score</th><th class="num">Total comments</th>
          <th class="num">YT views 12mo</th>
          <th class="num">News 12mo</th><th class="num">News YoY</th>
          <th class="num">Heat</th>
        </tr></thead>
        <tbody>{body}</tbody>
    </table></div>"""


# ─────────────────────────────────────────────────────────────────────────────
# HTML shell
# ─────────────────────────────────────────────────────────────────────────────
def build_html(d: dict) -> str:
    radar  = compute_release_radar(d)
    heat   = compute_heat_map(d)
    traj   = compute_trajectory(d)
    bear   = compute_bear_case(d, heat)
    growth = compute_growth_leaders(heat)
    volume = compute_volume_leaders(heat, top_n=12)
    stock  = compute_stock_overlay(d)
    effects = compute_drop_effects(d, stock)
    summary = compute_summary(d, bear, heat, radar, effects, stock)
    summary_html = render_summary_modal(summary)
    findings = compute_findings(d, bear, heat, radar, effects, stock)

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    chart_blob = json.dumps({
        "traj":    traj,
        "growth":  growth,
        "volume":  volume,
        "cadence": radar["cadence"],
        "stock":   stock,
    })

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{BRAND_NAME} Demand Acceleration — Intelligence Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns/dist/chartjs-adapter-date-fns.bundle.min.js"></script>
<style>
  /* Crocs-brand warm light palette — inspired by crocs.com homepage */
  :root {{
    --bg:       #faf6ef;  /* warm cream background */
    --surface:  #ffffff;  /* card surface (pure white for chart clarity) */
    --surface2: #fcf9f3;  /* secondary surface for subtle stripes */
    --border:   #e4dccd;  /* warm beige border */
    --border-strong: #d4c8ad;
    --text:     #2d2921;  /* dark warm brown, not pure black */
    --text-soft:#4a4238;
    --muted:    #8b8271;  /* warm muted gray */
    --accent:   #3F9C35;  /* Crocs brand green */
    --accent2:  #f4a989;  /* warm peach (Classic homepage tile) */
    --accent3:  #c5d670;  /* olive / Crafted green */
    --accent4:  #ffd84d;  /* Crocs banner yellow */
    --info:     #6b94b1;  /* Crocband pale blue */
    --pos:      #3F9C35;  /* positive = Crocs green */
    --neg:      #c95d4a;  /* negative = warm terracotta (not blood red) */
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html {{ scroll-behavior: smooth; }}
  body {{ background: var(--bg); color: var(--text);
         font-family: 'Inter', 'Segoe UI', system-ui, sans-serif;
         font-size: 14px; line-height: 1.6; }}
  a {{ color: var(--text); text-decoration: none; }}
  a:hover {{ color: var(--accent); }}

  .topbar {{ background: var(--surface); border-bottom: 1px solid var(--border);
             padding: 12px 32px; display: flex; align-items: center;
             justify-content: space-between; gap: 20px;
             position: sticky; top: 0; z-index: 100;
             box-shadow: 0 1px 0 rgba(45,41,33,0.03); }}
  .topbar h1 {{ font-size: 16px; font-weight: 700; letter-spacing: -0.3px; color: var(--text); }}
  .topbar .meta {{ color: var(--muted); font-size: 11px; }}
  .topbar-nav {{ display: flex; gap: 4px; flex-wrap: nowrap; }}
  .nav-btn {{ font-size: 11px; font-weight: 600; letter-spacing: 0.3px;
              color: var(--muted); background: transparent;
              border: 1px solid var(--border); border-radius: 999px;
              padding: 5px 12px; white-space: nowrap;
              transition: color .15s, border-color .15s, background .15s; }}
  .nav-btn:hover {{ color: var(--accent); border-color: var(--accent); background: rgba(63,156,53,0.06); }}
  .summary-btn {{ background: var(--accent); color: #fff; border: none;
                  border-radius: 999px; padding: 7px 16px;
                  font-size: 12px; font-weight: 700; letter-spacing: 0.3px;
                  cursor: pointer; white-space: nowrap;
                  transition: background .15s, transform .1s; }}
  .summary-btn:hover {{ background: #348029; transform: translateY(-1px); }}
  .summary-btn:active {{ transform: translateY(0); }}

  .container {{ max-width: 1280px; margin: 0 auto; padding: 24px 32px; }}

  .section-header {{ margin: 44px 0 14px; padding-bottom: 10px;
                     border-bottom: 1px solid var(--border); }}
  .section-header:first-of-type {{ margin-top: 0; }}
  .section-num {{ font-size: 10px; font-weight: 700; color: var(--accent); letter-spacing: 1.8px; }}
  .section-title {{ font-size: 21px; font-weight: 700; margin-top: 4px; letter-spacing: -0.4px; color: var(--text); }}
  .section-blurb {{ font-size: 13px; color: var(--text-soft); line-height: 1.6;
                    margin-top: 12px; padding: 12px 16px; max-width: 880px;
                    background: #fef9ec; border: 1px solid #f2e4b6;
                    border-left: 4px solid var(--accent4); border-radius: 6px; }}
  .section-blurb strong {{ color: var(--text); font-weight: 700; }}

  section {{ margin-bottom: 16px; }}

  .hero-row {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 14px; }}
  .hero-tile {{ background: var(--surface); border: 1px solid var(--border);
                border-radius: 10px; padding: 20px 22px;
                box-shadow: 0 1px 3px rgba(45,41,33,0.04); }}
  .hero-label {{ font-size: 10px; color: var(--muted); text-transform: uppercase;
                 letter-spacing: 0.8px; margin-bottom: 10px; font-weight: 700; }}
  .hero-val {{ font-size: 34px; font-weight: 700; letter-spacing: -0.8px; color: var(--text); }}
  .hero-val-suffix {{ font-size: 12px; color: var(--muted); font-weight: 500; margin-left: 8px; }}
  .hero-sub {{ font-size: 11.5px; color: var(--muted); margin-top: 6px; }}
  .hero-frame {{ font-size: 11px; color: var(--muted); font-style: italic; margin-top: 8px; }}

  .chart-card {{ background: var(--surface); border: 1px solid var(--border);
                 border-radius: 10px; padding: 20px 22px; margin-bottom: 14px;
                 box-shadow: 0 1px 3px rgba(45,41,33,0.04); }}
  .chart-card h3 {{ font-size: 14px; font-weight: 700; margin-bottom: 14px;
                    letter-spacing: -0.2px; color: var(--text); }}
  .chart-wrap {{ position: relative; height: 300px; }}
  .chart-wrap.big {{ height: 380px; }}
  .chart-wrap.tall {{ height: 480px; }}

  .chart-insight {{ background: linear-gradient(90deg, rgba(63,156,53,0.08), rgba(63,156,53,0.02));
                    border: 1px solid #cfe4c8; border-left: 4px solid var(--accent);
                    border-radius: 6px; padding: 11px 16px 11px 18px;
                    font-size: 12.5px; color: var(--text-soft); line-height: 1.6;
                    margin: 10px 0 16px; }}
  .chart-insight .insight-lbl {{ display: block; font-size: 9.5px; font-weight: 700;
                                 letter-spacing: 1.4px; text-transform: uppercase;
                                 color: var(--accent); margin-bottom: 6px; }}
  .chart-insight strong {{ color: var(--text); font-weight: 700; }}
  .chart-insight ul {{ margin: 6px 0 0 0; padding-left: 18px; }}
  .chart-insight li {{ margin-bottom: 4px; }}
  .source-caption {{ font-size: 10.5px; color: var(--muted);
                     line-height: 1.55; margin-top: 12px;
                     padding-top: 10px; border-top: 1px dashed var(--border);
                     font-style: italic; }}
  .source-caption code {{ color: var(--accent); font-family: 'SF Mono', Menlo, Consolas, monospace;
                          font-size: 10.5px; font-style: normal;
                          background: rgba(63,156,53,0.08); padding: 1px 5px; border-radius: 3px; }}
  .source-caption strong {{ color: var(--text-soft); }}

  .takeaway {{ background: #f0f7ec; border: 1px solid #cfe4c8;
               border-left: 4px solid var(--accent); border-radius: 6px;
               padding: 12px 18px; font-size: 13px; color: var(--text-soft);
               line-height: 1.65; margin: 10px 0 18px; }}
  .takeaway strong {{ color: var(--text); font-weight: 700; }}

  .info-box {{ background: #fef4e9; border: 1px solid #f5d9ba;
               border-left: 4px solid var(--accent2); border-radius: 6px;
               padding: 12px 18px; font-size: 12.5px; color: var(--text-soft);
               line-height: 1.7; margin-bottom: 14px; }}
  .info-box .info-lbl {{ display: block; font-size: 9px; font-weight: 700;
                         letter-spacing: 1.4px; text-transform: uppercase;
                         color: #b8682d; margin-bottom: 6px; }}
  .info-box code {{ color: #b8682d; font-family: 'SF Mono', Menlo, Consolas, monospace;
                    font-size: 11.5px; background: rgba(244,169,137,0.2); padding: 1px 5px; border-radius: 3px; }}
  .info-box strong {{ color: var(--text); font-weight: 700; }}

  .stat-row {{ display: flex; gap: 14px; margin-bottom: 14px; flex-wrap: wrap; }}
  .stat-card {{ background: var(--surface); border: 1px solid var(--border);
                border-radius: 10px; padding: 16px 22px; min-width: 160px; flex: 1 1 160px;
                box-shadow: 0 1px 3px rgba(45,41,33,0.04); }}
  .stat-val {{ font-size: 28px; font-weight: 700; letter-spacing: -0.5px; color: var(--text); }}
  .stat-lbl {{ font-size: 10px; color: var(--muted); text-transform: uppercase;
               letter-spacing: 0.8px; margin-top: 4px; font-weight: 600; }}

  .mini-title {{ font-size: 11px; font-weight: 700; text-transform: uppercase;
                 letter-spacing: 1px; color: var(--muted); margin-bottom: 10px; }}
  .dual-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 18px; }}
  @media (max-width: 1024px) {{ .dual-col {{ grid-template-columns: 1fr; }} }}

  .table-card {{ background: var(--surface); border: 1px solid var(--border);
                 border-radius: 10px; overflow: auto;
                 box-shadow: 0 1px 3px rgba(45,41,33,0.04); }}
  table {{ width: 100%; border-collapse: collapse; }}
  th {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.6px;
        color: var(--muted); font-weight: 700; padding: 10px 12px;
        border-bottom: 1px solid var(--border); text-align: left;
        background: var(--surface2); white-space: nowrap; }}
  td {{ padding: 10px 12px; border-bottom: 1px solid var(--border); font-size: 12.5px; color: var(--text); }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(63,156,53,0.04); }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  th.num {{ text-align: right; }}
  td.muted-cell {{ color: var(--muted); font-size: 11.5px; }}
  td strong {{ color: var(--text); font-weight: 700; }}
  .dot {{ display: inline-block; width: 9px; height: 9px; border-radius: 50%;
          margin-right: 8px; vertical-align: middle; }}

  .badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px;
            font-size: 10px; font-weight: 700; letter-spacing: 0.3px; white-space: nowrap; }}
  .badge-pos {{ background: #e1f2dc; color: #2a7a20; }}
  .badge-neg {{ background: #f8e2dc; color: #b34738; }}
  .badge-na  {{ background: #eee7d6; color: #8b8271; }}
  .badge-mid {{ background: #fdefc9; color: #8a6b10; }}

  .placeholder {{ background: var(--surface2); border: 1px dashed var(--border-strong);
                  border-radius: 8px; padding: 22px 18px; text-align: center;
                  color: var(--muted); font-size: 12.5px; }}
  .placeholder code {{ color: var(--accent); font-family: 'SF Mono', Menlo, Consolas, monospace; }}

  .pub-row {{ display: flex; flex-wrap: wrap; gap: 6px; margin: 6px 0 10px; }}
  .pub-chip {{ background: var(--surface2); border: 1px solid var(--border);
               border-radius: 999px; padding: 4px 12px; font-size: 11.5px; color: var(--text-soft); }}
  .pub-count {{ color: var(--muted); margin-left: 8px; font-variant-numeric: tabular-nums; }}
  .topic-chip {{ display: inline-block; font-size: 11px; font-weight: 600;
                 padding: 3px 10px; border-radius: 999px; white-space: nowrap; }}

  .legend-dot {{ display: inline-block; width: 10px; height: 10px;
                 border-radius: 50%; margin-right: 6px; vertical-align: middle; }}

  /* Drop Effects table — grouped headers + aux line under lift */
  .drop-effects-table th.grp-header {{
    background: var(--surface2); border-bottom: 2px solid var(--border-strong);
    text-align: center; padding: 9px 10px;
    font-size: 10px; font-weight: 700; letter-spacing: 1.2px;
    color: var(--muted);
  }}
  .drop-effects-table th.grp-header.grp-demand {{ color: {BRAND_ACCENT}; border-bottom-color: #b8ddb2; }}
  .drop-effects-table th.grp-header.grp-stock  {{ color: #b8682d; border-bottom-color: #f5d9ba; }}
  .drop-effects-table th.sub {{ font-size: 9.5px; font-weight: 600; padding: 7px 10px; }}
  .drop-effects-table td:nth-child(4) {{ border-left: 1px solid #d0e7cd; background: rgba(63,156,53,0.04); }}
  .drop-effects-table td:nth-child(7) {{ border-left: 1px solid #f5d9ba; background: rgba(244,169,137,0.05); }}
  .cell-aux {{ display: block; font-size: 9.5px; color: var(--muted);
               margin-top: 3px; font-variant-numeric: tabular-nums; }}

  .bear-row {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 14px; margin-bottom: 12px; }}
  .bear-tile {{ background: var(--surface); border: 1px solid var(--border);
                border-radius: 10px; padding: 16px 20px;
                box-shadow: 0 1px 3px rgba(45,41,33,0.04); }}
  .bear-label {{ font-size: 11px; color: var(--muted); text-transform: uppercase;
                 letter-spacing: 0.6px; margin-bottom: 10px; font-weight: 700; }}
  .bear-value {{ font-size: 24px; font-weight: 700; letter-spacing: -0.5px; color: var(--text); margin-bottom: 4px; }}
  .bear-frame {{ font-size: 11px; color: var(--muted); font-style: italic; margin-top: 6px; }}

  .detail-toggle {{ text-align: center; margin: 16px 0; }}
  .detail-btn {{ display: inline-block; background: var(--surface);
                 border: 2px solid var(--accent); color: var(--accent);
                 border-radius: 999px; padding: 10px 26px;
                 font-size: 12px; font-weight: 700; letter-spacing: 0.8px;
                 text-transform: uppercase; cursor: pointer;
                 transition: background .15s, color .15s; }}
  .detail-btn:hover {{ background: var(--accent); color: #fff; }}
  .detail-panel {{ display: none; margin-top: 18px; }}
  .detail-panel.open {{ display: block; }}

  /* Summary modal */
  .summary-overlay {{ display: none; position: fixed; inset: 0;
                      background: rgba(45,41,33,0.5); backdrop-filter: blur(4px);
                      z-index: 200; }}
  .summary-overlay.open {{ display: block; }}
  .summary-modal {{ display: none; position: fixed;
                    top: 5vh; left: 50%; transform: translateX(-50%);
                    width: min(780px, 92vw); max-height: 90vh; overflow-y: auto;
                    background: var(--surface); border: 1px solid var(--border);
                    border-radius: 14px; z-index: 201;
                    box-shadow: 0 20px 60px rgba(45,41,33,0.25); }}
  .summary-modal.open {{ display: block; }}
  .summary-close {{ position: absolute; top: 14px; right: 16px;
                    background: transparent; border: none;
                    font-size: 30px; line-height: 1; color: var(--muted);
                    cursor: pointer; padding: 4px 10px; }}
  .summary-close:hover {{ color: var(--text); }}
  .summary-body {{ padding: 30px 36px 32px; }}
  .summary-eyebrow {{ font-size: 10px; color: var(--accent); font-weight: 700;
                      text-transform: uppercase; letter-spacing: 1.4px; margin-bottom: 6px; }}
  .summary-modal h2 {{ font-size: 24px; font-weight: 700; color: var(--text);
                       margin-bottom: 14px; letter-spacing: -0.5px; }}
  .summary-modal h3 {{ font-size: 12px; font-weight: 700; color: var(--muted);
                       text-transform: uppercase; letter-spacing: 1.2px;
                       margin: 22px 0 8px; padding-bottom: 6px;
                       border-bottom: 1px solid var(--border); }}
  .summary-thesis {{ background: #f0f7ec; border: 1px solid #cfe4c8;
                     border-left: 4px solid var(--accent); border-radius: 6px;
                     padding: 14px 18px; font-size: 14px; color: var(--text-soft);
                     line-height: 1.7; margin-bottom: 10px; }}
  .summary-thesis strong {{ color: var(--text); font-weight: 700; }}
  .sm-tile-row {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }}
  @media (max-width: 640px) {{ .sm-tile-row {{ grid-template-columns: 1fr 1fr; }} }}
  .sm-tile {{ background: var(--surface2); border: 1px solid var(--border);
              border-radius: 8px; padding: 12px 14px; }}
  .sm-tile-val {{ font-size: 20px; font-weight: 700; color: var(--text);
                  letter-spacing: -0.4px; }}
  .sm-tile-lbl {{ font-size: 10.5px; color: var(--muted);
                  text-transform: uppercase; letter-spacing: 0.6px;
                  margin-top: 2px; font-weight: 600; line-height: 1.3; }}
  .summary-list {{ list-style: none; padding: 0; margin: 0; }}
  .summary-list li {{ padding: 7px 0 7px 18px; font-size: 13px; color: var(--text-soft);
                      line-height: 1.55; position: relative; }}
  .summary-list li::before {{ content: "▸"; color: var(--accent);
                              position: absolute; left: 0; font-weight: 700; }}
  .summary-list li strong {{ color: var(--text); font-weight: 700; }}
  .summary-footer {{ font-size: 11px; color: var(--muted); font-style: italic;
                     margin-top: 28px; padding-top: 14px;
                     border-top: 1px solid var(--border); line-height: 1.55; }}
  .summary-footer code {{ color: var(--accent); font-family: 'SF Mono', Menlo, Consolas, monospace;
                          font-size: 10.5px; background: rgba(63,156,53,0.06); padding: 1px 4px; border-radius: 3px; }}

  footer {{ color: var(--muted); font-size: 11px; text-align: center;
            padding: 36px 0 24px; border-top: 1px solid var(--border);
            margin-top: 32px; }}
</style>
</head>
<body>

<div class="topbar">
  <h1>{BRAND_NAME} <span style="color:var(--muted);font-weight:400">— Demand Acceleration Intelligence</span></h1>
  <nav class="topbar-nav">
    <a class="nav-btn" href="#s1">Price vs Demand</a>
    <a class="nav-btn" href="#s2">Category Trend</a>
    <a class="nav-btn" href="#s3">Silhouette Heat</a>
    <a class="nav-btn" href="#s4">Drop Calendar</a>
    <a class="nav-btn" href="#s5">Drop Effects</a>
    <a class="nav-btn" href="#s6">News &amp; Topics</a>
    <a class="nav-btn" href="#s7">Full Table</a>
  </nav>
  <button class="summary-btn" onclick="openSummary()">📋 Generate Summary</button>
  <div class="meta">Updated {generated_at}</div>
</div>

{summary_html}

<div class="container">

<div class="section-header" id="s1">
  <div class="section-num">01 — PRICE vs DEMAND</div>
  <div class="section-title">CROX stock + Reddit demand + every past drop — one chart</div>
  <div class="section-blurb"><strong>Why it matters:</strong> This is the big-picture view for the whole thesis in one chart. Does Reddit demand move with CROX price, or is there a gap between them? Every past drop is plotted as a colored dot — so you can immediately see which drops coincided with demand spikes and which ones didn't move anything. If demand is clearly rising but CROX is flat or falling, the mix-shift story hasn't been priced in yet — that gap is where the opportunity lives.</div>
</div>
{render_stock(stock, traj, findings)}

<div class="section-header" id="s2">
  <div class="section-num">02 — CATEGORY TREND</div>
  <div class="section-title">Is the mix shift showing up in demand?</div>
  <div class="section-blurb"><strong>Why it matters:</strong> Zooms into the demand component from Section 01. The bull case rests on Non-Legacy silhouettes (collabs, Echo, Stomp, sandals, designer lines) growing faster than Legacy (Classic Clog / Crocband / Bayaband). If Non-Legacy YoY is meaningfully higher than Legacy YoY (≥15pp gap), mix shift is real. If Legacy is matching or beating Non-Legacy, the thesis is not showing up in leading signals.</div>
</div>
{render_overview(bear, findings)}

<div class="section-header" id="s3">
  <div class="section-num">03 — SILHOUETTE HEAT</div>
  <div class="section-title">Which specific silhouettes are driving the trend?</div>
  <div class="section-blurb"><strong>Why it matters:</strong> Section 02 tells you <em>if</em> the category is moving. This section tells you <em>which specific silhouettes are doing the work</em> — and which ones are fading. If a handful of hot collabs (Bembury Pollex, Steven Smith line, Bad Bunny, MSCHF) are carrying growth while Classic Clog fades, that's a mix-shift story. If legacy silhouettes are the ones accelerating, the narrative has to be reframed.</div>
</div>
{render_heat(growth, volume, findings)}

<div class="section-header" id="s4">
  <div class="section-num">04 — DROP CALENDAR</div>
  <div class="section-title">What's dropping next + what just landed</div>
  <div class="section-blurb"><strong>Why it matters:</strong> The upcoming list is an early-warning system — drops announced today hit the market in weeks, before consensus notices. Recent coverage tells you the <em>type</em> of stories the press is running (release / collab / review / culture / financial). Paired with Section 05 below which shows how past drops actually performed.</div>
</div>
{render_releases(radar)}

<div class="section-header" id="s5">
  <div class="section-num">05 — DROP EFFECTS</div>
  <div class="section-title">Do drops actually move the stock — and which <em>types</em> of drops move it most?</div>
  <div class="section-blurb"><strong>Why it matters:</strong> The empirical stress-test for the "drops drive CROX" hypothesis. For every past drop, we measure demand lift (did people talk about it more after?) and CROX return 30 days later (did the stock care?) — broken out by drop category (collab vs designer vs sandal vs legacy) so you can see which <em>type</em> of drop has the biggest impact. Individual drops aren't reliable stock catalysts on their own; the pattern lives in aggregate across drop types.</div>
</div>
{render_drop_effects(effects, findings)}

<div class="section-header" id="s6">
  <div class="section-num">06 — NEWS & TOPICS</div>
  <div class="section-title">Is Crocs staying culturally relevant?</div>
  <div class="section-blurb"><strong>Why it matters:</strong> Brands that fade from culture lose pricing power before they lose unit volume. News-article cadence is a proxy for how often Crocs is in the conversation; publisher diversity (464+ distinct outlets) proves the signal isn't coming from one fanboy blog. Topic mix tells you whether coverage is about new drops (thesis-positive), earnings/financials (neutral), or fading reviews (thesis-negative).</div>
</div>
{render_news(radar, findings)}

<div class="section-header" id="s7">
  <div class="section-num">07 — FULL TABLE</div>
  <div class="section-title">Per-silhouette detail — drill-down</div>
  <div class="section-blurb"><strong>Why it matters:</strong> Every tracked silhouette with every metric. Hidden by default because the prior sections already summarize it — open when you want to verify a specific claim or cross-check a silhouette you're curious about.</div>
</div>
<div class="detail-toggle">
  <button class="detail-btn" onclick="document.getElementById('detail-panel').classList.toggle('open'); this.textContent = this.textContent.trim() === 'Show full heat map →' ? '← Hide full heat map' : 'Show full heat map →';">
    Show full heat map →
  </button>
</div>
<div class="detail-panel" id="detail-panel">
  {render_full_heat(heat)}
  {source_caption("Columns: Reddit 12mo = posts/trailing 12mo; Reddit YoY = vs prior 12mo; Total score = sum of Reddit post scores (upvotes − downvotes); Total comments = sum of comment counts on those posts; YT views 12mo = total views on relevance-filtered YouTube videos published in trailing 12mo; News 12mo = Google News articles tagged to silhouette; Heat = composite rank across metrics.")}
</div>

<footer>
  {BRAND_NAME} Demand Dashboard · generated {generated_at}
</footer>
</div>

<script>
  // Summary modal open/close
  function openSummary() {{
    document.getElementById('summary-overlay').classList.add('open');
    document.getElementById('summary-modal').classList.add('open');
    document.body.style.overflow = 'hidden';
  }}
  function closeSummary() {{
    document.getElementById('summary-overlay').classList.remove('open');
    document.getElementById('summary-modal').classList.remove('open');
    document.body.style.overflow = '';
  }}
  document.addEventListener('keydown', (e) => {{ if (e.key === 'Escape') closeSummary(); }});

  window.__crocs = {chart_blob};

  (function() {{
    const d = window.__crocs;

    // ─── 24-month Reddit trajectory ─────────────────────────────────
    if (d.traj && d.traj.labels && d.traj.labels.length) {{
      new Chart(document.getElementById('chart-traj'), {{
        type: 'line',
        data: {{
          labels: d.traj.labels,
          datasets: [
            {{ label: 'Legacy',     data: d.traj.legacy,     borderColor: '#e74c3c',
               backgroundColor: 'rgba(231,76,60,0.10)', borderWidth: 2.5, tension: 0.35, pointRadius: 2, fill: false }},
            {{ label: 'Non-Legacy', data: d.traj.non_legacy, borderColor: '{BRAND_ACCENT}',
               backgroundColor: 'rgba(6,165,80,0.10)',  borderWidth: 2.5, tension: 0.35, pointRadius: 2, fill: false }},
            {{ label: 'HeyDude',    data: d.traj.heydude,    borderColor: '#95a5a6',
               backgroundColor: 'rgba(149,165,166,0.10)', borderWidth: 2, tension: 0.35, pointRadius: 2, fill: false }},
          ]
        }},
        options: {{
          plugins: {{ legend: {{ labels: {{ color: '#6b6456', boxWidth: 12 }} }} }},
          scales: {{
            x: {{ ticks: {{ color: '#6b6456', maxRotation: 45, minRotation: 45 }}, grid: {{ color: '#ebe4d2' }} }},
            y: {{ ticks: {{ color: '#6b6456' }}, grid: {{ color: '#ebe4d2' }}, beginAtZero: true }},
          }},
          maintainAspectRatio: false,
        }},
      }});
    }}

    // ─── Growth leaders bar chart ────────────────────────────────────
    if (d.growth && d.growth.labels && d.growth.labels.length) {{
      new Chart(document.getElementById('chart-growth'), {{
        type: 'bar',
        data: {{
          labels: d.growth.labels,
          datasets: [{{ data: d.growth.yoy, backgroundColor: d.growth.colors, borderWidth: 0 }}]
        }},
        options: {{
          indexAxis: 'y',
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{ callbacks: {{ label: (ctx) => ctx.raw.toFixed(1) + '% YoY (' + d.growth.volumes[ctx.dataIndex] + ' posts/12mo)' }} }}
          }},
          scales: {{
            x: {{ ticks: {{ color: '#6b6456', callback: v => v + '%' }}, grid: {{ color: '#ebe4d2' }},
                  title: {{ display: true, text: 'YoY % change', color: '#6b6456' }} }},
            y: {{ ticks: {{ color: '#2d2921', font: {{ size: 11 }} }}, grid: {{ color: 'transparent' }} }},
          }},
          maintainAspectRatio: false,
        }},
      }});
    }}

    // ─── Volume bar chart ───────────────────────────────────────────
    if (d.volume && d.volume.labels && d.volume.labels.length) {{
      new Chart(document.getElementById('chart-volume'), {{
        type: 'bar',
        data: {{
          labels: d.volume.labels,
          datasets: [{{ data: d.volume.volumes, backgroundColor: d.volume.colors, borderWidth: 0 }}]
        }},
        options: {{
          indexAxis: 'y',
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{ callbacks: {{ label: (ctx) => ctx.raw + ' posts/12mo · ' + d.volume.comments[ctx.dataIndex] + ' total comments' }} }}
          }},
          scales: {{
            x: {{ ticks: {{ color: '#6b6456' }}, grid: {{ color: '#ebe4d2' }},
                  title: {{ display: true, text: 'Reddit posts, trailing 12mo', color: '#6b6456' }} }},
            y: {{ ticks: {{ color: '#2d2921', font: {{ size: 11 }} }}, grid: {{ color: 'transparent' }} }},
          }},
          maintainAspectRatio: false,
        }},
      }});
    }}

    // ─── News cadence ────────────────────────────────────────────────
    if (d.cadence && d.cadence.labels && d.cadence.labels.length) {{
      new Chart(document.getElementById('chart-cadence'), {{
        type: 'line',
        data: {{
          labels: d.cadence.labels,
          datasets: [
            {{ label: 'Crocs',   data: d.cadence.crocs,   borderColor: '{BRAND_ACCENT}',
               backgroundColor: 'rgba(6,165,80,0.15)', borderWidth: 2.5, tension: 0.35, pointRadius: 2, fill: true }},
            {{ label: 'HeyDude', data: d.cadence.heydude, borderColor: '#95a5a6',
               backgroundColor: 'rgba(149,165,166,0.10)', borderWidth: 2, tension: 0.35, pointRadius: 2, fill: true }},
          ]
        }},
        options: {{
          plugins: {{ legend: {{ labels: {{ color: '#6b6456', boxWidth: 12 }} }} }},
          scales: {{
            x: {{ ticks: {{ color: '#6b6456', maxRotation: 45, minRotation: 45 }}, grid: {{ color: '#ebe4d2' }} }},
            y: {{ ticks: {{ color: '#6b6456' }}, grid: {{ color: '#ebe4d2' }}, beginAtZero: true }},
          }},
          maintainAspectRatio: false,
        }},
      }});
    }}

    // ─── Drop Effects scatter: demand lift vs CROX 30d return ──────
    const scatterEl = document.getElementById('chart-drops-scatter');
    const scatterDataEl = document.getElementById('drops-scatter-data');
    if (scatterEl && scatterDataEl) {{
      const points = JSON.parse(scatterDataEl.textContent || '[]');
      if (points.length) {{
        const colors = points.map(p => {{
          const liftHot = p.x > 1.1;
          const stockUp = p.y > 0;
          if (liftHot && stockUp)   return '#27ae60';   // both moved — green
          if (liftHot && !stockUp)  return '#f39c12';   // demand moved, stock didn't — amber
          if (!liftHot && stockUp)  return '#58a6ff';   // stock moved without demand lift — blue
          return '#8b949e';                              // nothing moved — gray
        }});
        new Chart(scatterEl, {{
          type: 'scatter',
          data: {{
            datasets: [{{
              label: 'Drops',
              data: points,
              backgroundColor: colors,
              borderColor: '#0d1117', borderWidth: 1,
              pointRadius: 6, pointHoverRadius: 9,
            }}]
          }},
          options: {{
            plugins: {{
              legend: {{ display: false }},
              tooltip: {{
                callbacks: {{
                  title: (items) => {{
                    if (!items.length) return '';
                    const p = items[0].raw;
                    return p.date + ' — ' + p.label;
                  }},
                  label: (ctx) => {{
                    const p = ctx.raw;
                    return 'Lift ×' + p.x.toFixed(2) + ', CROX 30d ' + (p.y >= 0 ? '+' : '') + p.y.toFixed(1) + '%';
                  }}
                }}
              }}
            }},
            scales: {{
              x: {{ type: 'linear', position: 'bottom',
                    ticks: {{ color: '#6b6456', callback: v => '×' + v.toFixed(1) }},
                    grid: {{ color: '#ebe4d2' }},
                    title: {{ display: true, text: 'Composite demand lift (post ÷ pre, ±14d)', color: '#6b6456' }} }},
              y: {{ type: 'linear',
                    ticks: {{ color: '#6b6456', callback: v => (v >= 0 ? '+' : '') + v + '%' }},
                    grid: {{ color: '#ebe4d2' }},
                    title: {{ display: true, text: 'CROX 30d return after drop', color: '#6b6456' }} }},
            }},
            maintainAspectRatio: false,
          }},
        }});
      }}
    }}

    // ─── Combined CROX price + Reddit bars + drop event markers ────
    if (d.stock && d.stock.dates && d.stock.dates.length) {{
      const events = d.stock.events || [];

      // Reddit monthly counts → anchor ONE point at mid-month (day 15) of each month,
      // rendered as a smooth line on the secondary axis. spanGaps = true connects
      // the dots across the null-filled daily gaps so it looks clean next to the
      // daily stock price line.
      const rLabels = d.stock.reddit_labels || [];
      const rCounts = d.stock.reddit_counts || [];
      const redditByMonth = Object.fromEntries(rLabels.map((m, i) => [m, rCounts[i]]));
      const stockDates = d.stock.dates;
      // For each stock date, check if it's closest to the 15th of its month. Use
      // the specific trading day nearest to mid-month to anchor each Reddit value.
      const redditLine = stockDates.map((d) => {{
        const day = parseInt(d.slice(8, 10), 10);
        const m = d.slice(0, 7);
        // Mid-month anchor: pick the trading day closest to 15th for each month
        return (day >= 14 && day <= 16 && redditByMonth[m] !== undefined) ? redditByMonth[m] : null;
      }});

      // Build drop marker series — one {{x,y}} per event, colored by type/brand
      const dropPoints = events.map(e => ({{
        x: e.date, y: e.price,
        label: e.label, brand: e.brand, type: e.type,
        original_date: e.original_date || e.date,
      }}));
      const dropColors = events.map(e =>
        e.brand === 'HeyDude' ? '#95a5a6'
        : e.type === 'manual' ? '{BRAND_ACCENT2}'
        : '#e84393'
      );

      // Vertical drop guide lines — build as line segments from top of chart
      // down to the event price, so each drop is clearly visible.
      const guideDatasets = events.map((e, idx) => ({{
        type: 'line',
        label: '_guide_' + idx,
        data: stockDates.map(d => d === e.date ? e.price : null),
        borderColor: dropColors[idx],
        borderWidth: 1,
        borderDash: [4, 4],
        pointRadius: 0,
        spanGaps: false,
        fill: {{ target: 'origin', above: 'rgba(0,0,0,0)' }},
        yAxisID: 'y',
        order: 3,
      }}));

      new Chart(document.getElementById('chart-stock-combined'), {{
        data: {{
          labels: stockDates,
          datasets: [
            {{
              type: 'line', label: 'Reddit mentions / month',
              data: redditLine,
              borderColor: '#58a6ff',
              backgroundColor: 'rgba(88,166,255,0.10)',
              borderWidth: 2, tension: 0.35, pointRadius: 3, pointHoverRadius: 5,
              spanGaps: true, fill: true,
              yAxisID: 'y1', order: 4,
            }},
            {{
              type: 'line', label: 'CROX close', data: d.stock.closes,
              borderColor: '{BRAND_ACCENT}', backgroundColor: 'rgba(6,165,80,0.08)',
              borderWidth: 2, tension: 0.15, pointRadius: 0, fill: true,
              yAxisID: 'y', order: 2,
            }},
            ...guideDatasets,
            {{
              type: 'scatter', label: 'Drop events',
              data: dropPoints,
              backgroundColor: dropColors,
              borderColor: '#fff', borderWidth: 2,
              pointRadius: 8, pointHoverRadius: 12,
              yAxisID: 'y', order: 1,
            }},
          ]
        }},
        options: {{
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{
              filter: (item) => !(item.dataset.label || '').startsWith('_guide_'),
              callbacks: {{
                title: (items) => {{
                  if (!items.length) return '';
                  const it = items[0];
                  if (it.dataset.label === 'Drop events') {{
                    const e = it.raw;
                    return (e.brand || 'Crocs') + ' drop — ' + (e.original_date || e.x);
                  }}
                  return it.label;
                }},
                label: (ctx) => {{
                  if (ctx.dataset.label === 'Drop events') return ctx.raw.label;
                  if (ctx.dataset.label === 'CROX close') return 'CROX $' + ctx.parsed.y.toFixed(2);
                  if (ctx.dataset.label === 'Reddit mentions / month') return ctx.parsed.y + ' posts this month';
                  return null;
                }}
              }}
            }}
          }},
          scales: {{
            x: {{ type: 'category', ticks: {{ color: '#6b6456', maxTicksLimit: 14, autoSkip: true }},
                  grid: {{ color: '#ebe4d2' }} }},
            y: {{ type: 'linear', position: 'left',
                  ticks: {{ color: '{BRAND_ACCENT}', callback: v => '$' + v }},
                  grid: {{ color: '#ebe4d2' }},
                  title: {{ display: true, text: 'CROX close ($)', color: '{BRAND_ACCENT}' }} }},
            y1: {{ type: 'linear', position: 'right',
                   ticks: {{ color: '#58a6ff' }}, grid: {{ drawOnChartArea: false }},
                   title: {{ display: true, text: 'Reddit mentions / month', color: '#58a6ff' }},
                   beginAtZero: true }},
          }},
          maintainAspectRatio: false,
        }},
      }});
    }}
  }})();
</script>
</body>
</html>
"""


def main():
    print(f"── {BRAND_NAME} Demand Dashboard v4 ──────────────────────")
    d = load_all()
    html = build_html(d)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_HTML.stat().st_size // 1024
    print(f"  ✓ Dashboard → {OUTPUT_HTML} ({size_kb}KB)")

    print("\n  Data coverage:")
    print(f"    reddit (arctic):  {len(d['reddit_raw'])} posts")
    print(f"    google news:      {len(d['news_raw'])} articles, {len(d['news_publishers'])} publishers")
    print(f"    youtube:          {len(d['youtube_raw'])} videos")
    print(f"    stock (CROX):     {len(d['stock_daily'])} daily rows")
    print(f"    manual drops:     {len(d['manual_releases'])} rows")

    try:
        webbrowser.open(f"file://{OUTPUT_HTML}")
        print("  ✓ Opened in browser.")
    except Exception:
        pass


if __name__ == "__main__":
    main()
