"""
CROX stock price — daily close from Yahoo Finance (via yfinance).

Used to overlay stock price against demand signals (Reddit mentions, drop
events) so you can see whether past drops produced demand spikes that showed
up in the stock.

Output:
  data/crox_stock.csv  — daily close, volume, monthly rollup

No auth required. Update daily via the pipeline.

Run:
  python3 scripts/fetch_stock_price.py
"""
import sys
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

try:
    import yfinance as yf
except ImportError:
    print("ERROR: yfinance not installed. Run: ./venv/bin/pip install yfinance")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

TICKER     = "CROX"
LOOKBACK_Y = 3   # 3 years of daily data
DAILY_OUT  = DATA_DIR / "crox_stock.csv"
MONTHLY_OUT = DATA_DIR / "crox_stock_monthly.csv"


def main():
    print(f"── Stock price fetch — {TICKER} (via yfinance) ─────────────")
    start = (datetime.now() - timedelta(days=LOOKBACK_Y * 365 + 30)).strftime("%Y-%m-%d")

    t = yf.Ticker(TICKER)
    df = t.history(start=start, interval="1d", auto_adjust=False)
    if df.empty:
        print(f"  No price data returned for {TICKER}.")
        return

    df = df.reset_index()
    # Normalize: strip tz (if present) to avoid TZ arithmetic issues in the dashboard
    if hasattr(df["Date"].dtype, "tz") and df["Date"].dtype.tz is not None:
        df["Date"] = df["Date"].dt.tz_localize(None)
    df["date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df["month"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m")
    out = df[["date", "month", "Open", "High", "Low", "Close", "Volume"]].rename(
        columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
    )
    out.to_csv(DAILY_OUT, index=False)
    print(f"  → {DAILY_OUT} ({len(out)} daily rows, {out['date'].min()} → {out['date'].max()})")

    # Monthly avg close + volume
    monthly = (out.assign(close=pd.to_numeric(out["close"], errors="coerce"),
                          volume=pd.to_numeric(out["volume"], errors="coerce"))
                  .groupby("month")
                  .agg(avg_close=("close", "mean"),
                       end_close=("close", "last"),
                       total_volume=("volume", "sum"))
                  .reset_index())
    monthly["avg_close"] = monthly["avg_close"].round(2)
    monthly["end_close"] = monthly["end_close"].round(2)
    monthly.to_csv(MONTHLY_OUT, index=False)
    print(f"  → {MONTHLY_OUT} ({len(monthly)} monthly rows)")

    latest = out.iloc[-1]
    first  = out.iloc[0]
    pct    = (latest["close"] - first["close"]) / first["close"] * 100
    print(f"  Latest close {latest['date']}: ${latest['close']:.2f} "
          f"({pct:+.1f}% over {LOOKBACK_Y}yr window)")


if __name__ == "__main__":
    main()
