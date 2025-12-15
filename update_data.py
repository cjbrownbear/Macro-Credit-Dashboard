#!/usr/bin/env python3
"""
update_data.py

Creates:
  - data.json  (dashboard reads this)
  - macro_credit_metrics.xlsx
  - macro_credit_timeseries.xlsx
  - news.json  (RSS-based headlines, safe for GitHub Pages)

Data sources:
  - FRED "fredgraph.csv" endpoints (no API key required)
  - RSS feeds for headlines (CNBC + Yahoo Finance by default)

Series included (matches dashboard keys):
  - DRCCLACBS_pct   : Card 30+ delinquency rate (all banks), quarterly (%)
  - CORCCACBS_pct   : Net charge-off rate (all banks), quarterly (%)
  - JTSJOL_mil      : Job openings (total nonfarm), monthly (millions)
  - REVOLSL_bil_usd : Revolving consumer credit, monthly ($ billions)
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import xml.etree.ElementTree as ET


# -----------------------------
# Configuration
# -----------------------------

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

# Dashboard keys -> FRED series IDs + transformations
SERIES_CONFIG = {
    # quarterly %, already percent units
    "DRCCLACBS_pct": {
        "fred_id": "DRCCLACBS",
        "freq": "Q",
        "transform": lambda s: s,  # keep %
        "notes": "FRED DRCCLACBS (quarterly, %)"
    },
    "CORCCACBS_pct": {
        "fred_id": "CORCCACBS",
        "freq": "Q",
        "transform": lambda s: s,  # keep %
        "notes": "FRED CORCCACBS (quarterly, %)"
    },
    # JTSJOL is typically "thousands of persons" on FRED -> convert to millions
    "JTSJOL_mil": {
        "fred_id": "JTSJOL",
        "freq": "M",
        "transform": lambda s: s / 1000.0,
        "notes": "FRED JTSJOL (monthly, thousands -> millions)"
    },
    # REVOLSL is in billions of dollars (seasonally adjusted) on FRED -> keep as $B
    "REVOLSL_bil_usd": {
        "fred_id": "REVOLSL",
        "freq": "M",
        "transform": lambda s: s,
        "notes": "FRED REVOLSL (monthly, $ billions)"
    },
}

# Stress classification:
# direction = +1 means "higher is worse"; direction = -1 means "lower is worse"
METRIC_DEFS = [
    {"key": "DRCCLACBS_pct", "title": "Card 30+ Delinquency", "unit": "pct", "direction": +1},
    {"key": "CORCCACBS_pct", "title": "Net Charge-off Rate", "unit": "pct", "direction": +1},
    {"key": "JTSJOL_mil", "title": "Job Openings", "unit": "mil", "direction": -1},
    {"key": "REVOLSL_bil_usd", "title": "Revolving Consumer Credit", "unit": "usd_b", "direction": +1},
]

# News RSS feeds (RSS avoids CORS; Action writes news.json; dashboard fetches it)
RSS_FEEDS: List[Tuple[str, str]] = [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rss"),
]


# -----------------------------
# Utilities
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.strip()
            if x == "" or x == ".":
                return None
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a single FRED series via fredgraph.csv (no API key).
    Returns df with columns: date (datetime64[ns]), value (float)
    """
    url = FRED_CSV_URL.format(series_id=series_id)
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    # fredgraph.csv comes as:
    # DATE, SERIESID
    # 1947-01-01, 123.4
    df = pd.read_csv(pd.compat.StringIO(r.text)) if hasattr(pd.compat, "StringIO") else pd.read_csv(pd.io.common.StringIO(r.text))
    # robust rename:
    if "DATE" not in df.columns:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing DATE")
    value_col = [c for c in df.columns if c != "DATE"]
    if not value_col:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing value column")
    value_col = value_col[0]

    df = df.rename(columns={"DATE": "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def to_month_start(d: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=d.year, month=d.month, day=1)


def build_monthly_master_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.date_range(to_month_start(start), to_month_start(end), freq="MS")


def zscore_classify(current: float, avg: float, sd: float, direction: int) -> str:
    """
    Uses 10-year mean/sd and directionality.
    risk = direction * z
      - direction +1: higher is worse
      - direction -1: lower is worse
    Tripwire if risk >= 1.0, Stress if risk >= 2.0
    """
    if sd is None or sd == 0 or math.isnan(sd):
        return "healthy"
    z = (current - avg) / sd
    risk = direction * z
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"


def last_non_null(series: pd.Series) -> Tuple[Optional[pd.Timestamp], Optional[float]]:
    s = series.dropna()
    if s.empty:
        return None, None
    idx = s.index[-1]
    return idx, float(s.iloc[-1])


def nearest_at_or_before(series: pd.Series, target_date: pd.Timestamp) -> Tuple[Optional[pd.Timestamp], Optional[float]]:
    s = series.dropna()
    if s.empty:
        return None, None
    s = s[s.index <= target_date]
    if s.empty:
        return None, None
    idx = s.index[-1]
    return idx, float(s.iloc[-1])


def compute_10y_window(series: pd.Series, end_date: pd.Timestamp) -> pd.Series:
    start = end_date - pd.DateOffset(years=10)
    w = series[(series.index >= start) & (series.index <= end_date)].dropna()
    return w


# -----------------------------
# News (RSS -> news.json)
# -----------------------------

def _fetch_rss_items(url: str, limit: int = 6) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    root = ET.fromstring(r.text)

    items = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if title and link:
            items.append({"title": title, "link": link, "date": pub})
    return items


def build_news_json(out_path: str = "news.json") -> None:
    payload = {
        "meta": {"generated_utc": utc_now_iso()},
        "items": []
    }

    for source, url in RSS_FEEDS:
        try:
            for it in _fetch_rss_items(url, limit=6):
                it["source"] = source
                payload["items"].append(it)
        except Exception:
            payload["items"].append({
                "title": f"(Failed to load feed: {source})",
                "link": url,
                "date": "",
                "source": source
            })

    payload["items"] = payload["items"][:12]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# -----------------------------
# Main build logic
# -----------------------------

def main() -> None:
    # 1) Fetch all series
    raw_series: Dict[str, pd.Series] = {}
    source_notes: Dict[str, str] = {}

    min_date = None
    max_date = None

    for dash_key, cfg in SERIES_CONFIG.items():
        fred_id = cfg["fred_id"]
        df = fetch_fred_series(fred_id)
        # apply transform
        df["value"] = cfg["transform"](df["value"].astype(float))

        # normalize index (we will reindex to monthly master below)
        df = df.dropna(subset=["value"]).copy()
        if df.empty:
            raw_series[dash_key] = pd.Series(dtype=float)
        else:
            s = pd.Series(df["value"].values, index=pd.to_datetime(df["date"]))
            raw_series[dash_key] = s.sort_index()

            d0 = s.index.min()
            d1 = s.index.max()
            min_date = d0 if min_date is None else min(min_date, d0)
            max_date = d1 if max_date is None else max(max_date, d1)

        source_notes[dash_key] = cfg.get("notes", f"FRED {fred_id}")

    if min_date is None or max_date is None:
        raise RuntimeError("No data fetched from FRED (all series empty).")

    # 2) Create monthly master time index
    master_idx = build_monthly_master_index(pd.Timestamp(min_date), pd.Timestamp(max_date))
    master = pd.DataFrame(index=master_idx)
    master.index.name = "date"

    # 3) Merge series into monthly frame
    #    Monthly series: align by month-start
    #    Quarterly series: forward-fill within month buckets (quarter value applies until next quarter)
    for dash_key, cfg in SERIES_CONFIG.items():
        s = raw_series.get(dash_key, pd.Series(dtype=float)).copy()
        if s.empty:
            master[dash_key] = pd.NA
            continue

        # Map original observation dates to month-start
        s.index = s.index.map(to_month_start)

        # If multiple observations map to same month-start, keep last
        s = s[~s.index.duplicated(keep="last")].sort_index()

        # Reindex to monthly master and forward-fill (especially for quarterly)
        m = s.reindex(master_idx).astype(float)
        m = m.ffill()
        master[dash_key] = m

    # 4) Compute metrics (latest, 10y avg/sd, status, 1y delta)
    metrics_rows = []
    for md in METRIC_DEFS:
        key = md["key"]
        title = md["title"]
        unit = md["unit"]
        direction = int(md["direction"])

        series = master[key].astype(float)

        latest_dt, latest_val = last_non_null(series)
        if latest_dt is None or latest_val is None:
            continue

        teny = compute_10y_window(series, latest_dt)
        avg10 = float(teny.mean()) if not teny.empty else float(latest_val)
        sd10 = float(teny.std(ddof=0)) if not teny.empty else 0.0

        status = zscore_classify(float(latest_val), avg10, sd10, direction)

        # 1-year delta: compare to value at or before 1y ago (monthly index)
        one_year_ago = latest_dt - pd.DateOffset(years=1)
        prior_dt, prior_val = nearest_at_or_before(series, one_year_ago)

        delta_abs = None
        delta_pct = None
        if prior_val is not None:
            delta_abs = float(latest_val - prior_val)
            if prior_val != 0:
                delta_pct = float((latest_val - prior_val) / abs(prior_val) * 100.0)

        metrics_rows.append({
            "metric_key": key,
            "title": title,
            "unit": unit,
            "direction": direction,
            "latest_date": latest_dt.strftime("%Y-%m-%d"),
            "latest_value": float(latest_val),
            "avg_10y": avg10,
            "sd_10y": sd10,
            "status": status,
            # delta fields:
            # For pct series: delta_abs is in percentage points (pp). We'll store numeric as-is.
            "delta_1y_abs": delta_abs if delta_abs is not None else pd.NA,
            "delta_1y_pct": delta_pct if delta_pct is not None else pd.NA,
            "source_note": source_notes.get(key, ""),
        })

    metrics_df = pd.DataFrame(metrics_rows)

    # 5) Write data.json for dashboard
    #    Dashboard expects: { meta: {...}, data: [{date: 'YYYY-MM-DD', KEY: value|null, ...}, ...] }
    out_data = []
    for dt, row in master.reset_index().iterrows():
        d = {"date": row["date"].strftime("%Y-%m-%d")}
        for k in SERIES_CONFIG.keys():
            v = row.get(k, pd.NA)
            if pd.isna(v):
                d[k] = None
            else:
                d[k] = float(v)
        out_data.append(d)

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "source_notes": source_notes,
            "delta_notes": {
                "pct_series_delta_abs_unit": "percentage points (pp)",
                "delta_1y_pct_unit": "percent difference (%)",
            }
        },
        "data": out_data,
        "metrics": metrics_rows,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 6) Write Excel files
    # metrics workbook
    with pd.ExcelWriter("macro_credit_metrics.xlsx", engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)

    # metrics + timeseries workbook
    timeseries_df = master.reset_index()
    timeseries_df["date"] = timeseries_df["date"].dt.strftime("%Y-%m-%d")

    with pd.ExcelWriter("macro_credit_timeseries.xlsx", engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)
        timeseries_df.to_excel(writer, sheet_name="timeseries", index=False)

    # 7) Build news.json (RSS)
    build_news_json("news.json")

    print("✅ Updated: data.json, macro_credit_metrics.xlsx, macro_credit_timeseries.xlsx, news.json")


if __name__ == "__main__":
    main()
