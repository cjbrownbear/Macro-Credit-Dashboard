#!/usr/bin/env python3
"""
update_data.py

Generates dashboard artifacts in the REPO ROOT:
  - data.json  (dashboard reads this)
  - news.json  (RSS-based headlines; dashboard reads this)
  - macro_credit_metrics.xlsx (metrics tab)
  - macro_credit_timeseries.xlsx (metrics + timeseries)

Notes:
- No git commands belong in this file. Committing is handled by the GitHub Action workflow.
- Uses FRED "fredgraph.csv" endpoints (no API key required).
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests
import xml.etree.ElementTree as ET
from io import StringIO


# -----------------------------
# Configuration
# -----------------------------

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

SERIES_CONFIG = {
    # quarterly %, already percent units
    "DRCCLACBS_pct": {
        "fred_id": "DRCCLACBS",
        "transform": lambda s: s,
        "notes": "FRED DRCCLACBS (quarterly, %)"
    },
    "CORCCACBS_pct": {
        "fred_id": "CORCCACBS",
        "transform": lambda s: s,
        "notes": "FRED CORCCACBS (quarterly, %)"
    },
    # JTSJOL on FRED is in thousands of persons -> convert to millions
    "JTSJOL_mil": {
        "fred_id": "JTSJOL",
        "transform": lambda s: s / 1000.0,
        "notes": "FRED JTSJOL (monthly, thousands -> millions)"
    },
    # REVOLSL on FRED is in $ billions -> keep as $B
    "REVOLSL_bil_usd": {
        "fred_id": "REVOLSL",
        "transform": lambda s: s,
        "notes": "FRED REVOLSL (monthly, $ billions)"
    },
}

# direction: +1 means "higher is worse", -1 means "lower is worse"
METRIC_DEFS = [
    {"key": "DRCCLACBS_pct", "title": "Card 30+ Delinquency", "unit": "pct", "direction": +1},
    {"key": "CORCCACBS_pct", "title": "Net Charge-off Rate", "unit": "pct", "direction": +1},
    {"key": "JTSJOL_mil", "title": "Job Openings", "unit": "mil", "direction": -1},
    {"key": "REVOLSL_bil_usd", "title": "Revolving Consumer Credit", "unit": "usd_b", "direction": +1},
]

# RSS feeds (RSS avoids CORS; the Action writes news.json; index.html fetches it)
RSS_FEEDS: List[Tuple[str, str]] = [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rss"),
]


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a single FRED series via fredgraph.csv (no API key).
    Robust to header name differences.
    Returns df with columns: date (datetime64), value (float)
    """
    url = FRED_CSV_URL.format(series_id=series_id)
    r = requests.get(url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()

    df = pd.read_csv(StringIO(r.text))

    # ---- Robust date column detection ----
    date_col = None
    for c in df.columns:
        c_low = c.lower()
        if c_low in ("date", "observation_date", "time"):
            date_col = c
            break

    if date_col is None:
        raise ValueError(
            f"Unexpected FRED CSV format for {series_id}: no recognizable date column. "
            f"Columns={list(df.columns)}"
        )

    # ---- Value column = first non-date column ----
    value_cols = [c for c in df.columns if c != date_col]
    if not value_cols:
        raise ValueError(
            f"Unexpected FRED CSV format for {series_id}: missing value column. "
            f"Columns={list(df.columns)}"
        )

    val_col = value_cols[0]

    df = df.rename(columns={date_col: "date", val_col: "value"})
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    return df


def to_month_start(d: pd.Timestamp) -> pd.Timestamp:
    return pd.Timestamp(year=d.year, month=d.month, day=1)


def build_monthly_index(start: pd.Timestamp, end: pd.Timestamp) -> pd.DatetimeIndex:
    return pd.date_range(to_month_start(start), to_month_start(end), freq="MS")


def mean(arr: List[float]) -> float:
    return sum(arr) / len(arr)


def std_pop(arr: List[float]) -> float:
    """Population standard deviation (ddof=0)"""
    if len(arr) < 2:
        return 0.0
    m = mean(arr)
    v = mean([(x - m) ** 2 for x in arr])
    return math.sqrt(v)


def zscore_classify(current: float, avg: float, sd: float, direction: int) -> str:
    """
    Tripwire if risk >= 1.0, Stress if risk >= 2.0
    risk = direction * z
      - direction +1: higher is worse
      - direction -1: lower is worse
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
    return series[(series.index >= start) & (series.index <= end_date)].dropna()


# -----------------------------
# News builder (RSS -> news.json)
# -----------------------------

def fetch_rss_items(url: str, limit: int = 6) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    r.raise_for_status()
    root = ET.fromstring(r.text)

    items: List[Dict[str, str]] = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if title and link:
            items.append({"title": title, "link": link, "date": pub})
    return items


def build_news_json(out_path: str = "news.json") -> None:
    payload = {"meta": {"generated_utc": utc_now_iso()}, "items": []}

    for source, url in RSS_FEEDS:
        try:
            for it in fetch_rss_items(url, limit=6):
                it["source"] = source
                payload["items"].append(it)
        except Exception:
            # Don't fail the whole pipeline if a feed is down
            payload["items"].append({
                "title": f"(Feed unavailable: {source})",
                "link": url,
                "date": "",
                "source": source
            })

    payload["items"] = payload["items"][:12]
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    # 1) Fetch series and track global date range
    raw_series: Dict[str, pd.Series] = {}
    source_notes: Dict[str, str] = {}

    min_date: Optional[pd.Timestamp] = None
    max_date: Optional[pd.Timestamp] = None

    for dash_key, cfg in SERIES_CONFIG.items():
        fred_id = cfg["fred_id"]
        df = fetch_fred_series(fred_id)

        df["value"] = cfg["transform"](df["value"].astype(float))
        df = df.dropna(subset=["value"]).copy()

        if df.empty:
            raw_series[dash_key] = pd.Series(dtype=float)
        else:
            s = pd.Series(df["value"].values, index=pd.to_datetime(df["date"]))
            s = s.sort_index()
            raw_series[dash_key] = s

            d0 = s.index.min()
            d1 = s.index.max()
            min_date = d0 if min_date is None else min(min_date, d0)
            max_date = d1 if max_date is None else max(max_date, d1)

        source_notes[dash_key] = cfg.get("notes", f"FRED {fred_id}")

    if min_date is None or max_date is None:
        raise RuntimeError("No data fetched from FRED (all series empty).")

    # 2) Build monthly master frame
    master_idx = build_monthly_index(min_date, max_date)
    master = pd.DataFrame(index=master_idx)
    master.index.name = "date"

    # 3) Map series to month-start and forward-fill into monthly frame
    for dash_key, s in raw_series.items():
        if s.empty:
            master[dash_key] = pd.NA
            continue

        s2 = s.copy()
        s2.index = s2.index.map(to_month_start)
        s2 = s2[~s2.index.duplicated(keep="last")].sort_index()

        m = s2.reindex(master_idx).astype(float).ffill()
        master[dash_key] = m

    # 4) Compute metric rows for summary + export
    metrics_rows: List[Dict[str, object]] = []

    for md in METRIC_DEFS:
        key = md["key"]
        series = master[key].astype(float)

        latest_dt, latest_val = last_non_null(series)
        if latest_dt is None or latest_val is None:
            continue

        teny = compute_10y_window(series, latest_dt)
        vals = teny.dropna().tolist()
        avg10 = mean(vals) if vals else float(latest_val)
        sd10 = std_pop(vals) if vals else 0.0

        status = zscore_classify(float(latest_val), float(avg10), float(sd10), int(md["direction"]))

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
            "title": md["title"],
            "unit": md["unit"],
            "direction": int(md["direction"]),
            "latest_date": latest_dt.strftime("%Y-%m-%d"),
            "latest_value": float(latest_val),
            "avg_10y": float(avg10),
            "sd_10y": float(sd10),
            "status": status,
            # For pct series: delta_1y_abs is in percentage points (pp)
            "delta_1y_abs": delta_abs if delta_abs is not None else pd.NA,
            "delta_1y_pct": delta_pct if delta_pct is not None else pd.NA,
            "source_note": source_notes.get(key, ""),
        })

    metrics_df = pd.DataFrame(metrics_rows)

    # 5) Write data.json
    out_data: List[Dict[str, object]] = []
    master_reset = master.reset_index()
    for _, row in master_reset.iterrows():
        d: Dict[str, object] = {"date": row["date"].strftime("%Y-%m-%d")}
        for k in SERIES_CONFIG.keys():
            v = row.get(k, pd.NA)
            d[k] = None if pd.isna(v) else float(v)
        out_data.append(d)

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "source_notes": source_notes,
            "delta_notes": {
                "pct_series_delta_abs_unit": "percentage points (pp)",
                "delta_1y_pct_unit": "percent difference (%)"
            }
        },
        "data": out_data,
        "metrics": metrics_rows
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 6) Write Excel outputs
    with pd.ExcelWriter("macro_credit_metrics.xlsx", engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)

    timeseries_df = master.reset_index()
    timeseries_df["date"] = timeseries_df["date"].dt.strftime("%Y-%m-%d")

    with pd.ExcelWriter("macro_credit_timeseries.xlsx", engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)
        timeseries_df.to_excel(writer, sheet_name="timeseries", index=False)

    # 7) Write news.json
    build_news_json("news.json")

    print("✅ Wrote: data.json, news.json, macro_credit_metrics.xlsx, macro_credit_timeseries.xlsx")


if __name__ == "__main__":
    main()
