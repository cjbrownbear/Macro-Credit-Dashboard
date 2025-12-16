#!/usr/bin/env python3
"""
Macro–Credit Dashboard updater

Outputs (repo root):
- data.json
- news.json
- macro_credit_timeseries.xlsx
- macro_credit_metrics.xlsx

Data sources:
- FRED CSV (fredgraph.csv) for time series
- RSS feeds for headlines

Designed to run in GitHub Actions weekly + manual dispatch.
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# Optional dependency (we install it in workflow)
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None


# -------------------------
# Config
# -------------------------

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

OUTPUT_DATA_JSON = "data.json"
OUTPUT_NEWS_JSON = "news.json"
OUTPUT_TIMESERIES_XLSX = "macro_credit_timeseries.xlsx"
OUTPUT_METRICS_XLSX = "macro_credit_metrics.xlsx"

USER_AGENT = "macro-credit-dashboard/1.0 (github actions)"

REQUEST_TIMEOUT = 30


@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    out_key: str
    frequency: str
    units_note: str


# KPI + chart series we persist into data.json.
# NOTE: KPI tiles are computed in index.html, but we keep a single canonical dataset.
SERIES: List[SeriesDef] = [
    SeriesDef("DRCCLACBS", "DRCCLACBS_pct", "quarterly", "Card Delinquency Rate, 30+ Days Past Due (%)."),
    SeriesDef("CORCCACBS", "CORCCACBS_pct", "quarterly", "Net Charge-off Rate on Credit Card Loans (%)."),
    SeriesDef("TDSP", "TDSP_pct", "quarterly", "Household Debt Service Payments as a Percent of Disposable Personal Income (%)."),
    SeriesDef("JTSJOL", "JTSJOL_mil", "monthly", "Job Openings: Total Nonfarm (millions)."),
    SeriesDef("REVOLSL", "REVOLSL_bil_usd", "monthly", "Revolving Consumer Credit Outstanding ($ billions)."),
]

# RSS feeds (reputable, stable). CNBC provides RSS; Yahoo Finance provides RSS topics; Reuters RSS can be inconsistent.
# If you want more sources later, add them here.
NEWS_FEEDS: List[Tuple[str, str]] = [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance - Economy", "https://finance.yahoo.com/news/rssindex"),
]

# Basic keyword routing into two columns (optional, used by the HTML layout you now have)
NEWS_BUCKETS = {
    "Credit / consumer stress": ["credit", "consumer", "delinquen", "charge-off", "loan", "debt", "card", "bank", "default"],
    "Labor / macro signals": ["jobs", "job", "labor", "unemploy", "layoff", "inflation", "fed", "rates", "growth", "recession"],
}


# -------------------------
# Helpers
# -------------------------

def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _http_get(url: str) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch FRED series as a DataFrame with columns:
      - date (YYYY-MM-DD)
      - value (float or NaN)
    Robust to unexpected header names.
    """
    url = FRED_CSV_URL.format(series_id=series_id)
    r = _http_get(url)

    # Some FRED responses can have BOM or weird leading spaces.
    text = r.text.strip()
    if not text:
        raise ValueError(f"Empty response from FRED for {series_id}")

    from io import StringIO
    df = pd.read_csv(StringIO(text))

    # Try to identify date + value columns robustly
    cols = [c.strip() for c in df.columns.tolist()]
    df.columns = cols

    # Expected: DATE and {series_id}
    date_col = None
    for candidate in ["DATE", "date", "Date"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        # fallback: first column
        date_col = df.columns[0]

    value_col = None
    if series_id in df.columns:
        value_col = series_id
    else:
        # fallback: second column if present
        if len(df.columns) >= 2:
            value_col = df.columns[1]
        else:
            raise ValueError(f"Unexpected FRED CSV format for {series_id}: columns={df.columns.tolist()}")

    out = df[[date_col, value_col]].copy()
    out.rename(columns={date_col: "date", value_col: "value"}, inplace=True)

    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date.astype(str)

    def _coerce(v):
        if isinstance(v, str):
            v = v.strip()
            if v in (".", "", "NaN", "nan"):
                return math.nan
        try:
            return float(v)
        except Exception:
            return math.nan

    out["value"] = out["value"].apply(_coerce)

    # Drop rows with invalid dates
    out = out[out["date"].notna() & (out["date"] != "NaT")]

    return out


def build_news() -> Dict:
    """
    Build a small news.json feed using RSS.
    Output format expected by index.html:
      { "meta": {...}, "items": [...], "buckets": {bucket: [items...] } }
    """
    meta = {"last_updated_utc": _now_utc_iso(), "sources": []}

    if feedparser is None:
        # Provide a valid file that tells the UI it isn't available.
        return {"meta": meta, "items": [], "buckets": {}}

    items: List[Dict] = []

    for source_name, url in NEWS_FEEDS:
        try:
            d = feedparser.parse(url)
            meta["sources"].append({"name": source_name, "url": url})
            for e in d.entries[:20]:
                title = (getattr(e, "title", "") or "").strip()
                link = (getattr(e, "link", "") or "").strip()
                published = (getattr(e, "published", "") or getattr(e, "updated", "") or "").strip()
                if not title or not link:
                    continue
                items.append(
                    {
                        "source": source_name,
                        "title": title,
                        "url": link,
                        "published": published,
                    }
                )
        except Exception:
            # ignore feed errors; we still write a file
            continue

    # De-dupe by URL
    seen = set()
    deduped = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        deduped.append(it)

    # Bucket items by keywords
    buckets: Dict[str, List[Dict]] = {k: [] for k in NEWS_BUCKETS.keys()}
    for it in deduped:
        t = it["title"].lower()
        placed = False
        for bucket, keys in NEWS_BUCKETS.items():
            if any(k in t for k in keys):
                buckets[bucket].append(it)
                placed = True
                break
        if not placed:
            # put unclassified into labor/macro by default
            buckets["Labor / macro signals"].append(it)

    # Trim to reasonable sizes
    for k in buckets:
        buckets[k] = buckets[k][:8]

    return {"meta": meta, "items": deduped[:30], "buckets": buckets}


def main() -> None:
    # Fetch and join series
    series_frames: List[pd.DataFrame] = []
    source_notes: Dict[str, str] = {}

    for s in SERIES:
        df = fetch_fred_series(s.series_id)
        df.rename(columns={"value": s.out_key}, inplace=True)
        series_frames.append(df)
        source_notes[s.out_key] = f"FRED {s.series_id} ({s.frequency}) — {s.units_note}"

    # Outer join on date (string YYYY-MM-DD)
    merged = None
    for df in series_frames:
        merged = df if merged is None else merged.merge(df, on="date", how="outer")

    assert merged is not None
    merged.sort_values("date", inplace=True)

    # Keep numeric columns as floats, ensure NaNs rather than None
    for s in SERIES:
        merged[s.out_key] = pd.to_numeric(merged[s.out_key], errors="coerce")

    # Build data.json payload
    payload = {
        "meta": {
            "last_updated_utc": _now_utc_iso(),
            "source_notes": source_notes,
        },
        "data": merged.to_dict(orient="records"),
    }

    with open(OUTPUT_DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    # Build news.json
    news_payload = build_news()
    with open(OUTPUT_NEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(news_payload, f, ensure_ascii=False)

    # Write Excel outputs
    # 1) timeseries workbook
    with pd.ExcelWriter(OUTPUT_TIMESERIES_XLSX, engine="openpyxl") as xw:
        merged.to_excel(xw, sheet_name="timeseries", index=False)
        # lightweight dictionary sheet
        pd.DataFrame(
            [{"field": k, "source": v} for k, v in source_notes.items()]
        ).to_excel(xw, sheet_name="dictionary", index=False)

    # 2) metrics workbook (keep "metrics" tab name because you referenced it earlier)
    # We'll provide a "metrics" sheet that's the latest snapshot + 10y avg/std + 1y delta.
    # (index.html calculates on the fly too; this is for your export.)
    metric_rows = []
    merged_dt = merged.copy()
    merged_dt["date_dt"] = pd.to_datetime(merged_dt["date"], errors="coerce")

    def last_valid(col: str) -> Optional[pd.Series]:
        s = merged_dt.dropna(subset=[col]).tail(1)
        if s.empty:
            return None
        return s.iloc[0]

    for s in SERIES:
        lv = last_valid(s.out_key)
        if lv is None:
            continue
        as_of = lv["date_dt"]
        val = float(lv[s.out_key])

        ten_years_ago = as_of - pd.DateOffset(years=10)
        one_year_ago = as_of - pd.DateOffset(years=1)

        w10 = merged_dt[(merged_dt["date_dt"] >= ten_years_ago) & (~merged_dt[s.out_key].isna())][s.out_key].astype(float).tolist()
        avg10 = float(pd.Series(w10).mean()) if w10 else val
        sd10 = float(pd.Series(w10).std(ddof=0)) if w10 else 0.0

        # nearest at-or-before 1y
        prior = merged_dt[(merged_dt["date_dt"] <= one_year_ago) & (~merged_dt[s.out_key].isna())].tail(1)
        if prior.empty:
            d1_abs = math.nan
            d1_pct = math.nan
        else:
            pv = float(prior.iloc[0][s.out_key])
            d1_abs = val - pv
            d1_pct = ((val / pv) - 1.0) * 100.0 if pv not in (0.0, -0.0) else math.nan

        metric_rows.append(
            {
                "field": s.out_key,
                "fred_series_id": s.series_id,
                "as_of": as_of.date().isoformat(),
                "latest_value": val,
                "avg_10y": avg10,
                "std_10y": sd10,
                "delta_1y_abs": d1_abs,
                "delta_1y_pct": d1_pct,
            }
        )

    metrics_df = pd.DataFrame(metric_rows)
    with pd.ExcelWriter(OUTPUT_METRICS_XLSX, engine="openpyxl") as xw:
        metrics_df.to_excel(xw, sheet_name="metrics", index=False)
        pd.DataFrame(
            [{"field": k, "source": v} for k, v in source_notes.items()]
        ).to_excel(xw, sheet_name="dictionary", index=False)

    print("Wrote:", OUTPUT_DATA_JSON, OUTPUT_NEWS_JSON, OUTPUT_TIMESERIES_XLSX, OUTPUT_METRICS_XLSX)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise

