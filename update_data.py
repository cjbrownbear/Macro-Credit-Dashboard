#!/usr/bin/env python3
"""
Macro–Credit Dashboard updater (GitHub Pages friendly)

Outputs (repo root):
- data.json          (NO NaN tokens; uses null)
- news.json
- macro_credit_timeseries.xlsx
- macro_credit_metrics.xlsx

Sources:
- FRED CSV endpoint (fredgraph.csv)
- RSS feeds for headlines

Designed to run in GitHub Actions weekly + manual dispatch.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None


import math

def clean_for_json(obj):
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj


FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

OUTPUT_DATA_JSON = "data.json"
OUTPUT_NEWS_JSON = "news.json"
OUTPUT_TIMESERIES_XLSX = "macro_credit_timeseries.xlsx"
OUTPUT_METRICS_XLSX = "macro_credit_metrics.xlsx"

USER_AGENT = "macro-credit-dashboard/1.1 (github actions)"
REQUEST_TIMEOUT = 30


@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    out_key: str
    frequency: str
    units_note: str


SERIES: List[SeriesDef] = [
    SeriesDef("DRCCLACBS", "DRCCLACBS_pct", "quarterly", "Card Delinquency Rate, 30+ Days Past Due (%)."),
    SeriesDef("CORCCACBS", "CORCCACBS_pct", "quarterly", "Net Charge-off Rate on Credit Card Loans (%)."),
    SeriesDef("TDSP", "TDSP_pct", "quarterly", "Household Debt Service Payments as % of Disposable Personal Income (%)."),
    SeriesDef("JTSJOL", "JTSJOL_mil", "monthly", "Job Openings: Total Nonfarm (millions)."),
    SeriesDef("REVOLSL", "REVOLSL_bil_usd", "monthly", "Revolving Consumer Credit Outstanding ($ billions)."),
    SeriesDef("DRSFRMACBS","DRSFRMACBS_pct","quarterly","Delinquency Rate on Single-Family Residential Mortgages, All Commercial Banks (%)."),

]


NEWS_FEEDS: List[Tuple[str, str]] = [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance - Economy", "https://finance.yahoo.com/news/rssindex"),
]

NEWS_BUCKETS = {
    "Credit / consumer stress": ["credit", "consumer", "delinquen", "charge-off", "loan", "debt", "card", "bank", "default"],
    "Labor / macro signals": ["jobs", "job", "labor", "unemploy", "layoff", "inflation", "fed", "rates", "growth", "recession"],
}


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _http_get(url: str) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Returns DataFrame columns:
      - date (YYYY-MM-DD)
      - value (float; NaN for missing)
    """
    url = FRED_CSV_URL.format(series_id=series_id)
    r = _http_get(url)
    text = r.text.strip()
    if not text:
        raise ValueError(f"Empty response from FRED for {series_id}")

    from io import StringIO
    df = pd.read_csv(StringIO(text))

    df.columns = [c.strip() for c in df.columns.tolist()]

    # Detect date column
    date_col = None
    for candidate in ["DATE", "date", "Date"]:
        if candidate in df.columns:
            date_col = candidate
            break
    if date_col is None:
        date_col = df.columns[0]

    # Detect value column
    if series_id in df.columns:
        value_col = series_id
    elif len(df.columns) >= 2:
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
    out = out[out["date"].notna() & (out["date"] != "NaT")]
    return out


def build_news() -> Dict:
    meta = {"last_updated_utc": _now_utc_iso(), "sources": []}

    if feedparser is None:
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
                if title and link:
                    items.append({"source": source_name, "title": title, "url": link, "published": published})
        except Exception:
            continue

    # De-dupe by URL
    seen = set()
    deduped = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        deduped.append(it)

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
            buckets["Labor / macro signals"].append(it)

    for k in buckets:
        buckets[k] = buckets[k][:8]

    return {"meta": meta, "items": deduped[:30], "buckets": buckets}


def _df_to_json_records_no_nan(df: pd.DataFrame) -> List[Dict]:
    """
    Convert a DataFrame to JSON-safe records:
    - NaN/NaT -> None (null)
    - ensures json.dump produces valid JSON
    """
    clean = df.copy()
    # Replace NaN/NaT with None
    clean = clean.where(pd.notnull(clean), None)
    return clean.to_dict(orient="records")


def main() -> None:
    frames: List[pd.DataFrame] = []
    source_notes: Dict[str, str] = {}

    for s in SERIES:
        df = fetch_fred_series(s.series_id)
        df.rename(columns={"value": s.out_key}, inplace=True)
        frames.append(df)
        source_notes[s.out_key] = f"FRED {s.series_id} ({s.frequency}) — {s.units_note}"

    merged = None
    for df in frames:
        merged = df if merged is None else merged.merge(df, on="date", how="outer")

    assert merged is not None
    merged.sort_values("date", inplace=True)

    for s in SERIES:
        merged[s.out_key] = pd.to_numeric(merged[s.out_key], errors="coerce")

    payload = {
        "meta": {"last_updated_utc": _now_utc_iso(), "source_notes": source_notes},
        "data": _df_to_json_records_no_nan(merged),
    }

    # IMPORTANT: allow_nan=False forces failure if NaN sneaks in
    with open(OUTPUT_DATA_JSON, "w", encoding="utf-8") as f:
        payload = clean_for_json(payload)
        json.dump(payload, f, ensure_ascii=False)

    news_payload = build_news()
    with open(OUTPUT_NEWS_JSON, "w", encoding="utf-8") as f:
        news_payload = clean_for_json(news_payload)
        json.dump(news_payload, f, ensure_ascii=False)

    with pd.ExcelWriter(OUTPUT_TIMESERIES_XLSX, engine="openpyxl") as xw:
        merged.to_excel(xw, sheet_name="timeseries", index=False)
        pd.DataFrame([{"field": k, "source": v} for k, v in source_notes.items()]).to_excel(
            xw, sheet_name="dictionary", index=False
        )

    # Metrics export
    merged_dt = merged.copy()
    merged_dt["date_dt"] = pd.to_datetime(merged_dt["date"], errors="coerce")

    def last_valid(col: str) -> Optional[pd.Series]:
        s = merged_dt.dropna(subset=[col]).tail(1)
        return None if s.empty else s.iloc[0]

    metric_rows = []
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

        prior = merged_dt[(merged_dt["date_dt"] <= one_year_ago) & (~merged_dt[s.out_key].isna())].tail(1)
        if prior.empty:
            d1_abs = None
            d1_pct = None
        else:
            pv = float(prior.iloc[0][s.out_key])
            d1_abs = val - pv
            d1_pct = ((val / pv) - 1.0) * 100.0 if pv != 0.0 else None

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
        pd.DataFrame([{"field": k, "source": v} for k, v in source_notes.items()]).to_excel(
            xw, sheet_name="dictionary", index=False
        )

    print("Wrote:", OUTPUT_DATA_JSON, OUTPUT_NEWS_JSON, OUTPUT_TIMESERIES_XLSX, OUTPUT_METRICS_XLSX)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("ERROR:", e, file=sys.stderr)
        raise
