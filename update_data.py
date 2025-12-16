#!/usr/bin/env python3
"""
Macro-Credit Dashboard updater

Outputs (repo root):
- data.json
- news.json
- macro_credit_metrics.xlsx
- macro_credit_timeseries.xlsx

Design goals:
- Never fail the entire run because one series fails (skip + warn).
- Robust to FRED CSV format quirks (missing headers, extra lines).
- JSON-safe output (no NaN/Infinity).
- Minimal dependencies (requests, pandas, openpyxl, feedparser).
"""

from __future__ import annotations

import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

# RSS parsing for the news feed
import feedparser


# -----------------------------
# Config
# -----------------------------

OUT_DATA_JSON = "data.json"
OUT_NEWS_JSON = "news.json"
OUT_METRICS_XLSX = "macro_credit_metrics.xlsx"
OUT_TIMESERIES_XLSX = "macro_credit_timeseries.xlsx"

USER_AGENT = "MacroCreditDashboard/1.0 (+https://github.com/cjbrownbear/Macro-Credit-Dashboard)"
HTTP_TIMEOUT = 30

# FRED endpoints
FRED_GRAPH_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv"
FRED_API_BASE = "https://api.stlouisfed.org/fred/series/observations"

# If you set this secret in GitHub Actions (recommended), we’ll use the JSON API:
# Settings → Secrets and variables → Actions → New repository secret:
# Name: FRED_API_KEY, Value: <your key>
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

# Core series list: (FRED_SERIES_ID, output_key, unit_mode)
# unit_mode used for formatting/metadata only; values remain numeric.
SERIES = [
    ("DRCCLACBS", "DRCCLACBS_pct", "pct"),         # Card 30+ delinquency rate (%), quarterly
    ("CORCCACBS", "CORCCACBS_pct", "pct"),         # Card net charge-off rate (%), quarterly
    ("REVOLSL", "REVOLSL_bil_usd", "usd_b"),       # Revolving consumer credit ($B), monthly
    ("TDSP", "TDSP_pct", "pct"),                   # Household debt service burden (%), quarterly
    ("JTSJOL", "JTSJOL_mil", "mil"),               # Job openings (millions), monthly
    ("UNRATE", "UNRATE_pct", "pct"),               # Unemployment rate (%), monthly
    ("ICSA", "ICSA_k", "k"),                       # Initial jobless claims (thousands), weekly
    ("DRSFRMACBS", "DRSFRMACBS_pct", "pct"),       # 90+ day mortgage delinquency rate (%), quarterly
]

SOURCE_NOTES = {
    "DRCCLACBS_pct": "FRED DRCCLACBS (quarterly, %)",
    "CORCCACBS_pct": "FRED CORCCACBS (quarterly, %)",
    "REVOLSL_bil_usd": "FRED REVOLSL (monthly, $ billions)",
    "TDSP_pct": "FRED TDSP (quarterly, %)",
    "JTSJOL_mil": "FRED JTSJOL (monthly, millions)",
    "UNRATE_pct": "FRED UNRATE (monthly, %)",
    "ICSA_k": "FRED ICSA (weekly, level; stored here as thousands)",
    "DRSFRMACBS_pct": "FRED DRSFRMACBS (quarterly, %)",
}

# News RSS feeds (reputable + reliable RSS availability)
NEWS_FEEDS = [
    # CNBC
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    # Yahoo Finance
    ("Yahoo Finance Top", "https://finance.yahoo.com/news/rssindex"),
    # WSJ/Forbes often restrict RSS or require auth; keep it simple + stable.
]

NEWS_MAX_ITEMS_PER_FEED = 8


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def log(msg: str) -> None:
    print(msg, flush=True)

def warn(msg: str) -> None:
    print(f"[WARN] {msg}", flush=True)

def is_finite_number(x) -> bool:
    try:
        return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False

def to_float_or_none(x) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.strip()
            if x == "" or x.upper() in {"NA", "N/A", "NULL", "."}:
                return None
        v = float(x)
        if not math.isfinite(v):
            return None
        return v
    except Exception:
        return None

def http_get(url: str, params: Optional[dict] = None, retries: int = 3) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=HTTP_TIMEOUT)
            # Retry transient 5xx
            if r.status_code >= 500:
                warn(f"HTTP {r.status_code} from {url}; retry {attempt}/{retries}")
                time.sleep(1.25 * attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            warn(f"HTTP error on {url}: {e} (retry {attempt}/{retries})")
            time.sleep(1.25 * attempt)
    raise RuntimeError(f"Failed HTTP GET after retries: {url} ({last_err})")

def parse_fred_csv_text(text: str, series_id: str) -> pd.DataFrame:
    """
    Robustly parse the FRED fredgraph.csv output, which may include:
    - comments/blank lines
    - unexpected headers
    - missing DATE header in error cases
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Try to find a header line that contains DATE + something
    header_idx = None
    for i, ln in enumerate(lines[:20]):  # headers should be early
        if re.search(r"\bDATE\b", ln, flags=re.IGNORECASE) and "," in ln:
            header_idx = i
            break

    if header_idx is None:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing DATE")

    csv_text = "\n".join(lines[header_idx:])

    # Let pandas parse; accept additional columns
    df = pd.read_csv(pd.io.common.StringIO(csv_text))
    # Normalize column names
    cols = [c.strip() for c in df.columns]
    df.columns = cols

    # Identify date column
    date_col = None
    for c in df.columns:
        if c.upper() == "DATE":
            date_col = c
            break
    if date_col is None:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing DATE col")

    # Identify a value column (often the series id)
    value_col = None
    # Prefer exact match to series id
    for c in df.columns:
        if c.strip().upper() == series_id.upper():
            value_col = c
            break
    # Fallback: first non-DATE column
    if value_col is None:
        for c in df.columns:
            if c != date_col:
                value_col = c
                break
    if value_col is None:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing VALUE col")

    out = df[[date_col, value_col]].copy()
    out.columns = ["date", "value"]
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out["value"] = pd.to_numeric(out["value"], errors="coerce")

    out = out.dropna(subset=["date"])
    out = out.sort_values("date")
    return out

def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a FRED series into a DataFrame with columns: date (datetime), value (float)
    Prefers FRED JSON API if FRED_API_KEY is set; falls back to fredgraph.csv.
    """
    # 1) JSON API (more stable), requires key
    if FRED_API_KEY:
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
        }
        r = http_get(FRED_API_BASE, params=params, retries=3)
        payload = r.json()
        obs = payload.get("observations", [])
        rows = []
        for o in obs:
            d = pd.to_datetime(o.get("date"), errors="coerce")
            v = to_float_or_none(o.get("value"))
            if pd.isna(d):
                continue
            rows.append((d, v))
        df = pd.DataFrame(rows, columns=["date", "value"]).dropna(subset=["date"])
        df = df.sort_values("date")
        return df

    # 2) Fallback: fredgraph.csv (no key)
    params = {"id": series_id}
    r = http_get(FRED_GRAPH_CSV, params=params, retries=4)
    return parse_fred_csv_text(r.text, series_id=series_id)

def safe_json(obj):
    """
    Recursively replace NaN/inf with None so json.dump(..., allow_nan=False) won’t explode.
    """
    if isinstance(obj, dict):
        return {k: safe_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_json(v) for v in obj]
    if isinstance(obj, float):
        if not math.isfinite(obj):
            return None
        return obj
    return obj

def compact_date(dt: pd.Timestamp) -> str:
    # YYYY-MM-DD
    return dt.strftime("%Y-%m-%d")

def build_union_timeseries(series_map: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Union all series into a single wide df indexed by date with columns = output keys.
    """
    dfs = []
    for key, df in series_map.items():
        tmp = df.copy()
        tmp = tmp.rename(columns={"value": key})
        tmp = tmp.set_index("date")
        dfs.append(tmp[[key]])

    if not dfs:
        return pd.DataFrame(columns=["date"])

    wide = pd.concat(dfs, axis=1, join="outer").sort_index()
    wide = wide.reset_index().rename(columns={"index": "date"})
    return wide

def series_unit_normalize(output_key: str, unit_mode: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Optional normalization:
    - ICSA is level; store as thousands for UI readability
    - REVOLSL is in $ billions already; keep as-is
    """
    out = df.copy()
    if output_key == "ICSA_k":
        # FRED ICSA is weekly level (claims). Convert to thousands.
        out["value"] = out["value"] / 1000.0
    return out


# -----------------------------
# News
# -----------------------------

def build_news_json() -> dict:
    items = []
    for source_name, url in NEWS_FEEDS:
        try:
            feed = feedparser.parse(url)
            entries = feed.entries[:NEWS_MAX_ITEMS_PER_FEED]
            for e in entries:
                title = (e.get("title") or "").strip()
                link = (e.get("link") or "").strip()
                published = (e.get("published") or e.get("updated") or "").strip()
                if not title or not link:
                    continue
                items.append({
                    "source": source_name,
                    "title": title,
                    "url": link,
                    "published": published
                })
        except Exception as ex:
            warn(f"News feed failed for {source_name}: {ex}")

    # Basic dedupe by url
    seen = set()
    deduped = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        deduped.append(it)

    return {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "sources": [n for (n, _) in NEWS_FEEDS],
        },
        "items": deduped
    }


# -----------------------------
# Excel exports
# -----------------------------

def write_excel_metrics(wide: pd.DataFrame) -> None:
    """
    Create:
    - macro_credit_metrics.xlsx : latest snapshot table
    - macro_credit_timeseries.xlsx : full time series (wide)
    """
    # Build latest snapshot per series
    metrics_rows = []
    for _, out_key, unit_mode in SERIES:
        if out_key not in wide.columns:
            continue
        s = wide[["date", out_key]].dropna()
        if s.empty:
            continue
        latest = s.iloc[-1]
        metrics_rows.append({
            "metric_key": out_key,
            "latest_date": latest["date"],
            "latest_value": float(latest[out_key]),
            "unit_mode": unit_mode,
            "source_note": SOURCE_NOTES.get(out_key, ""),
        })
    metrics_df = pd.DataFrame(metrics_rows)

    # Write metrics-only workbook
    with pd.ExcelWriter(OUT_METRICS_XLSX, engine="openpyxl") as w:
        metrics_df.to_excel(w, index=False, sheet_name="metrics")

    # Write combined workbook
    with pd.ExcelWriter(OUT_TIMESERIES_XLSX, engine="openpyxl") as w:
        metrics_df.to_excel(w, index=False, sheet_name="metrics")
        # Timeseries: keep date as YYYY-MM-DD string for readability
        ts = wide.copy()
        ts["date"] = pd.to_datetime(ts["date"]).dt.date.astype(str)
        ts.to_excel(w, index=False, sheet_name="timeseries")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    log("Starting macro-credit updater...")

    series_map: Dict[str, pd.DataFrame] = {}
    fetched_count = 0

    for series_id, out_key, unit_mode in SERIES:
        try:
            df = fetch_fred_series(series_id)
            df = df.dropna(subset=["date"]).copy()
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"])
            df = series_unit_normalize(out_key, unit_mode, df)

            if df.empty:
                warn(f"{series_id} fetched but empty after cleaning; skipping")
                continue

            # Keep only last 15 years to keep JSON light
            cutoff = pd.Timestamp.now(tz=None) - pd.Timedelta(days=365 * 15)
            df = df[df["date"] >= cutoff]

            series_map[out_key] = df[["date", "value"]].copy()
            fetched_count += 1
            log(f"Fetched {series_id} -> {out_key} ({len(df)} rows)")
        except Exception as e:
            warn(f"{series_id} error: {e}")

    if fetched_count == 0:
        # IMPORTANT: do not "fatal" the site by deleting/blanking files.
        # Exit non-zero so you see it, but preserve existing artifacts.
        warn("[FATAL] No series could be fetched. Leaving existing artifacts untouched.")
        return 1

    wide = build_union_timeseries(series_map)

    # Convert date to YYYY-MM-DD
    wide["date"] = pd.to_datetime(wide["date"]).dt.date.astype(str)

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "source_notes": SOURCE_NOTES,
            "fred_api": "json" if bool(FRED_API_KEY) else "fredgraph.csv",
        },
        "data": wide.to_dict(orient="records"),
    }

    payload = safe_json(payload)

    # Write data.json (strict JSON, no NaN)
    with open(OUT_DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, allow_nan=False)

    log(f"Wrote {OUT_DATA_JSON} with {len(payload['data'])} date rows")

    # News
    news = build_news_json()
    news = safe_json(news)
    with open(OUT_NEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, allow_nan=False)
    log(f"Wrote {OUT_NEWS_JSON} with {len(news.get('items', []))} items")

    # Excel exports
    wide_for_excel = pd.DataFrame(payload["data"])
    wide_for_excel["date"] = pd.to_datetime(wide_for_excel["date"], errors="coerce")
    wide_for_excel = wide_for_excel.sort_values("date")

    write_excel_metrics(wide_for_excel)
    log(f"Wrote {OUT_METRICS_XLSX} and {OUT_TIMESERIES_XLSX}")

    log("Updater finished successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
