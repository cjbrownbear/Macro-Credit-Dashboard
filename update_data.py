#!/usr/bin/env python3
"""
Update macro/credit dashboard datasets.

Outputs (repo root):
- data.json (merged timeseries + metadata + computed KPI snapshot)
- news.json (RSS-based headlines)
- macro_credit_timeseries.xlsx (wide timeseries)
- macro_credit_metrics.xlsx (KPI snapshot table)

Designed to run in GitHub Actions.
"""

from __future__ import annotations

import json
import math
import os
import re
import numbers
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import feedparser


# -----------------------------
# Config
# -----------------------------

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()

# FRED series used by the dashboard. The JS expects these output keys.
SERIES = [
    # Credit / household stress
    ("DRCCLACBS", "DRCCLACBS_pct"),        # Delinquency Rate on Credit Card Loans (%)
    ("CORCCACBS", "CORCCACBS_pct"),        # Charge-Off Rate on Credit Card Loans (%)
    ("REVOLSL", "REVOLSL_bil_usd"),        # Revolving consumer credit ($B)
    ("TDSP", "TDSP_pct"),                  # Household debt service payments as % of DPI (%)

    # Labor / income
    ("JTSJOL", "JTSJOL_mil"),              # Job openings (millions)
    ("UNRATE", "UNRATE_pct"),              # Unemployment rate (%)
    ("ICSA", "ICSA_thou"),                 # Initial jobless claims (thousands)
    ("DSPIC96", "DSPIC96_bil_usd"),        # Real disposable personal income ($B, chained 2017)
]

# RSS feeds for headlines (reputable + simple)
RSS_FEEDS = [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
    ("Reuters Business", "https://www.reutersagency.com/feed/?best-topics=business-finance&post_type=best"),
]

NEWS_CATEGORIES = {
    "credit": ["credit", "consumer", "delinquen", "charge-off", "loan", "debt", "mortgage"],
    "labor":  ["jobs", "labor", "unemployment", "claims", "hiring", "payroll", "layoff"],
}

USER_AGENT = "Macro-Credit-Dashboard/1.0 (GitHub Actions)"


# -----------------------------
# Helpers: JSON sanitization
# -----------------------------

def _to_builtin_number(x: Any) -> Any:
    """
    Convert numpy/pandas scalars to Python builtins when possible.
    (Avoids importing numpy directly.)
    """
    if hasattr(x, "item") and callable(getattr(x, "item")):
        try:
            return x.item()
        except Exception:
            pass
    return x


def json_sanitize(obj: Any) -> Any:
    """
    Recursively convert values to JSON-safe types.

    - NaN/Inf -> None
    - numpy/pandas scalars -> Python scalars
    """
    obj = _to_builtin_number(obj)

    if obj is None:
        return None
    if isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, numbers.Integral):
        return int(obj)
    if isinstance(obj, numbers.Real):
        val = float(obj)
        if math.isnan(val) or math.isinf(val):
            return None
        return val
    if isinstance(obj, dict):
        return {str(k): json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_sanitize(v) for v in obj]

    # Fallback: stringify anything weird
    return str(obj)


# -----------------------------
# Fetching
# -----------------------------

def http_get(url: str, params: Optional[dict] = None, timeout: int = 30) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r


def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch a single FRED series as a tidy dataframe(date,value).

    Uses FRED JSON API when an API key is present; otherwise falls back to the
    fredgraph CSV endpoint (no key required).
    """
    if FRED_API_KEY:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
        }
        r = http_get(url, params=params)
        obs = r.json().get("observations", [])
        rows = []
        for o in obs:
            d = o.get("date")
            v = o.get("value")
            try:
                val = float(v)
            except Exception:
                val = math.nan
            rows.append((pd.to_datetime(d), val))
        df = pd.DataFrame(rows, columns=["date", "value"])
        return df.sort_values("date").reset_index(drop=True)

    # Fallback: fredgraph CSV (no key)
    url = "https://fred.stlouisfed.org/graph/fredgraph.csv"
    r = http_get(url, params={"id": series_id})

    # CSV is: DATE,<SERIES_ID>
    df = pd.read_csv(pd.io.common.StringIO(r.text))
    if "DATE" not in df.columns:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing DATE")

    value_cols = [c for c in df.columns if c != "DATE"]
    if not value_cols:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing value column")

    value_col = value_cols[0]
    df = df.rename(columns={"DATE": "date", value_col: "value"})
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.sort_values("date").reset_index(drop=True)


# -----------------------------
# KPI computations
# -----------------------------

def last_non_null(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, float]]:
    df2 = df.dropna(subset=["value"])
    if df2.empty:
        return None
    row = df2.iloc[-1]
    return row["date"], float(row["value"])


def value_at_or_before(df: pd.DataFrame, target_date: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    df2 = df.dropna(subset=["value"])
    df2 = df2[df2["date"] <= target_date]
    if df2.empty:
        return None
    row = df2.iloc[-1]
    return row["date"], float(row["value"])


def window_values(df: pd.DataFrame, start_date: pd.Timestamp) -> List[float]:
    df2 = df.dropna(subset=["value"])
    df2 = df2[df2["date"] >= start_date]
    return [float(x) for x in df2["value"].tolist()]


def mean(vals: List[float]) -> float:
    if not vals:
        return math.nan
    return float(sum(vals) / len(vals))


def std(vals: List[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = mean(vals)
    if math.isnan(m):
        return 0.0
    var = sum((x - m) ** 2 for x in vals) / len(vals)
    return float(math.sqrt(var))


def classify(current: float, avg: float, sd: float, direction: int) -> str:
    """
    direction: +1 means higher is worse; -1 means lower is worse
    """
    if not sd or sd == 0 or math.isnan(sd) or math.isnan(avg):
        return "healthy"
    z = (current - avg) / sd
    risk = direction * z
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"


def yoy_delta(current: float, prior: Optional[float], mode: str) -> Dict[str, Optional[float]]:
    """
    Return both absolute delta and percent change.
    For percent-rate series (mode == 'pct'), pp == abs (percentage points).
    """
    out: Dict[str, Optional[float]] = {"abs": None, "pct": None, "pp": None}
    if prior is None:
        return out

    out["abs"] = float(current - prior)
    if prior != 0:
        out["pct"] = float((current - prior) / abs(prior) * 100.0)
    if mode == "pct":
        out["pp"] = float(current - prior)
    return out


def compute_kpis(series_dfs: Dict[str, pd.DataFrame]) -> List[dict]:
    defs = [
        {"series": "DRCCLACBS", "key": "DRCCLACBS_pct", "title": "Card 30+ Delinquency", "sub": "FRED: DRCCLACBS (%, quarterly)", "fmt": "pct", "direction": +1},
        {"series": "CORCCACBS", "key": "CORCCACBS_pct", "title": "Net Charge-off Rate", "sub": "FRED: CORCCACBS (%, quarterly)", "fmt": "pct", "direction": +1},
        {"series": "TDSP",     "key": "TDSP_pct",      "title": "Debt Service Burden", "sub": "FRED: TDSP (%, quarterly)",     "fmt": "pct", "direction": +1},
        {"series": "REVOLSL",  "key": "REVOLSL_bil_usd","title": "Revolving Consumer Credit", "sub": "FRED: REVOLSL ($B, monthly)", "fmt": "usd_b", "direction": +1},

        {"series": "JTSJOL",   "key": "JTSJOL_mil",    "title": "Job Openings", "sub": "FRED: JTSJOL (millions, monthly)", "fmt": "mil", "direction": -1},
        {"series": "UNRATE",   "key": "UNRATE_pct",   "title": "Unemployment Rate", "sub": "FRED: UNRATE (%, monthly)", "fmt": "pct", "direction": +1},
        {"series": "ICSA",     "key": "ICSA_thou",    "title": "Initial Jobless Claims", "sub": "FRED: ICSA (thousands, weekly)", "fmt": "k", "direction": +1},
        {"series": "DSPIC96",  "key": "DSPIC96_bil_usd","title": "Real Disposable Income", "sub": "FRED: DSPIC96 ($B, monthly)", "fmt": "usd_b", "direction": -1},
    ]

    out = []
    for d in defs:
        df = series_dfs.get(d["series"])
        if df is None:
            continue

        latest = last_non_null(df)
        if not latest:
            continue

        latest_date, latest_val = latest
        one_year_ago = latest_date - pd.DateOffset(years=1)
        prior = value_at_or_before(df, one_year_ago)
        prior_val = prior[1] if prior else None

        ten_year_ago = latest_date - pd.DateOffset(years=10)
        vals10y = window_values(df, ten_year_ago)
        avg10y = mean(vals10y)
        sd10y = std(vals10y)

        status = classify(latest_val, avg10y, sd10y, d["direction"])
        dy = yoy_delta(latest_val, prior_val, d["fmt"])

        out.append({
            "key": d["key"],
            "series": d["series"],
            "title": d["title"],
            "sub": d["sub"],
            "fmt": d["fmt"],
            "direction": d["direction"],
            "latest": {"date": latest_date.strftime("%Y-%m-%d"), "value": latest_val},
            "avg10y": avg10y,
            "sd10y": sd10y,
            "status": status,
            "yoy": {
                "asof": prior[0].strftime("%Y-%m-%d") if prior else None,
                "abs": dy["abs"],
                "pct": dy["pct"],
                "pp": dy["pp"],
            }
        })
    return out


def overall_status(kpis: List[dict]) -> str:
    if not kpis:
        return "unknown"
    score_map = {"healthy": 0, "tripwire": 1, "stress": 2}
    scores = [score_map.get(k.get("status"), 0) for k in kpis]
    avg = sum(scores) / len(scores)
    if avg >= 1.4:
        return "stress"
    if avg >= 0.7:
        return "tripwire"
    return "healthy"


# -----------------------------
# Build merged time series (wide)
# -----------------------------

def build_timeseries(series_dfs: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    merged = None
    for series_id, out_key in SERIES:
        df = series_dfs.get(series_id)
        if df is None:
            continue
        dfx = df.copy()
        dfx = dfx.rename(columns={"value": out_key})
        if merged is None:
            merged = dfx
        else:
            merged = pd.merge(merged, dfx, on="date", how="outer")

    if merged is None:
        return pd.DataFrame(columns=["date"])

    merged = merged.sort_values("date").reset_index(drop=True)
    return merged


def to_records(df: pd.DataFrame) -> List[dict]:
    if df.empty:
        return []
    tmp = df.copy()
    tmp["date"] = tmp["date"].dt.strftime("%Y-%m-%d")
    records = tmp.to_dict(orient="records")
    return json_sanitize(records)


# -----------------------------
# News
# -----------------------------

def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def pick_bucket(title: str) -> str:
    t = (title or "").lower()
    for bucket, kws in NEWS_CATEGORIES.items():
        if any(k in t for k in kws):
            return bucket
    return "other"


def fetch_news(max_items_per_bucket: int = 8) -> dict:
    items = []
    for src_name, url in RSS_FEEDS:
        try:
            feed = feedparser.parse(url)
            for e in feed.entries[:25]:
                title = clean_text(getattr(e, "title", ""))
                link = getattr(e, "link", "")
                published = getattr(e, "published", "") or getattr(e, "updated", "")
                items.append({
                    "source": src_name,
                    "title": title,
                    "link": link,
                    "published": clean_text(published),
                    "bucket": pick_bucket(title),
                })
        except Exception:
            continue

    buckets = {"credit": [], "labor": [], "other": []}
    for it in items:
        buckets.setdefault(it.get("bucket", "other"), []).append(it)

    # dedupe by link+title and cap
    for b, arr in buckets.items():
        seen = set()
        out = []
        for it in arr:
            key = (it.get("link") or "") + "|" + (it.get("title") or "")
            if key in seen:
                continue
            seen.add(key)
            out.append(it)
            if len(out) >= max_items_per_bucket:
                break
        buckets[b] = out

    return {
        "meta": {
            "last_updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sources": [n for n, _ in RSS_FEEDS],
        },
        "buckets": buckets,
    }


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    series_dfs: Dict[str, pd.DataFrame] = {}
    errors: List[str] = []

    for sid, _out_key in SERIES:
        try:
            series_dfs[sid] = fetch_fred_series(sid)
        except Exception as e:
            errors.append(f"{sid}: {e}")

    if not series_dfs:
        raise RuntimeError("No FRED series could be fetched. " + "; ".join(errors[:5]))

    ts_df = build_timeseries(series_dfs)
    kpis = compute_kpis(series_dfs)
    overall = overall_status(kpis)

    payload = {
        "meta": {
            "last_updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fred_api_key_present": bool(FRED_API_KEY),
            "fetch_errors": errors,
            "overall_status": overall,
        },
        "kpis": kpis,
        "data": to_records(ts_df),
    }

    # FINAL sanitize (belt + suspenders)
    payload = json_sanitize(payload)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)

    # Excel exports
    ts_df.to_excel("macro_credit_timeseries.xlsx", index=False)

    # Flatten KPI snapshot for the small metrics Excel
    kpi_rows = []
    for k in kpis:
        kpi_rows.append({
            "key": k.get("key"),
            "series": k.get("series"),
            "title": k.get("title"),
            "status": k.get("status"),
            "latest_date": (k.get("latest") or {}).get("date"),
            "latest_value": (k.get("latest") or {}).get("value"),
            "avg10y": k.get("avg10y"),
            "sd10y": k.get("sd10y"),
            "yoy_abs": (k.get("yoy") or {}).get("abs"),
            "yoy_pct": (k.get("yoy") or {}).get("pct"),
            "yoy_pp": (k.get("yoy") or {}).get("pp"),
        })
    pd.DataFrame(kpi_rows).to_excel("macro_credit_metrics.xlsx", index=False)

    # News
    news = json_sanitize(fetch_news())
    with open("news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2, allow_nan=False)

    print("Wrote data.json, news.json, macro_credit_timeseries.xlsx, macro_credit_metrics.xlsx")


if __name__ == "__main__":
    main()
