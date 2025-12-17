#!/usr/bin/env python3
"""
update_data.py

Pulls selected FRED time series + builds:
- data.json (meta + merged time series)
- macro_credit_metrics.xlsx (latest snapshot table)
- macro_credit_timeseries.xlsx (wide time series table)
- news.json (optional stub / simple RSS-ready structure)

Required env:
- FRED_API_KEY : your FRED API key (store in GitHub Actions Secrets)
Optional:
- GITHUB_REPOSITORY, GITHUB_RUN_ID for metadata
"""

from __future__ import annotations

import json
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests


# -----------------------------
# Config
# -----------------------------

FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"
USER_AGENT = "macro-credit-dashboard/1.0 (github-actions)"

TIMEOUT = 30
RETRIES = 3
BACKOFF_S = 1.5

# Dashboard expects these keys/units:
# - *_pct : percent level (e.g., 2.98)
# - *_mil : millions level (e.g., 7.67)
# - *_bil_usd : USD billions (e.g., 1316.8)
# - *_k : thousands (e.g., 236.0)

@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    key: str
    title: str
    subtitle: str
    frequency_hint: str
    fmt: str  # pct | mil | usd_b | k | raw
    # direction only used for risk classification logic if you do it here;
    # your UI can still decide how to colorize. We'll keep it for completeness.
    direction_higher_worse: bool
    include_in_kpi: bool = True


SERIES: List[SeriesDef] = [
    SeriesDef("DRCCLACBS", "DRCCLACBS_pct", "Card 30+ Delinquency (All banks)", "FRED: DRCCLACBS (quarterly, %)", "quarterly", "pct", True, True),
    SeriesDef("CORCCACBS", "CORCCACBS_pct", "Net Charge-off Rate (All banks)", "FRED: CORCCACBS (quarterly, %)", "quarterly", "pct", True, True),
    SeriesDef("TDSP",      "TDSP_pct",      "Debt Service Burden (Households)", "FRED: TDSP (quarterly, %)", "quarterly", "pct", True, True),
    # Mortgage 30+ delinquency (if you’re using a different series id, swap it here)
    SeriesDef("DRM30",     "DRM30_pct",     "Mortgage 30+ Delinquency", "FRED: DRM30 (quarterly, %)", "quarterly", "pct", True, True),

    SeriesDef("REVOLSL",   "REVOLSL_bil_usd", "Revolving Consumer Credit", "FRED: REVOLSL (monthly, $ billions)", "monthly", "usd_b", True, True),
    SeriesDef("JTSJOL",    "JTSJOL_mil",      "Job Openings (Total nonfarm)", "FRED: JTSJOL (monthly, millions)", "monthly", "mil", False, True),
    SeriesDef("UNRATE",    "UNRATE_pct",      "Unemployment Rate", "FRED: UNRATE (monthly, %)", "monthly", "pct", True, True),
    SeriesDef("ICSA",      "ICSA_k",          "Initial Jobless Claims", "FRED: ICSA (weekly, thousands)", "weekly", "k", True, True),

    # Overlay / supporting series (NOT KPI tiles by default)
    SeriesDef("DSPIC96",   "DSPIC96_bil_usd", "Real Disposable Personal Income", "FRED: DSPIC96 (monthly, $ billions)", "monthly", "usd_b", False, False),
    # Optional housing-leading indicators you mentioned (trend-only)
    SeriesDef("PERMIT",    "PERMIT_k",        "Building Permits", "FRED: PERMIT (monthly, thousands)", "monthly", "k", False, False),
    SeriesDef("CSUSHPINSA","CSUSHPINSA_idx",  "Case-Shiller Home Price Index", "FRED: CSUSHPINSA (monthly, index)", "monthly", "raw", False, False),
]


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def http_get_json(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    headers = {"User-Agent": USER_AGENT}
    last_err: Optional[Exception] = None

    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(BACKOFF_S * attempt)
            else:
                raise

    raise RuntimeError(f"Unreachable: {last_err}")


def to_float_or_none(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        if isinstance(v, str):
            v = v.strip()
            if v == "" or v.lower() in {"nan", "na", "null", "."}:
                return None
        f = float(v)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def convert_units(value: Optional[float], fmt: str) -> Optional[float]:
    """
    Convert raw FRED observation to the dashboard unit conventions.

    IMPORTANT:
    - REVOLSL is already in $ billions -> keep as-is for usd_b
    - ICSA is already in thousands -> keep as-is for k
    - JTSJOL is already in millions -> keep as-is for mil
    """
    if value is None:
        return None
    # For most FRED series, value already matches the label in subtitle.
    # So we largely pass through.
    return value


def parse_observations(series_id: str) -> pd.DataFrame:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY is not set. Add it as a GitHub Actions Secret and export to env.")

    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        # Keep as full history; UI will window to last 10y etc.
        "sort_order": "asc",
    }
    payload = http_get_json(FRED_BASE, params=params)
    obs = payload.get("observations", [])
    rows: List[Tuple[str, Optional[float]]] = []
    for o in obs:
        date = o.get("date")
        val = to_float_or_none(o.get("value"))
        rows.append((date, val))

    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype("string")
    df = df.dropna(subset=["date"])
    return df


def merge_series() -> pd.DataFrame:
    dfs = []
    for s in SERIES:
        df = parse_observations(s.series_id)
        # convert units
        df["value"] = df["value"].apply(lambda x: convert_units(x, s.fmt))
        df = df.rename(columns={"value": s.key})
        dfs.append(df)

    # outer merge on date
    out = dfs[0]
    for df in dfs[1:]:
        out = out.merge(df, on="date", how="outer")

    out = out.sort_values("date").reset_index(drop=True)
    return out


def last_valid_point(df: pd.DataFrame, key: str) -> Optional[Tuple[pd.Timestamp, float]]:
    s = df[["date", key]].dropna()
    if s.empty:
        return None
    d = pd.to_datetime(s.iloc[-1]["date"])
    v = float(s.iloc[-1][key])
    return d, v


def point_at_or_before(df: pd.DataFrame, key: str, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    s = df[["date", key]].dropna()
    if s.empty:
        return None
    s["date_ts"] = pd.to_datetime(s["date"])
    s = s[s["date_ts"] <= target]
    if s.empty:
        return None
    d = pd.to_datetime(s.iloc[-1]["date_ts"])
    v = float(s.iloc[-1][key])
    return d, v


def window_values(df: pd.DataFrame, key: str, start: pd.Timestamp) -> List[float]:
    s = df[["date", key]].dropna()
    if s.empty:
        return []
    s["date_ts"] = pd.to_datetime(s["date"])
    s = s[s["date_ts"] >= start]
    return [float(x) for x in s[key].tolist() if x is not None and math.isfinite(float(x))]


def mean_std(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not vals:
        return None, None
    m = sum(vals) / len(vals)
    if len(vals) < 2:
        return m, 0.0
    var = sum((x - m) ** 2 for x in vals) / len(vals)
    return m, math.sqrt(var)


def classify_status(current: float, avg: Optional[float], sd: Optional[float], higher_worse: bool) -> str:
    """
    Simple z-score classification vs 10y distribution.
    """
    if avg is None or sd is None or sd == 0:
        return "healthy"
    z = (current - avg) / sd
    risk = z if higher_worse else (-z)
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"


def safe_number(x: Any) -> Any:
    """
    Recursively convert NaN/inf to None for JSON compliance.
    """
    if x is None:
        return None
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    if isinstance(x, (int, str, bool)):
        return x
    if isinstance(x, dict):
        return {k: safe_number(v) for k, v in x.items()}
    if isinstance(x, list):
        return [safe_number(v) for v in x]
    # pandas types
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    return x


def build_metrics(ts: pd.DataFrame) -> List[Dict[str, Any]]:
    metrics: List[Dict[str, Any]] = []

    for s in SERIES:
        if not s.include_in_kpi:
            continue

        latest = last_valid_point(ts, s.key)
        if not latest:
            continue
        latest_date, latest_value = latest

        # 1y comparison
        one_year_ago = latest_date - pd.DateOffset(years=1)
        prior = point_at_or_before(ts, s.key, one_year_ago)
        delta_abs = None
        delta_pct = None
        if prior and prior[1] is not None:
            prior_value = float(prior[1])
            delta_abs = latest_value - prior_value
            if prior_value != 0:
                delta_pct = (delta_abs / prior_value) * 100.0

        # 10y baseline (distribution)
        ten_years_ago = latest_date - pd.DateOffset(years=10)
        vals_10y = window_values(ts, s.key, ten_years_ago)
        avg_10y, sd_10y = mean_std(vals_10y)

        status = classify_status(latest_value, avg_10y, sd_10y, s.direction_higher_worse)

        metrics.append({
            "key": s.key,
            "series_id": s.series_id,
            "title": s.title,
            "subtitle": s.subtitle,
            "fmt": s.fmt,
            "latest_date": latest_date.date().isoformat(),
            "latest_value": latest_value,
            "avg_10y": avg_10y,
            "sd_10y": sd_10y,
            # YoY: keep *true arithmetic sign* (no “good/bad” flipping)
            "delta_1y_abs": delta_abs,
            "delta_1y_pct": delta_pct,
            "status": status,
        })

    return metrics


def compute_overall_health(metrics: List[Dict[str, Any]]) -> str:
    # Simple rollup: if any stress => stress; else if any tripwire => tripwire; else healthy
    statuses = [m.get("status") for m in metrics if m.get("status")]
    if "stress" in statuses:
        return "stress"
    if "tripwire" in statuses:
        return "tripwire"
    return "healthy"


def build_news_stub() -> Dict[str, Any]:
    """
    You can replace this with a real RSS pull later.
    Keeping it stable prevents the UI from breaking if news.json exists but is empty.
    """
    return {
        "meta": {"last_updated_utc": utc_now_iso()},
        "sections": {
            "Credit / consumer stress": [],
            "Labor / macro signals": [],
        }
    }


def write_excels(ts: pd.DataFrame, metrics: List[Dict[str, Any]]) -> None:
    metrics_df = pd.DataFrame(metrics)
    ts_df = ts.copy()

    # Make sure excel doesn’t get NaN string weirdness
    ts_df = ts_df.replace([math.inf, -math.inf], pd.NA)
    metrics_df = metrics_df.replace([math.inf, -math.inf], pd.NA)

    metrics_df.to_excel("macro_credit_metrics.xlsx", index=False)
    ts_df.to_excel("macro_credit_timeseries.xlsx", index=False)


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    ts = merge_series()

    metrics = build_metrics(ts)
    overall = compute_overall_health(metrics)

    meta = {
        "last_updated_utc": utc_now_iso(),
        "overall_health": overall,
        "repo": os.getenv("GITHUB_REPOSITORY"),
        "run_id": os.getenv("GITHUB_RUN_ID"),
    }

    payload = {
        "meta": meta,
        "metrics": metrics,
        "data": ts.to_dict(orient="records"),
    }

    # HARD sanitize for JSON compliance
    payload = safe_number(payload)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)

    # Keep a stable news.json available for the UI
    news = safe_number(build_news_stub())
    with open("news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2, allow_nan=False)

    write_excels(ts, metrics)

    print("[OK] Wrote data.json, news.json, and Excel outputs")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[FATAL] {e}", file=sys.stderr)
        raise
