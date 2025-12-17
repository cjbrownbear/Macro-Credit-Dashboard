#!/usr/bin/env python3
"""
update_data.py
Fetches macro/credit/labor time series from FRED + a small RSS news digest,
writes:
  - data.json
  - news.json
  - macro_credit_timeseries.xlsx
  - macro_credit_metrics.xlsx

Designed to run in GitHub Actions and commit the updated artifacts to the repo.
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

# Optional RSS parser (preferred). We'll fall back to basic XML parsing if missing.
try:
    import feedparser  # type: ignore
except Exception:  # pragma: no cover
    feedparser = None  # type: ignore

import xml.etree.ElementTree as ET


FRED_OBS_URL = "https://api.stlouisfed.org/fred/series/observations"

# -----------------------------
# Series configuration
# -----------------------------
@dataclass(frozen=True)
class SeriesDef:
    key: str
    series_id: str
    title: str
    definition: str
    units: str               # pct | usd_b | mil | thou | index
    freq: str                # weekly | monthly | quarterly
    direction: int           # +1 higher is worse, -1 lower is worse (for status)
    scale: float = 1.0       # applied to raw numeric values
    include_in_kpi: bool = True


# 8 KPI tiles (order controlled in index.html), plus extra chart-only series below.
SERIES: List[SeriesDef] = [
    # Credit / consumer
    SeriesDef(
        key="DRCCLACBS_pct",
        series_id="DRCCLACBS",
        title="Card 30+ Delinquency",
        definition="Share of credit-card balances that are 30+ days past due — an early consumer stress signal.",
        units="pct",
        freq="quarterly",
        direction=+1,
    ),
    SeriesDef(
        key="CORCCACBS_pct",
        series_id="CORCCACBS",
        title="Net Charge-off Rate",
        definition="Portion of card loans written off as uncollectible — tends to lag delinquencies but confirms credit deterioration.",
        units="pct",
        freq="quarterly",
        direction=+1,
    ),
    SeriesDef(
        key="TDSP_pct",
        series_id="TDSP",
        title="Debt Service Burden",
        definition="Household debt payments as a share of disposable income — a 'squeeze' measure that can rise before delinquencies spike.",
        units="pct",
        freq="quarterly",
        direction=+1,
    ),
    SeriesDef(
        key="DRSFRMACBS_pct",
        series_id="DRSFRMACBS",
        title="Mortgage 30+ Delinquency",
        definition="Share of single-family mortgage balances 30+ days past due — housing stress confirmation signal.",
        units="pct",
        freq="quarterly",
        direction=+1,
    ),

    # Credit / balance sheet
    SeriesDef(
        key="REVOLSL_bil_usd",
        series_id="REVOLSL",
        title="Revolving Consumer Credit",
        definition="Outstanding revolving consumer credit (e.g., credit cards). Higher levels can reflect borrowing pressure.",
        units="usd_b",
        freq="monthly",
        direction=+1,
        include_in_kpi=True,
    ),

    # Labor demand
    SeriesDef(
        key="JTSJOL_mil",
        series_id="JTSJOL",
        title="Job Openings",
        definition="Labor demand proxy — weakens as hiring appetite drops (lower is worse for the labor market).",
        units="mil",
        freq="monthly",
        direction=-1,
    ),
    SeriesDef(
        key="UNRATE_pct",
        series_id="UNRATE",
        title="Unemployment Rate",
        definition="Broad unemployment measure — tends to lag, but persistent increases typically worsen credit performance.",
        units="pct",
        freq="monthly",
        direction=+1,
    ),
    SeriesDef(
        key="ICSA_thou",
        series_id="ICSA",
        title="Initial Jobless Claims",
        definition="Faster-turn labor stress signal — spikes can precede unemployment increases.",
        units="thou",
        freq="weekly",
        direction=+1,
        scale=1.0 / 1000.0,  # store in thousands
    ),

    # ---- Chart-only helpers / enrichers (not KPI tiles) ----
    SeriesDef(
        key="DSPIC96_bil_usd",
        series_id="DSPIC96",
        title="Real Disposable Personal Income",
        definition="Inflation-adjusted household income available to spend — falling levels can pressure debt repayment capacity.",
        units="usd_b",
        freq="monthly",
        direction=-1,  # lower income is worse
        include_in_kpi=False,
    ),
    SeriesDef(
        key="PERMIT_thou_units",
        series_id="PERMIT",
        title="Building Permits",
        definition="New privately-owned housing units authorized — a leading housing-cycle indicator.",
        units="thou",
        freq="monthly",
        direction=-1,  # lower permits can indicate housing slowdown
        scale=1.0,      # FRED reports in thousands SAAR already; keep as thousands of units
        include_in_kpi=False,
    ),
    SeriesDef(
        key="CSUSHPINSA_index",
        series_id="CSUSHPINSA",
        title="Case-Shiller Home Price Index",
        definition="National home price index — wealth/cycle effects; accelerations can support consumption, declines can tighten credit.",
        units="index",
        freq="monthly",
        direction=+1,  # not strictly worse when higher, but we treat sharp declines as worse via normalization in UI
        include_in_kpi=False,
    ),
]


# RSS sources (keep reputable, stable feeds)
RSS_FEEDS: Dict[str, List[str]] = {
    "credit_consumer": [
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",  # Top News (CNBC)
        "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US",
    ],
    "labor_macro": [
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",  # Economy (CNBC)
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",
    ],
}


# -----------------------------
# Utility
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _is_bad_number(x: Any) -> bool:
    try:
        xf = float(x)
    except Exception:
        return True
    return math.isnan(xf) or math.isinf(xf)


def safe_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    if _is_bad_number(x):
        return None
    return float(x)


def mean(xs: List[float]) -> float:
    return float(sum(xs) / len(xs)) if xs else float("nan")


def std(xs: List[float]) -> float:
    if len(xs) < 2:
        return 0.0
    m = mean(xs)
    v = sum((x - m) ** 2 for x in xs) / len(xs)
    return float(math.sqrt(v))


def classify(current: float, avg: float, sd: float, direction: int) -> str:
    """Return healthy|tripwire|stress based on z-score vs 10y baseline.
    direction=+1 means higher is worse; -1 means lower is worse.
    """
    if sd is None or sd == 0 or math.isnan(sd) or math.isnan(avg):
        return "healthy"
    z = (current - avg) / sd
    risk = direction * z
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"


def _http_get(url: str, params: Dict[str, Any], timeout: int = 30, retries: int = 3) -> requests.Response:
    last_err: Optional[Exception] = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "macro-credit-dashboard/1.0"})
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            time.sleep(1.2 * (2 ** i))
    raise RuntimeError(f"HTTP request failed after {retries} retries: {last_err}")


def fetch_fred_series(series_id: str, api_key: str) -> pd.DataFrame:
    """Return DataFrame with columns: date (datetime), value (float|NaN)."""
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        # leaving observation_start blank gives full history; keep it simple and filter later.
    }
    r = _http_get(FRED_OBS_URL, params=params, retries=4)
    payload = r.json()
    obs = payload.get("observations", [])
    rows: List[Tuple[pd.Timestamp, Optional[float]]] = []
    for o in obs:
        ds = o.get("date")
        vs = o.get("value")
        if not ds:
            continue
        try:
            d = pd.to_datetime(ds)
        except Exception:
            continue
        try:
            v = float(vs)
        except Exception:
            v = float("nan")
        rows.append((d, v))
    df = pd.DataFrame(rows, columns=["date", "value"]).sort_values("date")
    return df


def apply_series_scaling(df: pd.DataFrame, s: SeriesDef) -> pd.DataFrame:
    """Apply scale + a couple of sanity heuristics to prevent obvious unit blow-ups."""
    out = df.copy()
    out["value"] = out["value"].astype(float) * float(s.scale)

    # Heuristic fixes for common unit mismatches:
    # - REVOLSL should be ~hundreds/thousands (billions). If we see ~millions, divide by 1000.
    if s.series_id == "REVOLSL":
        med = float(out["value"].dropna().median()) if out["value"].dropna().size else float("nan")
        if not math.isnan(med) and med > 20000:  # too large for "billions"
            out["value"] = out["value"] / 1000.0

    # - ICSA stored as thousands; if still looks like raw counts, divide by 1000 again.
    if s.series_id == "ICSA":
        med = float(out["value"].dropna().median()) if out["value"].dropna().size else float("nan")
        if not math.isnan(med) and med > 5000:  # should be ~200-400 (thousands)
            out["value"] = out["value"] / 1000.0

    # PERMIT is in thousands of units SAAR; typical values ~1k-2k. If it looks like units (millions), adjust.
    if s.series_id == "PERMIT":
        med = float(out["value"].dropna().median()) if out["value"].dropna().size else float("nan")
        if not math.isnan(med) and med > 20000:
            out["value"] = out["value"] / 1000.0

    return out


def value_at_or_near(df: pd.DataFrame, target: pd.Timestamp, tolerance_days: int) -> Optional[Tuple[pd.Timestamp, float]]:
    """Get value nearest to target within tolerance; else None."""
    if df.empty:
        return None
    window = df[(df["date"] >= target - pd.Timedelta(days=tolerance_days)) & (df["date"] <= target + pd.Timedelta(days=tolerance_days))]
    window = window.dropna(subset=["value"])
    if window.empty:
        return None
    # pick nearest
    window = window.assign(dist=(window["date"] - target).abs())
    row = window.sort_values("dist").iloc[0]
    return pd.Timestamp(row["date"]), float(row["value"])


def value_at_or_before(df: pd.DataFrame, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    if df.empty:
        return None
    sub = df[df["date"] <= target].dropna(subset=["value"])
    if sub.empty:
        return None
    row = sub.iloc[-1]
    return pd.Timestamp(row["date"]), float(row["value"])


def values_since(df: pd.DataFrame, start: pd.Timestamp) -> List[float]:
    if df.empty:
        return []
    sub = df[df["date"] >= start].dropna(subset=["value"])
    return [float(v) for v in sub["value"].tolist()]


def compute_series_metrics(df: pd.DataFrame, s: SeriesDef) -> Dict[str, Any]:
    df = df.dropna(subset=["value"]).sort_values("date")
    if df.empty:
        return {"key": s.key, "series_id": s.series_id, "title": s.title, "units": s.units, "freq": s.freq, "direction": s.direction,
                "definition": s.definition, "status": "healthy"}

    latest_row = df.iloc[-1]
    latest_date = pd.Timestamp(latest_row["date"])
    latest_value = float(latest_row["value"])

    # 10y baseline
    ten_years_ago = latest_date - pd.DateOffset(years=10)
    vals10 = values_since(df, ten_years_ago)
    avg10 = mean(vals10) if vals10 else float("nan")
    sd10 = std(vals10) if vals10 else 0.0

    status = classify(latest_value, avg10, sd10, s.direction)

    # YoY delta (prefer "near" match; fall back to at/before)
    one_year_ago = latest_date - pd.DateOffset(years=1)
    tol = 21 if s.freq == "weekly" else 45 if s.freq == "monthly" else 120
    prev = value_at_or_near(df, one_year_ago, tolerance_days=tol) or value_at_or_before(df, one_year_ago)
    prev_date = prev[0] if prev else None
    prev_value = prev[1] if prev else None

    delta_abs = (latest_value - prev_value) if prev_value is not None else None
    delta_pct = ((latest_value - prev_value) / prev_value * 100.0) if (prev_value not in (None, 0)) else None

    # Short rule-based “AI-ish” summary string for charts
    def fmt_num(x: float) -> str:
        if s.units == "pct":
            return f"{x:.2f}".rstrip("0").rstrip(".") + "%"
        if s.units == "mil":
            return f"{x:.2f}".rstrip("0").rstrip(".") + "M"
        if s.units == "thou":
            return f"{x:,.0f}K"
        if s.units == "usd_b":
            return f"${x:,.0f}B"
        if s.units == "index":
            return f"{x:,.1f}"
        return f"{x:,.2f}"

    above_below = ""
    if not math.isnan(avg10):
        diff = latest_value - avg10
        if s.units == "pct":
            above_below = f"{diff:+.2f}pp vs 10y avg"
        elif s.units in ("mil", "thou", "usd_b", "index"):
            above_below = f"{diff:+,.0f} vs 10y avg"
        else:
            above_below = f"{diff:+.2f} vs 10y avg"

    yoy = ""
    if delta_abs is not None:
        if s.units == "pct":
            yoy = f"{delta_abs:+.2f}pp YoY"
        elif s.units == "thou":
            yoy = f"{delta_abs:+,.0f}K YoY"
        elif s.units == "mil":
            yoy = f"{delta_abs:+.2f}M YoY"
        elif s.units == "usd_b":
            yoy = f"{delta_abs:+,.0f}B YoY"
        else:
            yoy = f"{delta_abs:+.2f} YoY"
        if delta_pct is not None:
            yoy += f" ({delta_pct:+.1f}%)"

    summary = f"Latest {fmt_num(latest_value)} ({latest_date.date()}). {yoy}. {above_below}. Status: {status.title()}."

    return {
        "key": s.key,
        "series_id": s.series_id,
        "title": s.title,
        "definition": s.definition,
        "units": s.units,
        "freq": s.freq,
        "direction": s.direction,
        "include_in_kpi": s.include_in_kpi,
        "latest_date": latest_date.strftime("%Y-%m-%d"),
        "latest_value": latest_value,
        "avg_10y": None if math.isnan(avg10) else float(avg10),
        "sd_10y": float(sd10),
        "status": status,
        "delta_1y_abs": None if delta_abs is None else float(delta_abs),
        "delta_1y_pct": None if delta_pct is None else float(delta_pct),
        "prev_1y_date": prev_date.strftime("%Y-%m-%d") if prev_date is not None else None,
        "prev_1y_value": None if prev_value is None else float(prev_value),
        "summary": summary,
    }


def compute_overall_status(metrics: List[Dict[str, Any]]) -> Tuple[str, str]:
    """Compute an overall status + a short summary aligned to it."""
    kpi = [m for m in metrics if m.get("include_in_kpi")]
    if not kpi:
        return "healthy", "Overall: Healthy (no KPI data available)."

    score_map = {"healthy": 0.0, "tripwire": 1.0, "stress": 2.0}
    scores = [score_map.get(m.get("status", "healthy"), 0.0) for m in kpi]
    avg_score = sum(scores) / len(scores)

    if avg_score >= 1.35:
        overall = "stress"
    elif avg_score >= 0.75:
        overall = "tripwire"
    else:
        overall = "healthy"

    tw = [m for m in kpi if m.get("status") == "tripwire"]
    st = [m for m in kpi if m.get("status") == "stress"]

    drivers = st[:2] + [m for m in tw[:3] if m not in st[:2]]
    if drivers:
        driver_txt = ", ".join([d["title"] for d in drivers])
        summary = f"Overall: {overall.title()} — driven by {driver_txt}."
    else:
        summary = f"Overall: {overall.title()}."

    return overall, summary


# -----------------------------
# News
# -----------------------------
def _parse_rss_basic(xml_text: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except Exception:
        return items

    # RSS 2.0
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if title and link:
            items.append({"title": title, "link": link, "published": pub})
    # Atom
    if not items:
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        for entry in root.findall(".//atom:entry", ns):
            title = (entry.findtext("atom:title", default="", namespaces=ns) or "").strip()
            link_el = entry.find("atom:link", ns)
            link = (link_el.get("href") if link_el is not None else "") or ""
            pub = (entry.findtext("atom:updated", default="", namespaces=ns) or "").strip()
            if title and link:
                items.append({"title": title, "link": link, "published": pub})
    return items


def fetch_news(max_items_per_bucket: int = 7) -> Dict[str, Any]:
    buckets: Dict[str, Any] = {
        "credit_consumer": {"title": "Credit / consumer stress", "items": []},
        "labor_macro": {"title": "Labor / macro signals", "items": []},
    }

    seen_links: set[str] = set()
    for bucket, urls in RSS_FEEDS.items():
        out: List[Dict[str, Any]] = []
        for url in urls:
            try:
                r = requests.get(url, timeout=25, headers={"User-Agent": "macro-credit-dashboard/1.0"})
                r.raise_for_status()

                if feedparser is not None:
                    parsed = feedparser.parse(r.text)
                    entries = getattr(parsed, "entries", []) or []
                    for e in entries[:20]:
                        link = getattr(e, "link", "") or ""
                        title = getattr(e, "title", "") or ""
                        published = getattr(e, "published", "") or getattr(e, "updated", "") or ""
                        source = getattr(getattr(e, "source", None), "title", "") if getattr(e, "source", None) else ""
                        if not source:
                            source = url.split("/")[2]
                        if link and title and link not in seen_links:
                            out.append({"title": title.strip(), "link": link.strip(), "published": published, "source": source})
                            seen_links.add(link)
                else:
                    entries = _parse_rss_basic(r.text)
                    for e in entries[:20]:
                        link = e.get("link", "")
                        title = e.get("title", "")
                        if link and title and link not in seen_links:
                            out.append({"title": title, "link": link, "published": e.get("published", ""), "source": url.split("/")[2]})
                            seen_links.add(link)

            except Exception:
                continue

        def _dt(item: Dict[str, Any]) -> float:
            t = item.get("published") or ""
            try:
                return pd.to_datetime(t, utc=True).timestamp()
            except Exception:
                return 0.0

        out = sorted(out, key=_dt, reverse=True)
        buckets[bucket]["items"] = out[:max_items_per_bucket]

    return {"generated_utc": utc_now_iso(), "buckets": buckets}


# -----------------------------
# Output writers
# -----------------------------
def build_wide_timeseries(series_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Outer-join all series on date -> wide df with one row per date."""
    dfs = []
    for key, df in series_frames.items():
        if df.empty:
            continue
        d = df[["date", "value"]].copy()
        d = d.rename(columns={"value": key})
        dfs.append(d)
    if not dfs:
        return pd.DataFrame(columns=["date"])
    out = dfs[0]
    for d in dfs[1:]:
        out = out.merge(d, on="date", how="outer")
    out = out.sort_values("date")
    return out


def sanitize_records(df_wide: pd.DataFrame) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for _, row in df_wide.iterrows():
        rec: Dict[str, Any] = {"date": pd.Timestamp(row["date"]).strftime("%Y-%m-%d")}
        for col in df_wide.columns:
            if col == "date":
                continue
            v = safe_float(row[col])
            rec[col] = v
        out.append(rec)
    return out


def write_excel(timeseries: pd.DataFrame, metrics: List[Dict[str, Any]]) -> None:
    ts = timeseries.copy()
    ts["date"] = ts["date"].dt.strftime("%Y-%m-%d")
    with pd.ExcelWriter("macro_credit_timeseries.xlsx", engine="openpyxl") as xw:
        ts.to_excel(xw, index=False, sheet_name="timeseries")
        pd.DataFrame(metrics).to_excel(xw, index=False, sheet_name="metrics")

    with pd.ExcelWriter("macro_credit_metrics.xlsx", engine="openpyxl") as xw:
        pd.DataFrame(metrics).to_excel(xw, index=False, sheet_name="metrics")


def main() -> int:
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        print("[FATAL] Missing FRED_API_KEY env var. Add it as a repo secret and pass into the workflow.", file=sys.stderr)
        return 1

    last_updated = utc_now_iso()

    series_frames: Dict[str, pd.DataFrame] = {}
    metrics: List[Dict[str, Any]] = []

    failures: List[str] = []

    for s in SERIES:
        try:
            df = fetch_fred_series(s.series_id, api_key=api_key)
            df = apply_series_scaling(df, s)
            series_frames[s.key] = df
            m = compute_series_metrics(df, s)
            metrics.append(m)
            print(f"[OK] {s.series_id} -> {s.key} ({len(df)} obs)")
        except Exception as e:
            failures.append(f"{s.series_id}: {e}")
            series_frames[s.key] = pd.DataFrame(columns=["date", "value"])
            metrics.append({
                "key": s.key, "series_id": s.series_id, "title": s.title, "definition": s.definition,
                "units": s.units, "freq": s.freq, "direction": s.direction, "include_in_kpi": s.include_in_kpi,
                "status": "healthy", "summary": f"No data available for {s.series_id}."
            })
            print(f"[WARN] {s.series_id} failed: {e}", file=sys.stderr)

    overall_status, overall_summary = compute_overall_status(metrics)

    wide = build_wide_timeseries(series_frames)
    if "date" in wide.columns:
        wide["date"] = pd.to_datetime(wide["date"])

    data_records = sanitize_records(wide)

    payload = {
        "meta": {
            "last_updated_utc": last_updated,
            "overall_status": overall_status,
            "overall_summary": overall_summary,
            "failures": failures[:20],
        },
        "metrics": metrics,
        "data": data_records,
    }

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)

    news = fetch_news()
    with open("news.json", "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2, allow_nan=False)

    write_excel(wide, metrics)

    # If ALL KPI series are missing, fail hard so workflow shows error.
    kpi_keys = [s.key for s in SERIES if s.include_in_kpi]
    any_kpi_data = False
    for k in kpi_keys:
        df = series_frames.get(k)
        if df is not None and not df.dropna(subset=["value"]).empty:
            any_kpi_data = True
            break
    if not any_kpi_data:
        print("[FATAL] No KPI series could be fetched. Check API key, connectivity, or series IDs.", file=sys.stderr)
        return 1

    print(f"[DONE] Wrote data.json, news.json, Excel files. Overall: {overall_status}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
