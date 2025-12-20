#!/usr/bin/env python3
"""
update_data.py — Macro/Credit Stress Dashboard updater

Outputs (repo root):
  - data.json  (timeseries + computed KPI tiles + meta summary)
  - news.json  (headline feed from RSS)
  - macro_credit_timeseries.xlsx
  - macro_credit_metrics.xlsx

Requires env var:
  - FRED_API_KEY  (https://fred.stlouisfed.org/docs/api/api_key.html)

Notes:
  - Uses FRED "series/observations" JSON endpoint (more stable than fredgraph.csv).
  - Sanitizes NaN/Inf to None to keep JSON compliant.
  - "YoY change" is true numeric movement (pp for % series; absolute for levels) + optional pct change.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests

try:
  import feedparser  # type: ignore
except Exception:
  feedparser = None

# -----------------------------
# Config
# -----------------------------
FRED_BASE = "https://api.stlouisfed.org/fred"
UA = "Macro-Credit-Dashboard/1.0 (+github-actions)"

# Primary KPI tiles (8) — keys MUST match what index.html expects
SERIES: List[Dict[str, Any]] = [
  # Row 1: card delinquencies, net charge-off, debt service, mortgage 30+ delinquency
  dict(
    key="DRCCLACBS_pct",
    series_id="DRCCLACBS",
    title="Card 30+ Delinquency",
    subtitle="FRED: DRCCLACBS (%, quarterly)",
    unit="pct",
    fmt="pct",
    direction=+1,  # higher is worse
    definition="Share of credit-card balances that are at least 30 days past due — an early consumer stress signal.",
    why="Rising delinquency is an early sign households are being squeezed."
  ),
  dict(
    key="CORCCACBS_pct",
    series_id="CORCCACBS",
    title="Net Charge-off Rate",
    subtitle="FRED: CORCCACBS (%, quarterly)",
    unit="pct",
    fmt="pct",
    direction=+1,  # higher is worse
    definition="Portion of card loans written off as uncollectible — tends to lag delinquencies but confirms credit deterioration.",
    why="Charge-offs typically lag delinquencies but confirm deterioration."
  ),
  dict(
    key="TDSP_pct",
    series_id="TDSP",
    title="Debt Service Burden",
    subtitle="FRED: TDSP (%, quarterly)",
    unit="pct",
    fmt="pct",
    direction=+1,  # higher is worse
    definition="Household debt payments as a percent of disposable personal income — a ‘squeeze’ measure that can rise even before delinquencies spike.",
    why="Higher debt service leaves less buffer to absorb shocks."
  ),
  dict(
    key="DRSFRMACBS_pct",
    series_id="DRSFRMACBS",
    title="Mortgage 30+ Delinquency",
    subtitle="FRED: DRSFRMACBS (%, quarterly)",
    unit="pct",
    fmt="pct",
    direction=+1,  # higher is worse
    definition="Share of residential mortgages 30+ days delinquent (all commercial banks).",
    why="Mortgage delinquencies can jump in downturns and housing stress episodes."
  ),

  # Row 2: revolving credit, job openings, unemployment, initial jobless claims
  dict(
    key="REVOLSL_bil_usd",
    series_id="REVOLSL",
    title="Revolving Consumer Credit",
    subtitle="FRED: REVOLSL ($B, monthly)",
    unit="usd_b",
    fmt="usd_b",
    direction=+1,  # higher is worse (more leverage)
    definition="Total revolving consumer credit outstanding (primarily credit cards).",
    why="Rapid growth can signal households leaning on credit."
  ),
  dict(
    key="JTSJOL_mil",
    series_id="JTSJOL",
    title="Job Openings",
    subtitle="FRED: JTSJOL (millions, monthly)",
    unit="mil",
    fmt="mil",
    direction=-1,  # lower openings is worse
    transform=lambda x: x / 1_000.0,  # FRED is thousands of persons -> millions
    definition="Job openings (JOLTS). Proxy for labor demand.",
    why="Falling openings often precede labor market cooling."
  ),
  dict(
    key="UNRATE_pct",
    series_id="UNRATE",
    title="Unemployment Rate",
    subtitle="FRED: UNRATE (%, monthly)",
    unit="pct",
    fmt="pct",
    direction=+1,  # higher is worse
    definition="Headline unemployment rate (U-3).",
    why="Rising unemployment tends to worsen credit performance."
  ),
  dict(
    key="ICSA_thou",
    series_id="ICSA",
    title="Initial Jobless Claims",
    subtitle="FRED: ICSA (thousands, weekly)",
    unit="thou",
    fmt="thou",
    direction=+1,  # higher is worse
    definition="Weekly initial claims for unemployment insurance.",
    why="Claims are a fast-turn labor stress signal; spikes can precede higher unemployment."
  ),
]

# Overlay series used in charts (not KPI tiles)
EXTRA_SERIES: List[Dict[str, Any]] = [
  dict(
    key="DSPIC96_bil_usd",
    series_id="DSPIC96",
    title="Real Disposable Personal Income",
    subtitle="FRED: DSPIC96 (monthly, $ billions)",
    unit="usd_b",
    fmt="usd_b",
    direction=-1,
    definition="Inflation-adjusted income available to households — falling levels can pressure debt repayment capacity.",
    why="Income growth supports debt repayment capacity."
  ),
]

# RSS feeds for headlines
RSS_SECTIONS = [
  ("Credit / consumer stress", [
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rssindex"),
  ]),
  ("Labor / macro signals", [
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Reuters Business (RSS)", "https://feeds.reuters.com/reuters/businessNews"),
  ]),
]


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
  return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def _http_get_json(url: str, params: Optional[dict] = None, timeout: int = 30) -> dict:
  r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": UA})
  r.raise_for_status()
  return r.json()

def _fred_observations(series_id: str, api_key: str) -> pd.DataFrame:
  """Return DataFrame(date, value_float_or_none) for a FRED series."""
  url = f"{FRED_BASE}/series/observations"
  params = {"series_id": series_id, "api_key": api_key, "file_type": "json"}
  payload = _http_get_json(url, params=params)

  obs = payload.get("observations", []) or []
  out = []
  for o in obs:
    d = o.get("date")
    v = o.get("value")
    if not d:
      continue
    try:
      dd = pd.to_datetime(d)
    except Exception:
      continue

    if v is None or v == ".":
      out.append((dd, None))
      continue
    try:
      fv = float(v)
      if math.isnan(fv) or math.isinf(fv):
        fv = None
    except Exception:
      fv = None
    out.append((dd, fv))

  df = pd.DataFrame(out, columns=["date", "value"]).sort_values("date").reset_index(drop=True)
  return df

def _apply_transform(v: Optional[float], sdef: Dict[str, Any]) -> Optional[float]:
  if v is None:
    return None
  fn = sdef.get("transform")
  if callable(fn):
    try:
      vv = float(fn(v))
      if math.isnan(vv) or math.isinf(vv):
        return None
      return vv
    except Exception:
      return None
  return v

def _safe_float(x: Any) -> Optional[float]:
  try:
    if x is None:
      return None
    v = float(x)
    if math.isnan(v) or math.isinf(v):
      return None
    return v
  except Exception:
    return None

def _mean_std(vals: List[float]) -> Tuple[Optional[float], Optional[float]]:
  if not vals:
    return None, None
  if len(vals) == 1:
    return vals[0], 0.0
  m = sum(vals) / len(vals)
  var = sum((v - m) ** 2 for v in vals) / len(vals)
  return m, math.sqrt(var)

def _latest_point(df: pd.DataFrame, col: str = "value") -> Optional[Tuple[pd.Timestamp, float]]:
  if df is None or df.empty:
    return None
  d = df[["date", col]].dropna()
  if d.empty:
    return None
  row = d.iloc[-1]
  return pd.Timestamp(row["date"]), float(row[col])

def _value_at_or_before(df: pd.DataFrame, col: str, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
  d = df[df["date"] <= target][["date", col]].dropna()
  if d.empty:
    return None
  row = d.iloc[-1]
  return pd.Timestamp(row["date"]), float(row[col])

def _window_values(df: pd.DataFrame, col: str, start: pd.Timestamp) -> List[float]:
  d = df[df["date"] >= start][col].dropna()
  return [float(x) for x in d.tolist() if _safe_float(x) is not None]

def _fmt(v: Optional[float], unit: str) -> str:
  if v is None:
    return "—"
  if unit == "pct":
    return f"{v:.2f}%"
  if unit == "mil":
    return f"{v:.2f}M"
  if unit == "usd_b":
    return f"${int(round(v)):,}B"
  if unit == "thou":
    return f"{int(round(v)):,}K"
  return f"{v:,.2f}"

def classify_status(current: float, avg10: Optional[float], sd10: Optional[float], direction: int) -> str:
  """
  direction: +1 => higher is worse, -1 => lower is worse
  risk = direction * zscore; if avg/sd missing, fall back to healthy.
  """
  if avg10 is None or sd10 is None or sd10 == 0:
    return "healthy"
  z = (current - avg10) / sd10
  risk = direction * z
  if risk >= 2.0:
    return "stress"
  if risk >= 1.0:
    return "tripwire"
  return "healthy"

def json_sanitize(obj: Any) -> Any:
  """Convert pandas/float NaN/Inf/date types into JSON-safe primitives."""
  if obj is None:
    return None
  if isinstance(obj, (dt.datetime, dt.date, pd.Timestamp)):
    return obj.isoformat()[:10]
  if isinstance(obj, float):
    if math.isnan(obj) or math.isinf(obj):
      return None
    return obj
  if isinstance(obj, (int, str, bool)):
    return obj
  if isinstance(obj, dict):
    return {k: json_sanitize(v) for k, v in obj.items()}
  if isinstance(obj, list):
    return [json_sanitize(v) for v in obj]
  return obj

# -----------------------------
# KPI computation
# -----------------------------
def compute_kpis(series_frames: Dict[str, pd.DataFrame], defs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  out: List[Dict[str, Any]] = []
  for s in defs:
    sid = s["series_id"]
    df = series_frames.get(sid)
    if df is None or df.empty:
      continue

    latest = _latest_point(df)
    if not latest:
      continue
    latest_date, latest_val_raw = latest
    latest_val = _apply_transform(latest_val_raw, s)

    # 10y baseline
    ten_years_ago = latest_date - pd.DateOffset(years=10)
    vals10 = _window_values(df, "value", ten_years_ago)
    # apply transform to baseline vals too (if any)
    vals10_t = []
    for v in vals10:
      vv = _apply_transform(v, s)
      if vv is not None:
        vals10_t.append(vv)
    avg10, sd10 = _mean_std(vals10_t)

    status = classify_status(latest_val, avg10, sd10, int(s.get("direction", +1)))

    # True YoY movement:
    one_year_ago = latest_date - pd.DateOffset(years=1)
    prior = _value_at_or_before(df, "value", one_year_ago)
    yoy_abs = None
    yoy_pct = None
    yoy_note = ""
    if prior:
      _, prior_raw = prior
      prior_val = _apply_transform(prior_raw, s)
      if prior_val is not None and latest_val is not None:
        yoy_abs = latest_val - prior_val  # percentage points for pct-series; absolute for others
        if prior_val != 0:
          yoy_pct = (latest_val - prior_val) / abs(prior_val)

    # Sparkline last ~24 points (or less)
    spark_vals = df["value"].dropna().tail(24).tolist()
    spark_vals_t = []
    for v in spark_vals:
      vv = _apply_transform(float(v), s)
      spark_vals_t.append(vv if vv is not None else None)

    unit = s.get("unit", "num")
    # display strings
    val_str = _fmt(latest_val, unit)
    avg_str = _fmt(avg10, unit)

    # delta string: abs + pct-change (if available)
    if yoy_abs is None:
      delta_str = "Δ 1y: —"
    else:
      if unit == "pct":
        abs_str = f"{yoy_abs:+.2f}pp"
      elif unit == "usd_b":
        abs_str = f"{yoy_abs:+.0f}B"
      elif unit == "mil":
        abs_str = f"{yoy_abs:+.2f}M"
      elif unit == "thou":
        abs_str = f"{yoy_abs:+.0f}K"
      else:
        abs_str = f"{yoy_abs:+.2f}"
      if yoy_pct is not None and isfinite(yoy_pct := float(yoy_pct)):
        delta_str = f"Δ 1y: {abs_str} ({yoy_pct*100:+.1f}%)"
      else:
        delta_str = f"Δ 1y: {abs_str}"

    out.append({
      "key": s["key"],
      "series_id": sid,
      "title": s["title"],
      "subtitle": s.get("subtitle", ""),
      "unit": unit,
      "status": status,
      "status_label": "Healthy" if status=="healthy" else ("Tripwire" if status=="tripwire" else "Stress"),
      "value": latest_val,
      "value_str": val_str,
      "as_of": latest_date.strftime("%b %Y"),
      "avg10": avg10,
      "avg_str": avg_str,
      "delta_abs": yoy_abs,
      "delta_pct": yoy_pct,
      "delta_str": delta_str,
      "spark": spark_vals_t,
      "definition": s.get("definition",""),
      "why": s.get("why",""),
    })

  return out

def isfinite(x: float) -> bool:
  return x is not None and isinstance(x, (int, float)) and math.isfinite(float(x))

def overall_status(kpis: List[Dict[str, Any]]) -> str:
  # simple rule: any stress => stress; else any tripwire => tripwire; else healthy
  statuses = [k.get("status") for k in kpis]
  if "stress" in statuses:
    return "stress"
  if "tripwire" in statuses:
    return "tripwire"
  return "healthy"

def make_executive_summary(kpis: List[Dict[str, Any]]) -> Dict[str, Any]:
  """
  Auto-generated executive summary based on KPI system + most recent changes.
  """
  if not kpis:
    return {"text": "No KPI data available.", "updated": utc_now_iso()}

  ov = overall_status(kpis)
  ov_label = "Healthy" if ov=="healthy" else ("Tripwire / Watch" if ov=="tripwire" else "Stress")

  # biggest movers by absolute YoY in standardized terms:
  movers = []
  for k in kpis:
    da = k.get("delta_abs")
    if da is None or not isfinite(float(da)):
      continue
    movers.append((abs(float(da)), k))
  movers.sort(key=lambda x: x[0], reverse=True)
  top = [m[1] for m in movers[:3]]

  def mover_line(k):
    return f"{k['title']} ({k['delta_str']})"

  movers_txt = "; ".join(mover_line(k) for k in top) if top else "No YoY deltas available."

  # status counts
  counts = {"healthy":0, "tripwire":0, "stress":0}
  for k in kpis:
    counts[k["status"]] += 1

  text = (
    f"Overall status is {ov_label}. "
    f"Across the dashboard: {counts['healthy']} healthy, {counts['tripwire']} tripwire, {counts['stress']} stress. "
    f"Biggest YoY moves: {movers_txt}."
  )
  return {"text": text, "updated": utc_now_iso(), "overall": ov}

# -----------------------------
# Trend charts payload
# -----------------------------
def build_trends_payload(series_frames: Dict[str, pd.DataFrame], kpis: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
  # Map KPI defs by series id for metadata
  meta_by_sid = {s["series_id"]: s for s in SERIES}
  meta_by_sid.update({s["series_id"]: s for s in EXTRA_SERIES})

  trends: List[Dict[str, Any]] = []

  # Trend charts:
  # - Each KPI series as its own chart
  # - plus revolving credit overlay w/ real disposable income
  for s in SERIES:
    sid = s["series_id"]
    df = series_frames.get(sid)
    if df is None or df.empty:
      continue

    unit = s.get("unit","num")
    pts = []
    bench = []

    # compute 10y rolling mean line (benchmark)
    d = df.dropna().copy()
    d["value_t"] = d["value"].apply(lambda v: _apply_transform(v, s))
    d = d.dropna(subset=["value_t"])

    # rolling mean on transformed values
    roll = d.set_index("date")["value_t"].rolling(window=120, min_periods=12).mean()  # ~10y monthly; safe-ish
    # for quarterly/weekly, window is approximate; display still useful

    for _, row in d.iterrows():
      val = float(row["value_t"])
      pts.append({
        "date": row["date"].strftime("%Y-%m-%d"),
        "value": val,
        "value_str": _fmt(val, unit),
      })

    # dotted benchmark points
    for dt_idx, v in roll.dropna().items():
      v = float(v)
      bench.append({"date": pd.Timestamp(dt_idx).strftime("%Y-%m-%d"), "value": v})

    # attach summary text for this chart
    kpi_match = next((k for k in kpis if k["series_id"] == sid), None)
    if kpi_match:
      summary_text = f"Latest: {kpi_match['value_str']} ({kpi_match['status_label']}). {kpi_match['delta_str']}."
    else:
      summary_text = "—"

    trends.append({
      "key": s["key"],
      "title": s["title"] + (" (All banks)" if "All banks" not in s["title"] and sid in ["DRCCLACBS","CORCCACBS"] else ""),
      "subtitle": s.get("subtitle",""),
      "definition": s.get("definition",""),
      "source": f"FRED: {sid}",
      "points": pts,
      "bench_points": bench,
      "summary_text": summary_text,
    })

  # Add overlay trend: revolving credit vs real disposable income (index-normalized in the client if desired later)
  # For now we keep it as a single trend card for REVOLSL and show DSPIC96 separately (you can combine later).
  for s in EXTRA_SERIES:
    sid = s["series_id"]
    df = series_frames.get(sid)
    if df is None or df.empty:
      continue
    unit = s.get("unit","num")

    d = df.dropna().copy()
    d["value_t"] = d["value"].apply(lambda v: _apply_transform(v, s))
    d = d.dropna(subset=["value_t"])
    pts=[]
    roll = d.set_index("date")["value_t"].rolling(window=120, min_periods=12).mean()

    for _, row in d.iterrows():
      val = float(row["value_t"])
      pts.append({"date": row["date"].strftime("%Y-%m-%d"), "value": val, "value_str": _fmt(val, unit)})

    bench=[]
    for dt_idx, v in roll.dropna().items():
      bench.append({"date": pd.Timestamp(dt_idx).strftime("%Y-%m-%d"), "value": float(v)})

    trends.append({
      "key": s["key"],
      "title": s["title"],
      "subtitle": s.get("subtitle",""),
      "definition": s.get("definition",""),
      "source": f"FRED: {sid}",
      "points": pts,
      "bench_points": bench,
      "summary_text": "Latest reading shown with a 10-year rolling mean benchmark.",
    })

  return trends

# -----------------------------
# News
# -----------------------------
def fetch_news() -> Dict[str, Any]:
  sections_out = []
  if feedparser is None:
    return {"updated": utc_now_iso(), "sections": []}

  for section_title, feeds in RSS_SECTIONS:
    items = []
    for src, url in feeds:
      try:
        d = feedparser.parse(url)
        for e in (d.entries or [])[:6]:
          items.append({
            "source": src,
            "title": getattr(e, "title", "")[:220],
            "link": getattr(e, "link", ""),
            "published": getattr(e, "published", None) or getattr(e, "updated", None),
          })
      except Exception:
        continue

    sections_out.append({"title": section_title, "items": items[:8]})

  return {"updated": utc_now_iso(), "sections": sections_out}

# -----------------------------
# Main
# -----------------------------
def main():
  api_key = os.getenv("FRED_API_KEY", "").strip()
  if not api_key:
    raise RuntimeError("Missing env var FRED_API_KEY")

  # Fetch all required series frames
  frames: Dict[str, pd.DataFrame] = {}
  all_defs = SERIES + EXTRA_SERIES
  for s in all_defs:
    sid = s["series_id"]
    df = _fred_observations(sid, api_key)
    frames[sid] = df

  # Compute KPIs
  kpis = compute_kpis(frames, SERIES)
  ov = overall_status(kpis)
  execsum = make_executive_summary(kpis)

  # All dates across series for window control
  all_dates = sorted(set(
    d.strftime("%Y-%m-%d")
    for df in frames.values()
    for d in pd.to_datetime(df["date"]).dropna().tolist()
  ))

  # Trends payload
  trends = build_trends_payload(frames, kpis)

  # Build payload
  payload = {
    "updated": utc_now_iso(),
    "overall_status": ov,
    "executive_summary": execsum,
    "kpis": kpis,
    "trends": trends,
    "all_dates": all_dates,
  }

  # Write outputs
  with open("data.json", "w", encoding="utf-8") as f:
    json.dump(json_sanitize(payload), f, ensure_ascii=False, indent=2, allow_nan=False)

  news = fetch_news()
  with open("news.json", "w", encoding="utf-8") as f:
    json.dump(json_sanitize(news), f, ensure_ascii=False, indent=2, allow_nan=False)

  # Excel outputs
  # Timeseries workbook
  with pd.ExcelWriter("macro_credit_timeseries.xlsx", engine="openpyxl") as xl:
    for s in all_defs:
      sid = s["series_id"]
      df = frames.get(sid)
      if df is None or df.empty:
        continue
      df2 = df.copy()
      df2["value_t"] = df2["value"].apply(lambda v: _apply_transform(v, s))
      df2.to_excel(xl, sheet_name=sid[:31], index=False)

  # Metrics workbook
  mdf = pd.DataFrame([{
    "key": k["key"],
    "title": k["title"],
    "series_id": k["series_id"],
    "status": k["status"],
    "value": k["value"],
    "as_of": k["as_of"],
    "avg10": k["avg10"],
    "delta_abs": k["delta_abs"],
    "delta_pct": k["delta_pct"],
  } for k in kpis])
  mdf.to_excel("macro_credit_metrics.xlsx", index=False)

  print("OK: wrote data.json, news.json, macro_credit_timeseries.xlsx, macro_credit_metrics.xlsx")

if __name__ == "__main__":
  main()
