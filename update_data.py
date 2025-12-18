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
- Uses FRED "series/observations" JSON endpoint (stable).
- Sanitizes NaN/Inf to None to keep JSON compliant.
- If a single series fails (bad ID, transient error), we skip it and continue.
"""

from __future__ import annotations

import os
import json
import math
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

import requests
import pandas as pd

# Optional, for RSS headlines (install in workflow via pip)
try:
    import feedparser  # type: ignore
except Exception:
    feedparser = None


# -----------------------------
# Config
# -----------------------------

FRED_BASE = "https://api.stlouisfed.org/fred"
UA = "Macro-Credit-Dashboard/1.1 (+github-actions)"

# KPI tiles (8)
SERIES: List[Dict[str, Any]] = [
    dict(
        key="DRCCLACBS_pct",
        series_id="DRCCLACBS",
        title="Card 30+ Delinquency",
        subtitle="FRED: DRCCLACBS (%; quarterly)",
        unit="pct",
        fmt="pct",
        direction=+1,  # higher is worse
        definition="Share of credit-card balances at least 30 days past due (all banks).",
        why="Rising delinquency is an early sign of household strain.",
    ),
    dict(
        key="CORCCACBS_pct",
        series_id="CORCCACBS",
        title="Net Charge-off Rate",
        subtitle="FRED: CORCCACBS (%; quarterly)",
        unit="pct",
        fmt="pct",
        direction=+1,  # higher is worse
        definition="Portion of credit-card loans written off as uncollectible (all banks).",
        why="Charge-offs typically lag delinquencies but confirm credit deterioration.",
    ),
    dict(
        key="TDSP_pct",
        series_id="TDSP",
        title="Debt Service Burden",
        subtitle="FRED: TDSP (%; quarterly)",
        unit="pct",
        fmt="pct",
        direction=+1,  # higher is worse
        definition="Household debt payments as a percent of disposable personal income.",
        why="Higher debt service leaves less buffer to absorb shocks.",
    ),
    dict(
        key="DRSFRMACBS_pct",
        series_id="DRSFRMACBS",
        title="Mortgage 30+ Delinquency",
        subtitle="FRED: DRSFRMACBS (%; quarterly)",
        unit="pct",
        fmt="pct",
        direction=+1,  # higher is worse
        definition="Share of residential mortgages 30+ days delinquent (all commercial banks).",
        why="Mortgage delinquencies can jump in downturns and housing stress episodes.",
    ),
    dict(
        key="REVOLSL_bil_usd",
        series_id="REVOLSL",
        title="Revolving Consumer Credit",
        subtitle="FRED: REVOLSL ($B; monthly)",
        unit="usd_b",
        fmt="usd_b",
        direction=+1,  # higher is worse (more leverage)
        definition="Total revolving consumer credit outstanding (primarily credit cards).",
        why="Rapid growth can signal households leaning on credit.",
    ),
    dict(
        key="JTSJOL_mil",
        series_id="JTSJOL",
        title="Job Openings",
        subtitle="FRED: JTSJOL (millions; monthly)",
        unit="mil",
        fmt="mil",
        direction=-1,  # lower openings is worse
        transform=lambda x: x / 1000.0,  # FRED is 'thousands of persons' -> millions
        definition="Job openings (JOLTS). Proxy for labor demand.",
        why="Falling openings often precede labor market cooling.",
    ),
    dict(
        key="UNRATE_pct",
        series_id="UNRATE",
        title="Unemployment Rate",
        subtitle="FRED: UNRATE (%; monthly)",
        unit="pct",
        fmt="pct",
        direction=+1,  # higher is worse
        definition="Headline unemployment rate (U-3).",
        why="Rising unemployment tends to worsen credit performance.",
    ),
    dict(
        key="ICSA_thou",
        series_id="ICSA",
        title="Initial Jobless Claims",
        subtitle="FRED: ICSA (thousands; weekly)",
        unit="thou",
        fmt="thou",
        direction=+1,  # higher is worse
        # IMPORTANT: keep as thousands; do NOT multiply by 1,000
        definition="Weekly initial claims for unemployment insurance.",
        why="Claims are a fast-turn labor stress signal; spikes can precede higher unemployment.",
    ),
]

# Optional overlay series (not KPI tiles)
EXTRA_SERIES: List[Dict[str, Any]] = [
    dict(
        key="DSPIC96_bil_usd",
        series_id="DSPIC96",
        title="Real Disposable Personal Income",
        unit="usd_b",
        fmt="usd_b",
        direction=-1,
        definition="Inflation-adjusted disposable personal income (billions of chained dollars).",
        why="Income growth supports debt repayment capacity.",
    ),
]

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

def _req_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": UA})
    return s

def _http_get_json(
    sess: requests.Session,
    url: str,
    params: dict,
    timeout: int = 30,
    retries: int = 4,
    backoff_s: float = 1.2,
) -> dict:
    """
    GET JSON with retry for transient failures.
    - Retries on 429 and 5xx
    - Raises immediately on 400/401/403 (bad request/key)
    """
    last_err: Optional[Exception] = None
    for i in range(retries + 1):
        try:
            r = sess.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                # transient
                time.sleep(backoff_s * (2 ** i))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            # If we got here due to a non-transient HTTP error, don't spin forever
            if isinstance(e, requests.HTTPError):
                status = e.response.status_code if e.response is not None else None
                if status in (400, 401, 403, 404):
                    raise
            if i < retries:
                time.sleep(backoff_s * (2 ** i))
                continue
            raise
    raise RuntimeError(f"HTTP failed: {last_err}")

def _fred_observations(sess: requests.Session, series_id: str, api_key: str) -> pd.DataFrame:
    url = f"{FRED_BASE}/series/observations"
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "asc",
    }
    payload = _http_get_json(sess, url, params=params)
    obs = payload.get("observations", [])
    out: List[Tuple[pd.Timestamp, Optional[float]]] = []
    for o in obs:
        ds = o.get("date")
        vs = o.get("value")
        if not ds:
            continue
        try:
            d = pd.to_datetime(ds)
        except Exception:
            continue
        if vs is None or vs == ".":
            v = None
        else:
            try:
                v = float(vs)
                if math.isnan(v) or math.isinf(v):
                    v = None
            except Exception:
                v = None
        out.append((d, v))
    return pd.DataFrame(out, columns=["date", "value"]).sort_values("date").reset_index(drop=True)

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
    if x is None:
        return None
    try:
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

def _latest_point(df: pd.DataFrame, col: str) -> Optional[Tuple[pd.Timestamp, float]]:
    s = df[["date", col]].dropna()
    if s.empty:
        return None
    row = s.iloc[-1]
    return row["date"], float(row[col])

def _value_at_or_before(df: pd.DataFrame, col: str, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    s = df[df["date"] <= target][["date", col]].dropna()
    if s.empty:
        return None
    row = s.iloc[-1]
    return row["date"], float(row[col])

def _window_values(df: pd.DataFrame, col: str, start: pd.Timestamp) -> List[float]:
    s = df[df["date"] >= start][col].dropna()
    return [float(x) for x in s.tolist() if x is not None]

def fmt_units(unit: str) -> str:
    return {"pct": "%", "mil": "M", "usd_b": "$B", "thou": "K"}.get(unit, "")

def classify_status(current: float, avg: Optional[float], sd: Optional[float], direction: int) -> str:
    # direction: +1 higher worse; -1 lower worse
    if avg is None or sd is None or sd == 0:
        return "healthy"
    z = (current - avg) / sd
    risk = direction * z
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"

def _to_jsonable(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (dt.datetime, dt.date, pd.Timestamp)):
        return str(obj)[:10]
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, (int, str, bool)):
        return obj
    if isinstance(obj, dict):
        return {k: _to_jsonable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_to_jsonable(v) for v in obj]
    return obj


# -----------------------------
# KPI computation
# -----------------------------

def compute_metrics(df: pd.DataFrame, sdefs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    metrics: List[Dict[str, Any]] = []
    for s in sdefs:
        key = s["key"]
        latest = _latest_point(df, key)
        if not latest:
            continue
        latest_date, latest_val = latest

        one_year_ago = latest_date - pd.DateOffset(years=1)
        prior = _value_at_or_before(df, key, one_year_ago)

        ten_years_ago = latest_date - pd.DateOffset(years=10)
        vals10 = _window_values(df, key, ten_years_ago)
        avg10, sd10 = _mean_std(vals10)

        status = classify_status(latest_val, avg10, sd10, int(s.get("direction", +1)))

        # TRUE YoY movement (not "good vs bad"):
        yoy_abs = None
        yoy_pct = None
        yoy_note = ""
        if prior:
            _, prior_val = prior
            yoy_abs = latest_val - prior_val
            if prior_val != 0:
                yoy_pct = (latest_val - prior_val) / abs(prior_val) * 100.0
            if s.get("unit") == "pct":
                yoy_note = "pp"  # percentage points

        why_status = []
        if avg10 is not None and sd10 is not None and sd10 != 0:
            z = (latest_val - avg10) / sd10
            risk = int(s.get("direction", +1)) * z
            why_status.append(f"10y avg {avg10:.2f}{fmt_units(s.get('unit',''))}, σ {sd10:.2f}{fmt_units(s.get('unit',''))}.")
            if risk >= 2:
                why_status.append("≥2σ worse than baseline (Stress).")
            elif risk >= 1:
                why_status.append("≥1σ worse than baseline (Tripwire).")
            else:
                why_status.append("Within ~1σ of baseline (Healthy).")
        else:
            why_status.append("Insufficient variance in 10y window; defaulted to Healthy.")

        metrics.append({
            "key": key,
            "series_id": s.get("series_id"),
            "title": s.get("title"),
            "subtitle": s.get("subtitle"),
            "unit": s.get("unit"),
            "format": s.get("fmt"),
            "direction": int(s.get("direction", +1)),
            "status": status,
            "definition": s.get("definition", ""),
            "why": s.get("why", ""),
            "why_status": " ".join(why_status),
            "latest_date": str(latest_date.date()),
            "latest_value": _safe_float(latest_val),
            "baseline_10y_avg": _safe_float(avg10),
            "baseline_10y_sd": _safe_float(sd10),
            "yoy_abs": _safe_float(yoy_abs),
            "yoy_pct": _safe_float(yoy_pct),
            "yoy_abs_unit": yoy_note,  # "pp" for percent series
        })

    order = {s["key"]: i for i, s in enumerate(sdefs)}
    metrics.sort(key=lambda x: order.get(x["key"], 999))
    return metrics

def overall_health(metrics: List[Dict[str, Any]]) -> Tuple[str, Dict[str, int]]:
    counts = {"healthy": 0, "tripwire": 0, "stress": 0}
    for m in metrics:
        st = m.get("status")
        if st in counts:
            counts[st] += 1

    if counts["stress"] >= 2:
        overall = "stress"
    elif counts["stress"] == 1 or counts["tripwire"] >= 3:
        overall = "tripwire"
    else:
        overall = "healthy"
    return overall, counts

def executive_summary(metrics: List[Dict[str, Any]], counts: Dict[str, int]) -> str:
    if not metrics:
        return "No KPI data available yet."

    movers = []
    for m in metrics:
        if m.get("unit") == "pct":
            abs_move = abs(_safe_float(m.get("yoy_abs")) or 0.0)
        else:
            abs_move = abs(_safe_float(m.get("yoy_pct")) or 0.0)
        movers.append((abs_move, m))
    movers.sort(key=lambda t: t[0], reverse=True)

    top = [mm for _, mm in movers[:3] if (_safe_float(mm.get("yoy_abs")) or _safe_float(mm.get("yoy_pct")))]
    top_bits = []
    for m in top:
        if m.get("unit") == "pct":
            da = m.get("yoy_abs")
            if da is not None:
                top_bits.append(f"{m['title']} is {'up' if da > 0 else 'down'} {abs(da):.2f}pp YoY.")
        else:
            dp = m.get("yoy_pct")
            if dp is not None:
                top_bits.append(f"{m['title']} is {'up' if dp > 0 else 'down'} {abs(dp):.1f}% YoY.")

    status_line = f"System status: {counts['healthy']} healthy, {counts['tripwire']} tripwire, {counts['stress']} stress."
    return status_line + (" Biggest moves: " + " ".join(top_bits) if top_bits else "")


# -----------------------------
# News (RSS)
# -----------------------------

def build_news() -> Dict[str, Any]:
    if feedparser is None:
        return {
            "meta": {"last_updated_utc": utc_now_iso(), "note": "feedparser not installed; skipping RSS"},
            "sections": [],
        }

    sections_out = []
    for section_title, feeds in RSS_SECTIONS:
        items = []
        for source, url in feeds:
            try:
                fp = feedparser.parse(url)
                for e in fp.entries[:8]:
                    title = getattr(e, "title", "").strip()
                    link = getattr(e, "link", "").strip()
                    if not title or not link:
                        continue
                    published = None
                    if getattr(e, "published_parsed", None):
                        published = dt.datetime.fromtimestamp(
                            time.mktime(e.published_parsed), tz=dt.timezone.utc
                        ).isoformat().replace("+00:00", "Z")
                    items.append({
                        "title": title,
                        "url": link,
                        "source": source,
                        "published_utc": published,
                    })
            except Exception:
                continue

        seen = set()
        dedup = []
        for it in items:
            if it["url"] in seen:
                continue
            seen.add(it["url"])
            dedup.append(it)

        sections_out.append({"title": section_title, "items": dedup[:10]})

    return {"meta": {"last_updated_utc": utc_now_iso()}, "sections": sections_out}


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    api_key = os.getenv("FRED_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Missing env var FRED_API_KEY. Set it in GitHub repo secrets and workflow env.")

    sess = _req_session()

    all_defs = SERIES + EXTRA_SERIES

    frames: List[pd.DataFrame] = []
    fetched_any = False

    for s in all_defs:
        sid = s["series_id"]
        try:
            df = _fred_observations(sess, sid, api_key)
        except requests.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code in (400, 401, 403):
                # This is NOT transient: either bad key or bad series_id params.
                # Raise with a clearer message.
                raise SystemExit(
                    f"FRED request failed ({code}) for series_id={sid}. "
                    f"Most likely: invalid FRED_API_KEY OR invalid series_id."
                ) from e
            print(f"[WARN] Skipping {sid}: HTTP error {code}")
            continue
        except Exception as e:
            print(f"[WARN] Skipping {sid}: {e}")
            continue

        if df.empty:
            print(f"[WARN] {sid} returned 0 observations; skipping")
            continue

        fetched_any = True
        df.rename(columns={"value": s["key"]}, inplace=True)

        if "transform" in s:
            df[s["key"]] = df[s["key"]].apply(lambda v: _apply_transform(v, s))

        frames.append(df)

    if not fetched_any or not frames:
        raise SystemExit("No FRED series could be fetched. Check FRED_API_KEY and series IDs.")

    merged = frames[0]
    for df in frames[1:]:
        merged = pd.merge(merged, df, on="date", how="outer")
    merged = merged.sort_values("date").reset_index(drop=True)

    # Derived overlay ratio (optional)
    if "REVOLSL_bil_usd" in merged.columns and "DSPIC96_bil_usd" in merged.columns:
        ratio = []
        for a, b in zip(merged["REVOLSL_bil_usd"].tolist(), merged["DSPIC96_bil_usd"].tolist()):
            av = _safe_float(a)
            bv = _safe_float(b)
            if av is None or bv in (None, 0.0):
                ratio.append(None)
            else:
                ratio.append(av / bv)
        merged["REVOLSL_to_DSPIC96_ratio"] = ratio

    metrics = compute_metrics(merged, SERIES)
    overall, counts = overall_health(metrics)
    exec_sum = executive_summary(metrics, counts)

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "overall_health": overall,
            "health_counts": counts,
            "executive_summary": exec_sum,
        },
        "metrics": metrics,
        "data": [],
    }

    for _, row in merged.iterrows():
        rec = {"date": str(pd.to_datetime(row["date"]).date())}
        for c in merged.columns:
            if c == "date":
                continue
            rec[c] = _safe_float(row[c])
        payload["data"].append(rec)

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(_to_jsonable(payload), f, ensure_ascii=False, indent=2)

    news = build_news()
    with open("news.json", "w", encoding="utf-8") as f:
        json.dump(_to_jsonable(news), f, ensure_ascii=False, indent=2)

    merged_out = merged.copy()
    merged_out["date"] = merged_out["date"].dt.date
    merged_out.to_excel("macro_credit_timeseries.xlsx", index=False)

    pd.DataFrame(metrics).to_excel("macro_credit_metrics.xlsx", index=False)

    print("Wrote: data.json, news.json, macro_credit_timeseries.xlsx, macro_credit_metrics.xlsx")


if __name__ == "__main__":
    main()
