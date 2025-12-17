#!/usr/bin/env python3
"""
update_data.py

Fetches public macro/credit series from FRED, builds:
- data.json (timeseries + per-metric metadata)
- news.json (RSS headlines)
- macro_credit_metrics.xlsx (metrics tab)
- macro_credit_timeseries.xlsx (metrics + timeseries tabs)

Designed to run in GitHub Actions.
"""

import os
import json
import math
import time
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd

try:
    import feedparser
except Exception:
    feedparser = None


# -----------------------------
# Config
# -----------------------------
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
USER_AGENT = "Macro-Credit-Dashboard/1.2 (GitHub Actions)"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

OUT_DATA_JSON = "data.json"
OUT_NEWS_JSON = "news.json"
OUT_XLSX_METRICS = "macro_credit_metrics.xlsx"
OUT_XLSX_TS = "macro_credit_timeseries.xlsx"

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    key: str
    title: str
    units: str        # "pct", "mil", "usd_b", "thou"
    freq: str         # "quarterly" | "monthly" | "weekly"
    direction: int    # +1 higher=worse, -1 lower=worse (used only for status)
    description: str
    scale: float = 1.0  # applied to raw FRED values before storing (e.g., ICSA / 1000)


# KPI set (8 tiles + 8 trend charts)
SERIES: List[SeriesDef] = [
    SeriesDef("DRCCLACBS", "DRCCLACBS_pct", "Card 30+ Delinquency", "pct", "quarterly", +1,
              "Share of credit card balances 30+ days past due (all banks).", 1.0),
    SeriesDef("CORCCACBS", "CORCCACBS_pct", "Net Charge-off Rate", "pct", "quarterly", +1,
              "Share of card balances charged off as uncollectible (all banks).", 1.0),
    SeriesDef("TDSP", "TDSP_pct", "Debt Service Burden", "pct", "quarterly", +1,
              "Household debt service payments as a share of disposable income.", 1.0),
    SeriesDef("DRSFRMACBS", "DRSFRMACBS_pct", "Mortgage 30+ Delinquency", "pct", "quarterly", +1,
              "Share of residential mortgage balances 30+ days past due (all banks).", 1.0),
    SeriesDef("REVOLSL", "REVOLSL_bil_usd", "Revolving Consumer Credit", "usd_b", "monthly", +1,
              "Outstanding revolving consumer credit (nominal, $ billions).", 1.0),
    SeriesDef("JTSJOL", "JTSJOL_mil", "Job Openings", "mil", "monthly", -1,
              "Total nonfarm job openings (labor demand proxy; lower is worse).", 1.0),
    SeriesDef("UNRATE", "UNRATE_pct", "Unemployment Rate", "pct", "monthly", +1,
              "Unemployment rate (U-3).", 1.0),
    # FRED ICSA is a raw count; dashboard wants "thousands"
    SeriesDef("ICSA", "ICSA_thou", "Initial Jobless Claims", "thou", "weekly", +1,
              "Weekly initial unemployment insurance claims (higher can signal stress).", 1.0 / 1000.0),
]


NEWS_FEEDS = {
    "Credit / consumer stress": [
        "https://finance.yahoo.com/rss/topstories",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    ],
    "Macro / markets": [
        "https://www.cnbc.com/id/10000664/device/rss/rss.html",
        "https://finance.yahoo.com/rss/industry",
    ],
    "Labor / layoffs": [
        "https://www.cnbc.com/id/10000113/device/rss/rss.html",
        "https://finance.yahoo.com/rss/",
    ],
}


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return dt.datetime.now(tz=dt.timezone.utc).isoformat().replace("+00:00", "Z")


def safe_float(x) -> Optional[float]:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return None
        return float(x)
    except Exception:
        return None


def sanitize_records(recs: List[dict]) -> List[dict]:
    """Ensure JSON-safe numbers (no NaN/inf)."""
    out = []
    for r in recs:
        rr = {}
        for k, v in r.items():
            if isinstance(v, float):
                if math.isnan(v) or math.isinf(v):
                    rr[k] = None
                else:
                    rr[k] = v
            else:
                rr[k] = v
        out.append(rr)
    return out


def mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def std(vals: List[float]) -> float:
    if not vals or len(vals) < 2:
        return 0.0
    m = mean(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / len(vals))


def last_non_null(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, float]]:
    d = df.dropna(subset=["value"]).sort_values("date")
    if d.empty:
        return None
    r = d.iloc[-1]
    return pd.Timestamp(r["date"]), float(r["value"])


def prev_non_null(df: pd.DataFrame) -> Optional[Tuple[pd.Timestamp, float]]:
    d = df.dropna(subset=["value"]).sort_values("date")
    if len(d) < 2:
        return None
    r = d.iloc[-2]
    return pd.Timestamp(r["date"]), float(r["value"])


def value_at_or_before(df: pd.DataFrame, target: pd.Timestamp) -> Optional[Tuple[pd.Timestamp, float]]:
    d = df.dropna(subset=["value"]).sort_values("date")
    d = d[d["date"] <= target]
    if d.empty:
        return None
    r = d.iloc[-1]
    return pd.Timestamp(r["date"]), float(r["value"])


def yoy_by_observation(df: pd.DataFrame, freq: str) -> Optional[Tuple[pd.Timestamp, float]]:
    """
    YoY baseline picked by observation count to avoid picking the "wrong" nearby date:
      quarterly: 4 obs
      monthly: 12 obs
      weekly: 52 obs
    Falls back to None if insufficient history.
    """
    offsets = {"quarterly": 4, "monthly": 12, "weekly": 52}
    k = offsets.get(freq)
    if not k:
        return None
    d = df.dropna(subset=["value"]).sort_values("date").reset_index(drop=True)
    if len(d) <= k:
        return None
    r = d.iloc[-1 - k]
    return pd.Timestamp(r["date"]), float(r["value"])


def values_since(df: pd.DataFrame, start: pd.Timestamp) -> List[float]:
    d = df.dropna(subset=["value"]).sort_values("date")
    d = d[d["date"] >= start]
    return [float(x) for x in d["value"].tolist()]


def classify(current: float, avg: float, sd: float, direction: int) -> Tuple[str, float, float, float]:
    """
    Returns (status, z, tripwire_level, stress_level)

    z = (current - avg) / sd
    risk_z = direction * z, where direction=+1 means higher is worse; -1 means lower is worse
    tripwire at risk_z >= 1, stress at risk_z >= 2
    """
    if not sd or sd == 0 or math.isnan(sd):
        return "healthy", 0.0, float("nan"), float("nan")
    z = (current - avg) / sd
    sign = 1.0 if direction >= 0 else -1.0
    tripwire = avg + sign * 1.0 * sd
    stress = avg + sign * 2.0 * sd

    risk_z = direction * z
    if risk_z >= 2.0:
        return "stress", z, tripwire, stress
    if risk_z >= 1.0:
        return "tripwire", z, tripwire, stress
    return "healthy", z, tripwire, stress


def fmt_value(v: Optional[float], units: str) -> str:
    if v is None:
        return "—"
    if units == "pct":
        return f"{v:.2f}%".rstrip("0").rstrip(".") + "%"
    if units == "mil":
        return f"{v:.2f}M".rstrip("0").rstrip(".") + "M"
    if units == "thou":
        return f"{v:,.0f}K"
    if units == "usd_b":
        # billions
        if v >= 10000:
            return f"${v/1000:,.2f}T"
        return f"${v:,.0f}B"
    return str(v)


def fmt_delta_abs(delta: Optional[float], units: str) -> str:
    if delta is None:
        return "—"
    sign = "+" if delta > 0 else ""
    if units == "pct":
        return f"{sign}{delta:.2f} pp".replace(".00", "")
    if units == "mil":
        return f"{sign}{delta:.2f}M".replace(".00", "")
    if units == "thou":
        return f"{sign}{delta:,.0f}K"
    if units == "usd_b":
        return f"{sign}${delta:,.0f}B"
    return f"{sign}{delta}"


def fmt_delta_pct(delta_pct: Optional[float]) -> str:
    if delta_pct is None:
        return "—"
    sign = "+" if delta_pct > 0 else ""
    return f"{sign}{delta_pct:.1f}%".replace("+0.0%", "0.0%")


def overall_health(metrics_df: pd.DataFrame) -> str:
    s = int((metrics_df["status"] == "stress").sum())
    t = int((metrics_df["status"] == "tripwire").sum())
    if s >= 2:
        return "Stress"
    if t >= 2 or s >= 1:
        return "Tripwire / Watch"
    return "Healthy"


def build_exec_summary(overall: str, metrics_df: pd.DataFrame) -> Tuple[str, List[dict]]:
    """
    Produces an executive summary string + structured 'key_moves' list
    based on latest period-over-period changes (not "good vs bad", just movement).
    """
    # movers by standardized last-period change
    moves = []
    for _, r in metrics_df.iterrows():
        dv = r.get("delta_prev_abs")
        sd = r.get("sd_10y")
        if dv is None or (isinstance(dv, float) and math.isnan(dv)):
            continue
        score = abs(float(dv)) / (float(sd) if sd and not math.isnan(float(sd)) and float(sd) != 0 else 1.0)
        moves.append((score, r.to_dict()))
    moves.sort(key=lambda x: x[0], reverse=True)

    top = []
    for _, r in moves[:3]:
        top.append({
            "key": r["key"],
            "title": r["title"],
            "latest_date": r["latest_date"],
            "latest_value": r["latest_value"],
            "delta_prev_abs": r.get("delta_prev_abs"),
            "delta_prev_pct": r.get("delta_prev_pct"),
            "units": r["units"],
        })

    if top:
        parts = []
        for m in top:
            parts.append(
                f"{m['title']} moved {fmt_delta_abs(m['delta_prev_abs'], m['units'])} "
                f"since the prior release (as of {m['latest_date']})."
            )
        key_sentence = "Key moves since the last release: " + " ".join(parts)
    else:
        key_sentence = "Key moves since the last release: (insufficient history to compute short-term changes)."

    # short plain-language framing
    if "stress" in overall.lower():
        frame = "Multiple indicators are in the stress zone versus their own 10-year baselines."
    elif "tripwire" in overall.lower():
        frame = "Several indicators are in a watch zone versus their own 10-year baselines."
    else:
        frame = "Most indicators are near their 10-year baselines."

    summary = f"{frame} Overall health: {overall}. {key_sentence}"
    return summary, top


def fred_observations(series_id: str) -> pd.DataFrame:
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "1900-01-01",
    }
    # If no API key, FRED may still respond for some users, but it's not guaranteed.
    if not params["api_key"]:
        params.pop("api_key", None)

    r = SESSION.get(FRED_BASE, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    obs = payload.get("observations", [])
    rows = []
    for o in obs:
        ds = o.get("date")
        vs = o.get("value")
        if vs is None or vs == ".":
            continue
        try:
            v = float(vs)
        except Exception:
            continue
        rows.append({"date": ds, "value": v})

    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df


# -----------------------------
# News
# -----------------------------
def fetch_news() -> dict:
    """
    Pull a few RSS/Atom feeds into news.json.

    This intentionally avoids hard dependencies. If `feedparser` is installed it will be used;
    otherwise a small XML parser fallback is used.
    """
    out = {"generated_utc": utc_now_iso(), "sections": {}}

    def parse_with_feedparser(url: str) -> List[dict]:
        d = feedparser.parse(url)
        items = []
        src = (d.feed.get("title") or "").strip()
        for e in d.entries[:14]:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            if not title or not link:
                continue
            published = None
            if getattr(e, "published_parsed", None):
                published = dt.datetime.fromtimestamp(time.mktime(e.published_parsed), tz=dt.timezone.utc)
            elif getattr(e, "updated_parsed", None):
                published = dt.datetime.fromtimestamp(time.mktime(e.updated_parsed), tz=dt.timezone.utc)

            items.append({
                "title": title,
                "source": src,
                "link": link,
                "published_utc": published.isoformat().replace("+00:00", "Z") if published else None,
            })
        return items

    def parse_with_xml(url: str) -> List[dict]:
        # Minimal RSS/Atom support (best effort)
        import xml.etree.ElementTree as ET

        r = SESSION.get(url, timeout=25)
        r.raise_for_status()
        root = ET.fromstring(r.text)

        # detect RSS
        items = []
        if root.tag.lower().endswith("rss") or root.find("channel") is not None:
            channel = root.find("channel")
            src = (channel.findtext("title") or "").strip() if channel is not None else ""
            for it in (channel.findall("item") if channel is not None else [])[:14]:
                title = (it.findtext("title") or "").strip()
                link = (it.findtext("link") or "").strip()
                pub = (it.findtext("pubDate") or "").strip()
                published = None
                if pub:
                    # RFC822-ish; parse loosely
                    try:
                        published = dt.datetime.strptime(pub[:25], "%a, %d %b %Y %H:%M:%S").replace(tzinfo=dt.timezone.utc)
                    except Exception:
                        published = None
                if title and link:
                    items.append({"title": title, "source": src, "link": link,
                                  "published_utc": published.isoformat().replace("+00:00", "Z") if published else None})
            return items

        # Atom
        ns = {"a": "http://www.w3.org/2005/Atom"}
        src = (root.findtext("a:title", default="", namespaces=ns) or "").strip()
        for e in root.findall("a:entry", ns)[:14]:
            title = (e.findtext("a:title", default="", namespaces=ns) or "").strip()
            link = ""
            for l in e.findall("a:link", ns):
                if l.get("rel") in (None, "", "alternate"):
                    link = (l.get("href") or "").strip()
                    break
            upd = (e.findtext("a:updated", default="", namespaces=ns) or "").strip()
            published = None
            if upd:
                try:
                    published = dt.datetime.fromisoformat(upd.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
                except Exception:
                    published = None
            if title and link:
                items.append({"title": title, "source": src, "link": link,
                              "published_utc": published.isoformat().replace("+00:00", "Z") if published else None})
        return items

    def parse_feed(url: str) -> List[dict]:
        if feedparser is not None:
            try:
                return parse_with_feedparser(url)
            except Exception:
                pass
        try:
            return parse_with_xml(url)
        except Exception:
            return []

    for section, feeds in NEWS_FEEDS.items():
        agg: List[dict] = []
        for url in feeds:
            agg.extend(parse_feed(url))

        # De-dupe by link; keep newest first (if timestamps exist)
        seen = set()
        uniq = []
        agg.sort(key=lambda x: (x.get("published_utc") or ""), reverse=True)
        for it in agg:
            lk = it.get("link")
            if not lk or lk in seen:
                continue
            seen.add(lk)
            uniq.append(it)

        out["sections"][section] = uniq[:10]

    return out


# -----------------------------
# Main
# -----------------------------
def main():
    series_frames: Dict[str, pd.DataFrame] = {}
    source_notes: Dict[str, str] = {}

    for s in SERIES:
        try:
            df = fred_observations(s.series_id)
            if df.empty:
                series_frames[s.key] = df
                source_notes[s.key] = f"FRED series {s.series_id}: no observations returned."
                continue

            # scaling to display units
            df = df.copy()
            df["value"] = df["value"] * float(s.scale)

            # defensive normalization if a feed unexpectedly changes magnitude:
            # - REVOLSL should be ~1,000–2,500 in $B; if it's 1,000x bigger, divide by 1,000
            # - ICSA_thou should be ~150–500 (thousands); if it's 1,000x bigger, divide by 1,000
            vmax = float(df["value"].max())
            if s.units == "usd_b" and vmax > 10000:
                df["value"] = df["value"] / 1000.0
            if s.units == "thou" and vmax > 10000:
                df["value"] = df["value"] / 1000.0

            series_frames[s.key] = df
            source_notes[s.key] = f"FRED series {s.series_id} ({s.freq})."
        except Exception as e:
            series_frames[s.key] = pd.DataFrame(columns=["date", "value"])
            source_notes[s.key] = f"FRED series {s.series_id} fetch failed: {type(e).__name__}: {e}"

    # Wide timeseries for JSON + Excel
    # Use union of all dates; keep ISO date in "date" column
    all_dates = sorted(set(pd.concat([df["date"] for df in series_frames.values() if not df.empty]).tolist()))
    ts_df = pd.DataFrame({"date": all_dates})
    for s in SERIES:
        df = series_frames.get(s.key, pd.DataFrame(columns=["date", "value"]))
        if df.empty:
            ts_df[s.key] = pd.NA
            continue
        ts_df = ts_df.merge(df.rename(columns={"value": s.key}), on="date", how="left")

    # Metrics calculations
    rows = []
    for s in SERIES:
        df = series_frames.get(s.key, pd.DataFrame(columns=["date", "value"])).copy()
        if df.empty:
            rows.append({
                "key": s.key, "series_id": s.series_id, "title": s.title, "units": s.units, "freq": s.freq,
                "direction": s.direction, "latest_date": None, "latest_value": None,
                "avg_10y": None, "sd_10y": None,
                "delta_prev_abs": None, "delta_prev_pct": None,
                "delta_1y_abs": None, "delta_1y_pct": None,
                "z_score": None, "tripwire_level": None, "stress_level": None,
                "status": "healthy",
                "description": s.description,
                "how_to_read": None,
                "why_status": None,
                "chart_summary": None,
            })
            continue

        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")

        latest = last_non_null(df)
        if not latest:
            continue
        latest_date, latest_value = latest

        # Prior period (most recent previous observation)
        prevp = prev_non_null(df)
        prevp_value = prevp[1] if prevp else None
        delta_prev_abs = (latest_value - prevp_value) if prevp_value is not None else None
        delta_prev_pct = None
        if prevp_value is not None and prevp_value != 0:
            delta_prev_pct = (latest_value - prevp_value) / prevp_value * 100.0

        # YoY baseline
        yoy = yoy_by_observation(df, s.freq)
        if yoy is None:
            one_year_ago = latest_date - pd.DateOffset(years=1)
            yoy = value_at_or_before(df, one_year_ago)
        yoy_value = yoy[1] if yoy else None

        delta_1y_abs = (latest_value - yoy_value) if yoy_value is not None else None
        delta_1y_pct = None
        if yoy_value is not None and yoy_value != 0:
            delta_1y_pct = (latest_value - yoy_value) / yoy_value * 100.0

        ten_years_ago = latest_date - pd.DateOffset(years=10)
        vals10 = values_since(df, ten_years_ago)
        avg10 = mean(vals10) if vals10 else float("nan")
        sd10 = std(vals10) if vals10 else 0.0

        status, z, tripwire, stress = classify(latest_value, avg10, sd10, s.direction)
        z_score = z if (sd10 and sd10 != 0 and not math.isnan(sd10)) else None

        # Explanations
        direction_text = "Higher is generally worse." if s.direction >= 0 else "Lower is generally worse."
        how_to_read = (
            f"{s.description} {direction_text} "
            f"Health is based on how far the latest value is from its own 10-year average "
            f"(Tripwire ≈ 1σ, Stress ≈ 2σ)."
        )

        if sd10 and sd10 != 0 and not math.isnan(sd10):
            above_below = "above" if latest_value >= avg10 else "below"
            if s.direction >= 0:
                worse_text = "higher-than-normal"
            else:
                worse_text = "lower-than-normal"
            why_status = (
                f"Latest is {fmt_value(latest_value, s.units)} ({above_below} 10y avg {fmt_value(avg10, s.units)}). "
                f"That is {abs(z):.2f}σ from baseline; {worse_text} readings are treated as more risky for this metric."
            )
        else:
            why_status = "Insufficient 10-year variability to compute a z-score; defaulting status to Healthy."

        # Auto chart summary (short, state + recent movement)
        pieces = [
            f"Status: {status.title()}",
            f"Latest: {fmt_value(latest_value, s.units)} (as of {latest_date.strftime('%Y-%m-%d')})",
        ]
        if delta_prev_abs is not None:
            pieces.append(f"Since prior release: {fmt_delta_abs(delta_prev_abs, s.units)} ({fmt_delta_pct(delta_prev_pct)})")
        if delta_1y_abs is not None:
            # pct-series: abs is pp
            pieces.append(f"YoY: {fmt_delta_abs(delta_1y_abs, s.units)} ({fmt_delta_pct(delta_1y_pct)})")
        chart_summary = " • ".join(pieces)

        rows.append({
            "key": s.key,
            "series_id": s.series_id,
            "title": s.title,
            "units": s.units,
            "freq": s.freq,
            "direction": s.direction,
            "latest_date": latest_date.strftime("%Y-%m-%d"),
            "latest_value": safe_float(latest_value),
            "avg_10y": safe_float(avg10),
            "sd_10y": safe_float(sd10),
            "delta_prev_abs": safe_float(delta_prev_abs),
            "delta_prev_pct": safe_float(delta_prev_pct),
            "delta_1y_abs": safe_float(delta_1y_abs),
            "delta_1y_pct": safe_float(delta_1y_pct),
            "z_score": safe_float(z_score),
            "tripwire_level": safe_float(tripwire),
            "stress_level": safe_float(stress),
            "status": status,
            "description": s.description,
            "how_to_read": how_to_read,
            "why_status": why_status,
            "chart_summary": chart_summary,
        })

    metrics_df = pd.DataFrame(rows)

    overall = overall_health(metrics_df)
    exec_summary, key_moves = build_exec_summary(overall, metrics_df)

    # JSON timeseries records
    ts_df_out = ts_df.copy()
    ts_df_out["date"] = ts_df_out["date"].dt.strftime("%Y-%m-%d")
    data_records = ts_df_out.to_dict(orient="records")

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "overall_health": overall,
            "executive_summary": exec_summary,
            "key_moves": key_moves,
            "source_notes": source_notes,
        },
        "metrics": sanitize_records(metrics_df.to_dict(orient="records")),
        "data": data_records,
    }

    with open(OUT_DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, allow_nan=False)

    news = fetch_news()
    with open(OUT_NEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False, indent=2, allow_nan=False)

    with pd.ExcelWriter(OUT_XLSX_METRICS, engine="openpyxl") as w:
        metrics_df.to_excel(w, index=False, sheet_name="metrics")

    with pd.ExcelWriter(OUT_XLSX_TS, engine="openpyxl") as w:
        metrics_df.to_excel(w, index=False, sheet_name="metrics")
        ts_df_out.to_excel(w, index=False, sheet_name="timeseries")


if __name__ == "__main__":
    main()
