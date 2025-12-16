#!/usr/bin/env python3
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


FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
USER_AGENT = "Macro-Credit-Dashboard/1.0 (GitHub Actions)"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})

OUT_DATA_JSON = "data.json"
OUT_NEWS_JSON = "news.json"
OUT_XLSX_METRICS = "macro_credit_metrics.xlsx"
OUT_XLSX_TS = "macro_credit_timeseries.xlsx"


@dataclass
class SeriesDef:
    series_id: str
    key: str
    title: str
    units: str        # "pct", "mil", "usd_b", "thou"
    freq: str
    direction: int    # +1 higher=worse, -1 lower=worse (used only for status)
    description: str
    scale: float = 1.0  # applied to raw FRED values before storing (e.g., ICSA / 1000)


SERIES: List[SeriesDef] = [
    SeriesDef("DRCCLACBS", "DRCCLACBS_pct", "Card 30+ Delinquency", "pct", "quarterly", +1,
              "Share of credit card balances 30+ days past due (all banks).", 1.0),
    SeriesDef("CORCCACBS", "CORCCACBS_pct", "Net Charge-off Rate", "pct", "quarterly", +1,
              "Share of card balances charged off as uncollectible (all banks).", 1.0),
    SeriesDef("REVOLSL", "REVOLSL_bil_usd", "Revolving Consumer Credit", "usd_b", "monthly", +1,
              "Outstanding revolving consumer credit ($ billions).", 1.0),
    SeriesDef("TDSP", "TDSP_pct", "Debt Service Burden", "pct", "quarterly", +1,
              "Household debt service payments as a share of disposable income.", 1.0),
    SeriesDef("JTSJOL", "JTSJOL_mil", "Job Openings", "mil", "monthly", -1,
              "Total nonfarm job openings (labor demand proxy; lower is worse).", 1.0),
    SeriesDef("UNRATE", "UNRATE_pct", "Unemployment Rate", "pct", "monthly", +1,
              "Unemployment rate (U-3).", 1.0),
    # FRED ICSA is a raw count; dashboard wants "thousands"
    SeriesDef("ICSA", "ICSA_thou", "Initial Jobless Claims", "thou", "weekly", +1,
              "Weekly initial unemployment insurance claims (higher can signal stress).", 1.0 / 1000.0),
    SeriesDef("DRSFRMACBS", "DRSFRMACBS_pct", "Mortgage 30+ Delinquency", "pct", "quarterly", +1,
              "Share of residential mortgage balances 30+ days past due (all banks).", 1.0),
]


NEWS_FEEDS = {
    "Credit / consumer stress": [
        "https://finance.yahoo.com/rss/topstories",
        "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    ],
    "Labor / macro signals": [
        "https://www.cnbc.com/id/10001147/device/rss/rss.html",
        "https://finance.yahoo.com/rss/economy",
    ],
}


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def http_get(url: str, params: Optional[dict] = None, timeout: int = 30) -> requests.Response:
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r


def fred_observations(series_id: str) -> pd.DataFrame:
    if not FRED_API_KEY:
        raise RuntimeError("FRED_API_KEY is missing. Add it as a GitHub Actions secret.")

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": "2000-01-01",
    }
    data = http_get(url, params=params).json()
    obs = data.get("observations", [])
    if not obs:
        return pd.DataFrame(columns=["date", "value"])

    rows = []
    for o in obs:
        d = o.get("date")
        v = o.get("value")
        if v in (".", None, ""):
            val = math.nan
        else:
            try:
                val = float(v)
            except Exception:
                val = math.nan
        rows.append((d, val))

    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def safe_float(x) -> Optional[float]:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def sanitize_records(records: List[dict]) -> List[dict]:
    clean = []
    for r in records:
        rr = {}
        for k, v in r.items():
            if isinstance(v, float):
                rr[k] = safe_float(v)
            else:
                rr[k] = v
        clean.append(rr)
    return clean


def mean(vals: List[float]) -> float:
    return sum(vals) / len(vals) if vals else float("nan")


def std(vals: List[float]) -> float:
    if len(vals) < 2:
        return 0.0
    m = mean(vals)
    return math.sqrt(sum((x - m) ** 2 for x in vals) / len(vals))


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


def values_since(df: pd.DataFrame, start_date: pd.Timestamp) -> List[float]:
    df2 = df.dropna(subset=["value"])
    df2 = df2[df2["date"] >= start_date]
    return [float(x) for x in df2["value"].tolist()]


def classify(current: float, avg10: float, sd10: float, direction: int) -> str:
    if sd10 <= 0 or any(map(lambda z: z is None or math.isnan(z), [current, avg10, sd10])):
        return "healthy"
    z = (current - avg10) / sd10
    risk = direction * z
    if risk >= 2.0:
        return "stress"
    if risk >= 1.0:
        return "tripwire"
    return "healthy"


def fetch_news() -> dict:
    out = {"generated_utc": utc_now_iso(), "sections": {}}

    if feedparser is None:
        for section in NEWS_FEEDS:
            out["sections"][section] = []
        return out

    def parse_feed(url: str) -> List[dict]:
        d = feedparser.parse(url)
        items = []
        for e in d.entries[:10]:
            title = (e.get("title") or "").strip()
            link = (e.get("link") or "").strip()
            source = (d.feed.get("title") or "").strip()
            published = None
            if "published_parsed" in e and e.published_parsed:
                published = dt.datetime.fromtimestamp(time.mktime(e.published_parsed), tz=dt.timezone.utc)
            elif "updated_parsed" in e and e.updated_parsed:
                published = dt.datetime.fromtimestamp(time.mktime(e.updated_parsed), tz=dt.timezone.utc)

            items.append({
                "title": title,
                "source": source,
                "link": link,
                "published_utc": published.isoformat().replace("+00:00", "Z") if published else None
            })
        return items

    for section, feeds in NEWS_FEEDS.items():
        agg = []
        for url in feeds:
            try:
                agg.extend(parse_feed(url))
            except Exception:
                continue
        agg.sort(key=lambda x: x["published_utc"] or "", reverse=True)
        out["sections"][section] = agg[:10]

    return out


def build_timeseries_frame(series_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    base = None
    for s in SERIES:
        df = series_frames.get(s.key)
        if df is None or df.empty:
            continue
        dfx = df.copy()
        dfx["date"] = pd.to_datetime(dfx["date"])
        dfx = dfx[["date", "value"]].rename(columns={"value": s.key})
        base = dfx if base is None else base.merge(dfx, on="date", how="outer")

    if base is None:
        return pd.DataFrame(columns=["date"] + [s.key for s in SERIES])

    base = base.sort_values("date").reset_index(drop=True)
    base["date"] = base["date"].dt.strftime("%Y-%m-%d")
    return base


def build_metrics_table(series_frames: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows = []
    for s in SERIES:
        df = series_frames.get(s.key, pd.DataFrame(columns=["date", "value"])).copy()
        if df.empty:
            rows.append({
                "key": s.key, "series_id": s.series_id, "title": s.title, "units": s.units, "freq": s.freq,
                "direction": s.direction, "latest_date": None, "latest_value": None,
                "avg_10y": None, "sd_10y": None, "delta_1y_abs": None, "delta_1y_pct": None,
                "status": "healthy", "description": s.description
            })
            continue

        df["date"] = pd.to_datetime(df["date"])
        latest = last_non_null(df)
        if not latest:
            rows.append({
                "key": s.key, "series_id": s.series_id, "title": s.title, "units": s.units, "freq": s.freq,
                "direction": s.direction, "latest_date": None, "latest_value": None,
                "avg_10y": None, "sd_10y": None, "delta_1y_abs": None, "delta_1y_pct": None,
                "status": "healthy", "description": s.description
            })
            continue

        latest_date, latest_value = latest

        one_year_ago = latest_date - pd.DateOffset(years=1)
        prev = value_at_or_before(df, one_year_ago)
        prev_value = prev[1] if prev else None

        ten_years_ago = latest_date - pd.DateOffset(years=10)
        vals10 = values_since(df, ten_years_ago)
        avg10 = mean(vals10) if vals10 else float("nan")
        sd10 = std(vals10) if vals10 else 0.0

        status = classify(latest_value, avg10, sd10, s.direction)

        # IMPORTANT: always true direction
        delta_abs = (latest_value - prev_value) if prev_value is not None else None
        delta_pct = None
        if prev_value is not None and prev_value != 0:
            delta_pct = (latest_value - prev_value) / prev_value * 100.0

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
            "delta_1y_abs": safe_float(delta_abs),
            "delta_1y_pct": safe_float(delta_pct),
            "status": status,
            "description": s.description
        })

    return pd.DataFrame(rows)


def compute_overall_health(metrics_df: pd.DataFrame) -> str:
    s = (metrics_df["status"] == "stress").sum()
    t = (metrics_df["status"] == "tripwire").sum()
    if s >= 2:
        return "Stress"
    if t >= 2 or s >= 1:
        return "Tripwire / Watch"
    return "Healthy"


def main():
    series_frames: Dict[str, pd.DataFrame] = {}
    source_notes = {}

    for s in SERIES:
        try:
            df = fred_observations(s.series_id)
            # Apply scaling for normalized display units
            if not df.empty:
                df["value"] = df["value"] * s.scale
            series_frames[s.key] = df
            source_notes[s.key] = f"FRED series {s.series_id} ({s.freq}, {s.units}) scale={s.scale}"
        except Exception as e:
            series_frames[s.key] = pd.DataFrame(columns=["date", "value"])
            source_notes[s.key] = f"ERROR fetching {s.series_id}: {type(e).__name__}"

    ts_df = build_timeseries_frame(series_frames)
    metrics_df = build_metrics_table(series_frames)
    overall = compute_overall_health(metrics_df)

    data_records = sanitize_records(ts_df.to_dict(orient="records"))

    payload = {
        "meta": {
            "last_updated_utc": utc_now_iso(),
            "overall_health": overall,
            "source_notes": source_notes,
        },
        "metrics": sanitize_records(metrics_df.to_dict(orient="records")),
        "data": data_records
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
        ts_df.to_excel(w, index=False, sheet_name="timeseries")

    print("Wrote:", OUT_DATA_JSON, OUT_NEWS_JSON, OUT_XLSX_METRICS, OUT_XLSX_TS)


if __name__ == "__main__":
    main()
