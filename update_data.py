#!/usr/bin/env python3
import csv
import io
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional

import pandas as pd
import requests
import feedparser

OUTPUT_DATA_JSON = "data.json"
OUTPUT_NEWS_JSON = "news.json"
OUTPUT_METRICS_XLSX = "macro_credit_metrics.xlsx"
OUTPUT_TIMESERIES_XLSX = "macro_credit_timeseries.xlsx"

HEADERS = {"User-Agent": "MacroCreditDashboard/1.0 (GitHub Actions)"}
TIMEOUT = 30

@dataclass(frozen=True)
class SeriesDef:
    series_id: str
    out_key: str
    freq: str        # "weekly" | "monthly" | "quarterly"
    note: str

SERIES: List[SeriesDef] = [
    # Credit / household
    SeriesDef("DRCCLACBS",   "DRCCLACBS_pct",   "quarterly", "FRED DRCCLACBS: Delinquency Rate on Credit Card Loans, All Commercial Banks (%)."),
    SeriesDef("CORCCACBS",   "CORCCACBS_pct",   "quarterly", "FRED CORCCACBS: Charge-Off Rate on Credit Card Loans, All Commercial Banks (%)."),
    SeriesDef("REVOLSL",     "REVOLSL_bil_usd", "monthly",   "FRED REVOLSL: Revolving consumer credit outstanding ($ billions)."),
    SeriesDef("TDSP",        "TDSP_pct",        "quarterly", "FRED TDSP: Household Debt Service Payments as a Percent of Disposable Personal Income (%)."),

    # Mortgage delinquency (trend chart only, still in data.json)
    SeriesDef("DRSFRMACBS",  "DRSFRMACBS_pct",  "quarterly", "FRED DRSFRMACBS: Delinquency Rate on Single-Family Residential Mortgages, All Commercial Banks (%)."),

    # Labor / income
    SeriesDef("JTSJOL",      "JTSJOL_mil",      "monthly",   "FRED JTSJOL: Job Openings (Total nonfarm, millions)."),
    SeriesDef("UNRATE",      "UNRATE_pct",      "monthly",   "FRED UNRATE: Unemployment Rate (%)."),
    SeriesDef("ICSA",        "ICSA_k",          "weekly",    "FRED ICSA: Initial Claims (weekly level)."),
    SeriesDef("DSPIC96",     "DSPIC96_bil_usd", "monthly",   "FRED DSPIC96: Real Disposable Personal Income (chained dollars; level)."),
]

# RSS feeds (server-side, so no CORS issues)
NEWS_SOURCES = [
    # credit / consumer stress
    ("credit", "CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("credit", "Yahoo Finance", "https://finance.yahoo.com/rss/"),
    # labor / macro
    ("labor",  "CNBC Economy",  "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("labor",  "Reuters Business (via RSSHub fallback not used)", ""),  # placeholder (kept simple)
]

def http_get(url: str) -> requests.Response:
    r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
    r.raise_for_status()
    return r

def fetch_fred_series(series_id: str) -> pd.DataFrame:
    """
    Fetch using FRED's fredgraph CSV endpoint.
    """
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    r = http_get(url)
    text = r.text.strip().splitlines()

    # Parse CSV manually to handle edge cases
    reader = csv.DictReader(io.StringIO("\n".join(text)))
    if not reader.fieldnames or "DATE" not in reader.fieldnames:
        raise ValueError(f"Unexpected FRED CSV format for {series_id}: missing DATE")

    rows = []
    for row in reader:
        d = row.get("DATE")
        v = row.get(series_id)
        if not d:
            continue
        # '.' is missing in fredgraph CSV
        if v is None or v.strip() == "." or v.strip() == "":
            rows.append((d, None))
        else:
            try:
                rows.append((d, float(v)))
            except ValueError:
                rows.append((d, None))

    df = pd.DataFrame(rows, columns=["date", "value"])
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    return df

def clean_for_json(obj):
    """
    Recursively replace NaN/inf with None to keep JSON strict.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    return obj

def compute_metrics(raw_rows: List[Dict], defs: List[SeriesDef]) -> pd.DataFrame:
    """
    Compute snapshot metrics per series: latest, as_of, 10y avg/sd, 1y delta abs + pct.
    """
    df = pd.DataFrame(raw_rows)
    df["date"] = pd.to_datetime(df["date"])

    out = []
    for s in defs:
        if s.out_key not in df.columns:
            continue

        sub = df[["date", s.out_key]].dropna()
        if sub.empty:
            continue

        latest_row = sub.iloc[-1]
        latest_date = latest_row["date"]
        latest_val = float(latest_row[s.out_key])

        # 10y window
        start_10y = latest_date - pd.DateOffset(years=10)
        win10 = sub[sub["date"] >= start_10y][s.out_key].astype(float)
        avg10 = float(win10.mean()) if len(win10) else latest_val
        sd10  = float(win10.std(ddof=0)) if len(win10) else 0.0

        # 1y prior: nearest <= latest_date - 1y
        target = latest_date - pd.DateOffset(years=1)
        prior_sub = sub[sub["date"] <= target]
        prior_val = float(prior_sub.iloc[-1][s.out_key]) if len(prior_sub) else None

        delta_abs = (latest_val - prior_val) if prior_val is not None else None
        delta_pct = ((latest_val - prior_val) / abs(prior_val) * 100.0) if (prior_val not in (None, 0.0)) else None

        out.append({
            "key": s.out_key,
            "series_id": s.series_id,
            "freq": s.freq,
            "latest_value": latest_val,
            "as_of": latest_date.strftime("%Y-%m-%d"),
            "avg_10y": avg10,
            "sd_10y": sd10,
            "delta_1y_abs": delta_abs,
            "delta_1y_pct": delta_pct,
            "note": s.note,
        })

    return pd.DataFrame(out)

def fetch_news() -> Dict:
    items = []
    now = datetime.now(timezone.utc)
    for cat, source, url in NEWS_SOURCES:
        if not url:
            continue
        try:
            fp = feedparser.parse(url)
            for e in fp.entries[:10]:
                title = getattr(e, "title", "").strip()
                link = getattr(e, "link", "").strip()
                published = getattr(e, "published", "") or getattr(e, "updated", "")
                published = published.strip()
                if not title or not link:
                    continue

                # Normalize published string (keep simple)
                items.append({
                    "category": cat,
                    "source": source,
                    "title": title,
                    "url": link,
                    "published": published if published else now.strftime("%a, %d %b %Y %H:%M:%S GMT"),
                })
        except Exception as ex:
            print(f"[WARN] news fetch failed for {source}: {ex}")

    # Simple de-dupe by URL
    seen = set()
    deduped = []
    for it in items:
        if it["url"] in seen:
            continue
        seen.add(it["url"])
        deduped.append(it)

    return {
        "meta": {
            "last_updated_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sources": [s for s in NEWS_SOURCES if s[2]],
        },
        "items": deduped
    }

def to_timeseries_rows(merged: pd.DataFrame) -> List[Dict]:
    merged = merged.copy()
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    rows = merged.to_dict(orient="records")
    # convert pandas NaN to None
    cleaned = []
    for r in rows:
        cleaned.append({k: (None if (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) else v) for k, v in r.items()})
    return cleaned

def main():
    now = datetime.now(timezone.utc)

    series_frames = []
    source_notes = {}

    # Fetch series with soft-fail (one bad ID doesn't kill the run)
    for s in SERIES:
        try:
            df = fetch_fred_series(s.series_id)
            df = df.rename(columns={"value": s.out_key})
            series_frames.append(df)
            source_notes[s.out_key] = s.note
            print(f"[OK] {s.series_id} -> {s.out_key} ({len(df)} rows)")
        except requests.HTTPError as he:
            print(f"[WARN] {s.series_id} HTTP error: {he}")
        except Exception as e:
            print(f"[WARN] {s.series_id} error: {e}")

    if not series_frames:
        raise RuntimeError("No FRED series could be fetched. Check connectivity and series IDs.")

    # Merge on date (outer)
    merged = series_frames[0][["date", series_frames[0].columns[1]]].copy()
    for df in series_frames[1:]:
        merged = pd.merge(merged, df, on="date", how="outer")

    merged = merged.sort_values("date").reset_index(drop=True)

    # Additional transforms:
    # ICSA is weekly "level" -> convert to thousands for nicer display
    if "ICSA_k" in merged.columns:
        merged["ICSA_k"] = merged["ICSA_k"].apply(lambda x: (x/1000.0) if (x is not None and not (isinstance(x,float) and math.isnan(x))) else x)

    # DSPIC96 is already a level series; keep in billions by dividing by 1e9 if it looks too large
    # FRED DSPIC96 is often in billions already depending on series; we normalize heuristically.
    if "DSPIC96_bil_usd" in merged.columns:
        # If median is > 100000, assume it's in millions or dollars and scale down.
        med = pd.to_numeric(merged["DSPIC96_bil_usd"], errors="coerce").median()
        if pd.notna(med) and med > 100000:
            merged["DSPIC96_bil_usd"] = merged["DSPIC96_bil_usd"] / 1000.0

    # REVOLSL should be in $ billions already; do NOT rescale
    # TDSP/Delinq/Chargeoff/Mortgage delinq are percents; do NOT rescale
    # JTSJOL is millions already; do NOT rescale
    # UNRATE percent already; do NOT rescale

    raw_rows = to_timeseries_rows(merged)

    payload = {
        "meta": {
            "last_updated_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source_notes": source_notes
        },
        "data": raw_rows
    }

    payload = clean_for_json(payload)
    with open(OUTPUT_DATA_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    # Metrics workbook
    metrics_df = compute_metrics(raw_rows, SERIES)
    with pd.ExcelWriter(OUTPUT_METRICS_XLSX, engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)

    # Timeseries workbook (metrics + timeseries)
    ts_df = pd.DataFrame(raw_rows)
    with pd.ExcelWriter(OUTPUT_TIMESERIES_XLSX, engine="openpyxl") as writer:
        metrics_df.to_excel(writer, sheet_name="metrics", index=False)
        ts_df.to_excel(writer, sheet_name="timeseries", index=False)

    # News
    news = fetch_news()
    news = clean_for_json(news)
    with open(OUTPUT_NEWS_JSON, "w", encoding="utf-8") as f:
        json.dump(news, f, ensure_ascii=False)

    print("[DONE] wrote:", OUTPUT_DATA_JSON, OUTPUT_NEWS_JSON, OUTPUT_METRICS_XLSX, OUTPUT_TIMESERIES_XLSX)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[FATAL]", e)
        sys.exit(1)
