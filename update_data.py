#!/usr/bin/env python3
import json
import datetime as dt
from pathlib import Path

import pandas as pd
import requests
from io import StringIO

FRED_CSV = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"

ROOT = Path(__file__).resolve().parent

def fetch_fred(series_id: str) -> pd.DataFrame:
    url = FRED_CSV.format(series=series_id)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = ["date","value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna().sort_values("date")

def main():
    drcc = fetch_fred("DRCCLACBS").rename(columns={"value":"DRCCLACBS_pct"})
    corc = fetch_fred("CORCCACBS").rename(columns={"value":"CORCCACBS_pct"})
    jts  = fetch_fred("JTSJOL").rename(columns={"value":"JTSJOL_raw"})
    rev  = fetch_fred("REVOLSL").rename(columns={"value":"REVOLSL_raw"})

    # Unit conversions
    jts["JTSJOL_mil"] = jts["JTSJOL_raw"] / 1000.0     # thousands -> millions
    rev["REVOLSL_bil_usd"] = rev["REVOLSL_raw"] / 1000.0  # millions -> billions

    all_dates = pd.DataFrame({
        "date": pd.concat([drcc["date"], corc["date"], jts["date"], rev["date"]]).drop_duplicates().sort_values()
    })

    ts = (all_dates
          .merge(drcc, on="date", how="left")
          .merge(corc, on="date", how="left")
          .merge(jts[["date","JTSJOL_mil"]], on="date", how="left")
          .merge(rev[["date","REVOLSL_bil_usd"]], on="date", how="left"))

    # Keep last 12 years
    cutoff = ts["date"].max() - pd.DateOffset(years=12)
    ts = ts[ts["date"] >= cutoff].reset_index(drop=True)

    last_updated = dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    data = []
    for _, r in ts.iterrows():
        data.append({
            "date": r["date"].strftime("%Y-%m-%d"),
            "DRCCLACBS_pct": None if pd.isna(r["DRCCLACBS_pct"]) else float(r["DRCCLACBS_pct"]),
            "CORCCACBS_pct": None if pd.isna(r["CORCCACBS_pct"]) else float(r["CORCCACBS_pct"]),
            "JTSJOL_mil": None if pd.isna(r["JTSJOL_mil"]) else float(r["JTSJOL_mil"]),
            "REVOLSL_bil_usd": None if pd.isna(r["REVOLSL_bil_usd"]) else float(r["REVOLSL_bil_usd"]),
        })

    payload = {
        "meta": {
            "last_updated_utc": last_updated,
            "source_notes": {
                "DRCCLACBS_pct": "FRED DRCCLACBS (quarterly, %)",
                "CORCCACBS_pct": "FRED CORCCACBS (quarterly, %)",
                "JTSJOL_mil": "FRED JTSJOL (monthly, millions; converted from thousands)",
                "REVOLSL_bil_usd": "FRED REVOLSL (monthly, $ billions; converted from millions)"
            }
        },
        "data": data
    }

    (ROOT / "data.json").write_text(json.dumps(payload, separators=(",",":"), ensure_ascii=False), encoding="utf-8")

    # Export timeseries workbook (for download)
    out_xlsx = ROOT / "macro_credit_timeseries.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        ts.to_excel(w, index=False, sheet_name="timeseries")

if __name__ == "__main__":
    main()
