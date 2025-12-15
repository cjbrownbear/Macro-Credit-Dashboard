# --- NEWS FEED (RSS -> news.json) ---
import json
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

RSS_FEEDS = [
    # RSS feeds avoid CORS and work great with GitHub Pages
    ("CNBC Top News", "https://www.cnbc.com/id/100003114/device/rss/rss.html"),
    ("CNBC Economy", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
    ("Yahoo Finance", "https://finance.yahoo.com/news/rss"),
]

def _fetch_rss_items(url: str, limit: int = 8):
    resp = requests.get(url, timeout=25, headers={"User-Agent": "Mozilla/5.0"})
    resp.raise_for_status()
    root = ET.fromstring(resp.text)

    out = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or "").strip()
        if title and link:
            out.append({"title": title, "link": link, "date": pub})
    return out

def build_news_json(out_path: str = "news.json"):
    payload = {
        "meta": {"generated_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z"},
        "items": []
    }

    for source, url in RSS_FEEDS:
        try:
            for item in _fetch_rss_items(url, limit=6):
                item["source"] = source
                payload["items"].append(item)
        except Exception:
            payload["items"].append({
                "title": f"(Failed to load feed: {source})",
                "link": url,
                "date": "",
                "source": source
            })

    # keep feed compact and “tile-sized”
    payload["items"] = payload["items"][:12]

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

# Call it
build_news_json("news.json")
