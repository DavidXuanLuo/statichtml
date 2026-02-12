#!/usr/bin/env python3
import json
import urllib.request
import urllib.parse
import time
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
JSON_PATH = DATA_DIR / "prediction-markets-today.json"
HTML_PATH = BASE_DIR / "prediction-markets-today.html"

TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
UA = {"User-Agent": "Mozilla/5.0 (OpenClaw prediction-markets-today bot)"}


def http_get_json(url: str, retries: int = 3):
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={**UA, "Accept-Encoding": "identity"})
            with urllib.request.urlopen(req, timeout=45) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
            if i < retries - 1:
                time.sleep(1.2 * (i + 1))
    raise last_err


def parse_iso_utc(s: str) -> datetime:
    # 兼容少于/多于6位小数秒
    s = s.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    if "." in s:
        head, tail = s.split(".", 1)
        tz_pos = max(tail.find("+"), tail.find("-"))
        if tz_pos != -1:
            frac = tail[:tz_pos]
            tz = tail[tz_pos:]
            frac = (frac + "000000")[:6]
            s = f"{head}.{frac}{tz}"
    return datetime.fromisoformat(s)


def today_window():
    now_local = datetime.now(TZ)
    start_local = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=TZ)
    end_local = start_local + timedelta(days=1)
    return now_local, start_local, end_local, start_local.astimezone(UTC), end_local.astimezone(UTC)


def count_polymarket(start_utc: datetime, end_utc: datetime):
    count = 0
    offset = 0
    limit = 200
    pages = 0
    max_pages = 30
    partial = False
    while True:
        params = urllib.parse.urlencode({
            "limit": limit,
            "offset": offset,
            "order": "createdAt",
            "ascending": "false",
            "start_date_min": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        })
        url = f"https://gamma-api.polymarket.com/markets?{params}"
        data = http_get_json(url)
        if not data:
            break

        pages += 1
        for m in data:
            created = m.get("createdAt")
            if not created:
                continue
            dt = parse_iso_utc(created)
            if start_utc <= dt < end_utc:
                count += 1

        if len(data) < limit:
            break
        if pages >= max_pages:
            partial = True
            break
        offset += limit

    return {
        "count": count,
        "status": "partial" if partial else "ok",
        "note": f"分页抓取 {pages} 页，按 createdAt 统计" + ("（达到页数上限，可能低估）" if partial else ""),
    }


def count_manifold(start_utc: datetime, end_utc: datetime):
    # Manifold /v0/markets 当前实测缺少可靠分页参数，取最新1000条统计“今日新增”。
    url = "https://api.manifold.markets/v0/markets?limit=1000"
    data = http_get_json(url)
    count = 0
    for m in data:
        created_ms = m.get("createdTime")
        if created_ms is None:
            continue
        dt = datetime.fromtimestamp(created_ms / 1000, tz=UTC)
        if start_utc <= dt < end_utc:
            count += 1
    return {
        "count": count,
        "status": "ok",
        "note": "基于最新1000条 markets 统计（接口分页能力受限）",
    }


def count_kalshi(start_utc: datetime, end_utc: datetime):
    count = 0
    cursor = None
    pages = 0
    max_pages = 30
    partial = False

    while True:
        q = {"limit": 200}
        if cursor:
            q["cursor"] = cursor
        url = "https://api.elections.kalshi.com/trade-api/v2/markets?" + urllib.parse.urlencode(q)
        payload = http_get_json(url)
        markets = payload.get("markets", [])
        if not markets:
            break

        pages += 1
        stop = False
        for m in markets:
            created = m.get("created_time")
            if not created:
                continue
            dt = parse_iso_utc(created)
            if dt < start_utc:
                stop = True
                break
            if start_utc <= dt < end_utc:
                count += 1

        if stop:
            break
        if pages >= max_pages:
            partial = True
            break
        cursor = payload.get("cursor")
        if not cursor:
            break

    return {
        "count": count,
        "status": "partial" if partial else "ok",
        "note": f"分页抓取 {pages} 页，按 created_time 统计" + ("（达到页数上限，可能低估）" if partial else ""),
    }


def render_html(report):
    rows = ""
    for name in ["Polymarket", "Manifold", "Kalshi"]:
        d = report["platforms"][name]
        c = d.get("count")
        status = d.get("status", "missing")
        note = d.get("note", "")
        c_display = "缺失" if c is None else str(c)
        rows += f"<tr><td>{name}</td><td>{c_display}</td><td>{status}</td><td>{note}</td></tr>"

    generated = report["generated_at"]
    date_cn = report["date_shanghai"]
    complete = report["completeness"]
    total = report["totals"].get("known_sum")

    return f"""<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>今日预测市场合约数 - {date_cn}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 16px; color: #111; }}
    h1 {{ margin-bottom: 8px; }}
    .meta {{ color: #555; margin-bottom: 16px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
    th {{ background: #f5f5f5; }}
    .ok {{ color: #0a7; }}
    .warn {{ color: #b60; }}
    code {{ background:#f3f3f3; padding:2px 6px; border-radius:4px; }}
  </style>
</head>
<body>
  <h1>今日预测市场合约数（Asia/Shanghai）</h1>
  <div class=\"meta\">日期：<b>{date_cn}</b>｜生成时间：<code>{generated}</code></div>
  <p>已统计平台：Polymarket / Manifold / Kalshi。已知总和：<b>{total}</b>，数据完整性：<b>{complete}</b>。</p>
  <table>
    <thead><tr><th>平台</th><th>今日新增合约/市场数</th><th>状态</th><th>说明</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
"""


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now_local, start_local, end_local, start_utc, end_utc = today_window()

    platforms = {}

    for name, fn in [
        ("Polymarket", count_polymarket),
        ("Manifold", count_manifold),
        ("Kalshi", count_kalshi),
    ]:
        try:
            platforms[name] = fn(start_utc, end_utc)
        except Exception as e:
            platforms[name] = {
                "count": None,
                "status": "missing",
                "note": f"抓取失败: {e}",
            }

    known_sum = sum(v["count"] for v in platforms.values() if isinstance(v.get("count"), int))
    available_count = sum(1 for v in platforms.values() if v.get("status") in ("ok", "partial"))
    completeness = f"{available_count}/3"

    report = {
        "metric": "today_new_prediction_markets",
        "timezone": "Asia/Shanghai",
        "date_shanghai": start_local.strftime("%Y-%m-%d"),
        "window": {
            "start_local": start_local.isoformat(),
            "end_local": end_local.isoformat(),
            "start_utc": start_utc.isoformat(),
            "end_utc": end_utc.isoformat(),
        },
        "generated_at": now_local.isoformat(),
        "platforms": platforms,
        "totals": {
            "known_sum": known_sum,
        },
        "completeness": completeness,
    }

    JSON_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(render_html(report), encoding="utf-8")

    print(f"Wrote: {JSON_PATH}")
    print(f"Wrote: {HTML_PATH}")
    print(json.dumps(report["platforms"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
