#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
JSON_PATH = DATA_DIR / "prediction-markets-today.json"
HTML_PATH = BASE_DIR / "prediction-markets-today.html"
TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
UA = {"User-Agent": "Mozilla/5.0 (OpenClaw prediction-markets-today bot)", "Accept-Encoding": "identity"}


def http_get_json(url: str, retries: int = 3):
    last_err = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        except Exception as e:
            last_err = e
    raise last_err


def parse_iso_utc(s: str) -> datetime:
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
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=TZ)
    end_local = start_local + timedelta(days=1)
    return now_local, start_local, end_local, start_local.astimezone(UTC), end_local.astimezone(UTC)


def polymarket(start_utc: datetime, end_utc: datetime):
    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": None,
            "unit": "missing",
            "source_metric": "unavailable",
        },
        "derived": {"traded_contract_entries": None, "traded_markets": None},
        "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
        "status": "missing",
        "note": "Polymarket公开接口当前在本环境下无法稳定完成大结果集拉取（频繁IncompleteRead），暂无法可靠产出当日成交合约总量，已标记缺失。",
    }


def manifold(start_utc: datetime, end_utc: datetime):
    # 主指标：sum(abs(shares)) from latest 10000 bets（近似/partial）
    b = http_get_json("https://api.manifold.markets/v0/bets?limit=3000")
    vol = 0.0
    cset = set()
    for x in b:
        t = int(x.get("createdTime") or 0)
        dt = datetime.fromtimestamp(t / 1000, tz=UTC)
        if start_utc <= dt < end_utc:
            vol += abs(float(x.get("shares") or 0))
            cid = x.get("contractId")
            if cid:
                cset.add(cid)

    m = http_get_json("https://api.manifold.markets/v0/markets?limit=1000")
    mkt = 0
    ctr = 0
    for x in m:
        t = int(x.get("createdTime") or 0)
        dt = datetime.fromtimestamp(t / 1000, tz=UTC)
        if start_utc <= dt < end_utc:
            mkt += 1
            ctr += 2 if (x.get("outcomeType") or "").upper() == "BINARY" else 1

    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": round(vol, 2),
            "unit": "shares(latest 3k bets)",
            "source_metric": "sum(abs(bets.shares))",
        },
        "derived": {"traded_contract_entries": len(cset)},
        "auxiliary": {"new_market_count": mkt, "new_contract_listing_count": ctr},
        "status": "partial",
        "note": "Manifold使用公开/v0/bets近似；当前仅基于最新3000条bets，超出部分可能漏计。",
    }


def kalshi(start_utc: datetime, end_utc: datetime):
    k = http_get_json("https://api.elections.kalshi.com/trade-api/v2/markets?limit=1000")
    mkts = k.get("markets", [])
    vol = 0.0
    traded = 0
    mkt = 0
    ctr = 0
    for x in mkts:
        v24 = float(x.get("volume_24h") or x.get("volume_24h_fp") or 0)
        vol += v24
        if v24 > 0:
            traded += 1
        c = x.get("created_time")
        if c:
            dt = parse_iso_utc(c)
            if start_utc <= dt < end_utc:
                mkt += 1
                ctr += 2

    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": None,
            "unit": "missing",
            "source_metric": "unavailable",
        },
        "derived": {"traded_contract_entries": None, "traded_markets": traded},
        "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
        "status": "missing",
        "note": "Kalshi公开市场列表缺少稳定可用的全量排序/聚合接口，当前无法可靠得到“当日成交合约总量”全站值；已标记缺失。",
    }


def render_html(report):
    rows = ""
    for name in ["Polymarket", "Manifold", "Kalshi"]:
        d = report["platforms"][name]
        p = d["primary"]
        a = d["auxiliary"]
        pv = "缺失" if p['value'] is None else p['value']
        am = "缺失" if a['new_market_count'] is None else a['new_market_count']
        ac = "缺失" if a['new_contract_listing_count'] is None else a['new_contract_listing_count']
        rows += f"<tr><td>{name}</td><td>{pv}</td><td>{p['unit']}</td><td>{p['source_metric']}</td><td>{am}</td><td>{ac}</td><td>{d['status']}</td><td>{d['note']}</td></tr>"

    return f"""<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width, initial-scale=1'><title>预测市场日报（主指标：当日成交合约总量）</title><style>body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:1100px;margin:32px auto;padding:0 16px}}table{{border-collapse:collapse;width:100%}}th,td{{border:1px solid #ddd;padding:8px;text-align:left;vertical-align:top}}th{{background:#f6f6f6}}.box{{background:#f7f7f7;border:1px solid #e9e9e9;border-radius:10px;padding:12px;margin:12px 0}}</style></head><body><h1>预测市场日报：主指标=当日成交合约总量</h1><p>日期：<b>{report['date_shanghai']}</b>｜生成：{report['generated_at']}｜完整性：{report['completeness']}</p><div class='box'><b>口径定义：</b><br>主指标优先：当日成交合约总量（或公开接口可得的最接近成交口径，单位按平台单独展示，不跨平台直接加总）。<br>辅助指标：新增市场/新增合约条目（listing口径），仅用于供给侧参考，不代表成交活跃度。</div><table><thead><tr><th>平台</th><th>主指标值</th><th>单位</th><th>主指标来源</th><th>辅助：新增市场</th><th>辅助：新增合约条目</th><th>状态</th><th>说明</th></tr></thead><tbody>{rows}</tbody></table></body></html>"""


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now_local, start_local, end_local, start_utc, end_utc = today_window()
    platforms = {}
    for name, fn in [("Polymarket", polymarket), ("Manifold", manifold), ("Kalshi", kalshi)]:
        try:
            platforms[name] = fn(start_utc, end_utc)
        except Exception as e:
            platforms[name] = {
                "primary": {"name": "当日成交合约总量", "value": None, "unit": "missing", "source_metric": "missing"},
                "derived": {"traded_contract_entries": None},
                "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
                "status": "missing",
                "note": f"抓取失败: {e}",
            }

    report = {
        "metric": "today_traded_contract_volume_primary",
        "timezone": "Asia/Shanghai",
        "date_shanghai": start_local.strftime("%Y-%m-%d"),
        "window": {
            "start_local": start_local.isoformat(),
            "end_local": end_local.isoformat(),
            "start_utc": start_utc.isoformat(),
            "end_utc": end_utc.isoformat(),
        },
        "generated_at": now_local.isoformat(),
        "definition": {
            "primary": "当日成交合约总量（或最接近公开口径）",
            "auxiliary": "新增市场/新增合约条目（listing口径）",
            "note": "避免与listing count混淆",
        },
        "platforms": platforms,
        "completeness": f"{sum(1 for v in platforms.values() if v['status'] != 'missing')}/3",
    }

    JSON_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(render_html(report), encoding="utf-8")
    print(json.dumps(platforms, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
