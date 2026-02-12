#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import time
import urllib.parse
import subprocess
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
JSON_PATH = DATA_DIR / "prediction-markets-today.json"
JSON_PATH_ROOT = BASE_DIR / "prediction-markets-today.json"
HTML_PATH = BASE_DIR / "prediction-markets-today.html"
TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
UA = {
    "User-Agent": "Mozilla/5.0 (OpenClaw prediction-markets-today bot)",
    "Accept-Encoding": "identity",
}


def http_get_json(url: str, retries: int = 3, timeout: int = 12, backoff: float = 1.3):
    last_err = None
    for i in range(retries):
        try:
            cmd = ["curl", "-sS", "--fail", "--max-time", str(timeout), "-A", UA["User-Agent"], "-H", "Accept-Encoding: identity", url]
            out = subprocess.check_output(cmd, timeout=timeout + 2)
            return json.loads(out.decode("utf-8"))
        except Exception as e:
            last_err = e
            if i < retries - 1:
                time.sleep(backoff ** i)
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


def polymarket(_start_utc: datetime, _end_utc: datetime):
    limit = 200
    max_pages = 4
    total_24h = 0.0
    market_count = 0
    nonzero_count = 0
    for page in range(max_pages):
        params = {"limit": limit, "offset": page * limit, "active": "true", "closed": "false", "order": "volume24hr", "ascending": "false"}
        arr = http_get_json("https://gamma-api.polymarket.com/markets?" + urllib.parse.urlencode(params), retries=4)
        if not isinstance(arr, list) or not arr:
            break
        market_count += len(arr)
        for m in arr:
            v = float(m.get("volume24hr") or 0)
            total_24h += v
            if v > 0:
                nonzero_count += 1
        if len(arr) < limit:
            break
    return {
        "primary": {"name": "当日成交合约总量", "value": round(total_24h, 2) if market_count > 0 else None, "unit": "USDC(volume24hr sum, proxy)", "source_metric": "sum(markets.volume24hr)"},
        "derived": {"traded_contract_entries": nonzero_count, "traded_markets": nonzero_count},
        "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
        "status": "partial" if market_count > 0 else "missing",
        "note": f"公开Gamma接口分页抓取{market_count}个活跃未结算市场，以volume24hr汇总作为近似（24h滚动）。" if market_count > 0 else "Polymarket接口失败。",
    }


def manifold(start_utc: datetime, _end_utc: datetime):
    limit = 500
    max_pages = 4
    before = None
    vol = 0.0
    cset = set()
    scanned = 0
    for _ in range(max_pages):
        params = {"limit": limit}
        if before is not None:
            params["beforeTime"] = before
        bets = http_get_json("https://api.manifold.markets/v0/bets?" + urllib.parse.urlencode(params), retries=4)
        if not isinstance(bets, list) or not bets:
            break
        scanned += len(bets)
        min_ts, stop = None, False
        for x in bets:
            t = int(x.get("createdTime") or 0)
            min_ts = t if min_ts is None else min(min_ts, t)
            dt = datetime.fromtimestamp(t / 1000, tz=UTC)
            if dt < start_utc:
                stop = True
                continue
            vol += abs(float(x.get("shares") or 0))
            if x.get("contractId"):
                cset.add(x["contractId"])
        before = (min_ts - 1) if min_ts is not None else None
        if stop or before is None:
            break
    m = http_get_json("https://api.manifold.markets/v0/markets?limit=1000", retries=4)
    mkt = sum(1 for x in m if datetime.fromtimestamp(int(x.get("createdTime") or 0) / 1000, tz=UTC) >= start_utc)
    ctr = sum((2 if (x.get("outcomeType") or "").upper() == "BINARY" else 1) for x in m if datetime.fromtimestamp(int(x.get("createdTime") or 0) / 1000, tz=UTC) >= start_utc)
    return {
        "primary": {"name": "当日成交合约总量", "value": round(vol, 2), "unit": "shares(today, paged)", "source_metric": "sum(abs(bets.shares))"},
        "derived": {"traded_contract_entries": len(cset), "bets_scanned": scanned},
        "auxiliary": {"new_market_count": mkt, "new_contract_listing_count": ctr},
        "status": "partial",
        "note": f"分页扫描{scanned}条bets，累加当日shares绝对值近似。",
    }


def kalshi(_start_utc: datetime, _end_utc: datetime):
    snapshots = http_get_json("https://www.kalshidata.com/api/analytics/historical-snapshots", retries=4, timeout=20)
    arr = snapshots.get("snapshots", []) if isinstance(snapshots, dict) else []

    today_local_str = datetime.now(TZ).strftime("%Y-%m-%d")
    published = [x for x in arr if isinstance(x, dict) and x.get("date") and x["date"] < today_local_str]
    published.sort(key=lambda x: x["date"])

    if not published:
        return {
            "primary": {"name": "最新已公布交易日成交合约总量", "value": None, "unit": "接口失败", "source_metric": "n/a"},
            "derived": {"method": "missing", "published_date": None, "robinhood_inferred_contracts": None},
            "auxiliary": {"new_market_count": None, "new_contract_listing_count": None, "robinhood_inferred_contracts": None},
            "status": "missing",
            "note": "Kalshi公开日度接口未返回可用历史快照。",
        }

    latest = published[-1]
    prev = published[-2] if len(published) >= 2 else None

    daily_change = latest.get("total_contracts_traded_change")
    if daily_change is None and prev is not None:
        daily_change = float(latest.get("total_contracts_traded") or 0) - float(prev.get("total_contracts_traded") or 0)

    kalshi_main = int(round(float(daily_change or 0)))
    robinhood_inferred = int(round(kalshi_main * 0.5))

    published_date = latest.get("date")
    return {
        "primary": {
            "name": "最新已公布交易日成交合约总量",
            "value": kalshi_main,
            "unit": "contracts（整数）",
            "source_metric": "kalshidata historical-snapshots.total_contracts_traded_change",
        },
        "derived": {
            "method": "published_daily_t_plus_1",
            "published_date": published_date,
            "total_contracts_traded_cum": int(round(float(latest.get("total_contracts_traded") or 0))),
            "robinhood_inferred_contracts": robinhood_inferred,
        },
        "auxiliary": {
            "new_market_count": None,
            "new_contract_listing_count": None,
            "robinhood_inferred_contracts": robinhood_inferred,
        },
        "status": "ok",
        "note": f"采用公开日度口径（T+1）：展示最新已公布交易日 {published_date} 的Kalshi日度总值={kalshi_main:,} contracts；Robinhood反推={robinhood_inferred:,}（=Kalshi×0.5，整数）。",
    }


def render_html(report):
    cards = ""
    for name in ["Polymarket", "Manifold", "Kalshi"]:
        d = report["platforms"][name]
        p = d["primary"]
        a = d["auxiliary"]
        val = "接口失败" if p["value"] is None else f"{int(p['value']):,}" if name == "Kalshi" else f"{p['value']:,}"
        cls = "card kalshi" if name == "Kalshi" else "card"
        focus = "<div class='focus'>Kalshi主值（优先展示）</div>" if name == "Kalshi" else ""
        rh_line = ""
        published_line = ""
        if name == "Kalshi":
            pub_date = d.get("derived", {}).get("published_date")
            if pub_date:
                published_line = f"<li><b>公布交易日：</b>{pub_date}</li>"
            if a.get("robinhood_inferred_contracts") is not None:
                rh_line = f"<li><b>Robinhood反推：</b>{int(a['robinhood_inferred_contracts']):,} contracts（=Kalshi×0.5，整数）</li>"
        cards += f"""
<section class='{cls}'>
  <h3>{name} <span class='tag {d['status']}'>{d['status']}</span></h3>
  {focus}
  <div class='big'>{val}</div>
  <div class='sub'>{p['unit']}</div>
  <ul>
    <li><b>主指标来源：</b>{p['source_metric']}</li>
    <li><b>辅助-新增市场：</b>{'—' if a['new_market_count'] is None else a['new_market_count']}</li>
    <li><b>辅助-新增合约条目：</b>{'—' if a['new_contract_listing_count'] is None else a['new_contract_listing_count']}</li>
    {published_line}
    {rh_line}
    <li><b>说明：</b>{d['note']}</li>
  </ul>
</section>
"""

    return f"""<!doctype html>
<html lang='zh-CN'>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<title>预测市场日报（主指标：当日成交合约总量）</title>
<style>
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;max-width:980px;margin:0 auto;padding:14px;background:#fafafa;color:#111}}
.h{{font-size:20px;font-weight:700;margin:6px 0 10px}}
.meta{{font-size:13px;color:#444;margin-bottom:10px}}
.box{{background:#fff;border:1px solid #e6e6e6;border-radius:12px;padding:12px;margin:10px 0;line-height:1.45}}
.grid{{display:grid;grid-template-columns:1fr;gap:10px}}
.card{{background:#fff;border:1px solid #e7e7e7;border-radius:12px;padding:12px}}
.card h3{{margin:0 0 8px;font-size:16px;display:flex;justify-content:space-between;align-items:center}}
.card.kalshi{{border:2px solid #9b5cff;background:linear-gradient(180deg,#faf6ff,#fff)}}
.focus{{display:inline-block;font-size:11px;color:#6b2bd9;background:#f1e8ff;border:1px solid #d6bfff;border-radius:999px;padding:3px 8px;margin-bottom:6px}}
.big{{font-size:28px;font-weight:800;line-height:1.1;word-break:break-word}}
.sub{{font-size:12px;color:#666;margin-top:2px;margin-bottom:8px;word-break:break-word}}
ul{{padding-left:18px;margin:6px 0 0}} li{{margin:4px 0;font-size:13px;line-height:1.45}}
.tag{{font-size:11px;padding:2px 6px;border-radius:999px;border:1px solid #ddd;background:#f7f7f7}}
.tag.partial{{background:#fff6e8;border-color:#f1d39f}} .tag.missing{{background:#ffeef0;border-color:#ffccd5}} .tag.ok{{background:#eaf7ea;border-color:#bce0bc}}
@media (min-width: 860px){{.grid{{grid-template-columns:1fr 1fr 1fr}}}}
</style>
</head>
<body>
<div class='h'>预测市场日报：主指标=当日成交合约总量</div>
<div class='meta'>日期：<b>{report['date_shanghai']}</b> ｜ 生成：{report['generated_at']} ｜ 完整性：{report['completeness']}</div>
<div class='box'><b>口径说明</b>：
主指标为“当日成交合约总量”。Kalshi主口径切换为<b>公开日度总值（T+1）</b>，展示“最新已公布交易日”的整数contracts，并标注该日期；Robinhood同步展示反推值（Kalshi×0.5，整数）。不同平台单位不同，<b>不做跨平台直接加总</b>。
</div>
<div class='grid'>{cards}</div>
</body></html>"""


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    now_local, start_local, end_local, start_utc, end_utc = today_window()
    platforms = {}
    for name, fn in [("Polymarket", polymarket), ("Manifold", manifold), ("Kalshi", kalshi)]:
        try:
            platforms[name] = fn(start_utc, end_utc)
        except Exception as e:
            platforms[name] = {
                "primary": {"name": "当日成交合约总量", "value": None, "unit": "接口失败", "source_metric": "n/a"},
                "derived": {"traded_contract_entries": None},
                "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
                "status": "missing",
                "note": f"接口重试后仍失败：{e}",
            }

    report = {
        "metric": "today_traded_contract_volume_primary",
        "timezone": "Asia/Shanghai",
        "date_shanghai": start_local.strftime("%Y-%m-%d"),
        "window": {"start_local": start_local.isoformat(), "end_local": end_local.isoformat(), "start_utc": start_utc.isoformat(), "end_utc": end_utc.isoformat()},
        "generated_at": now_local.isoformat(),
        "definition": {
            "primary": "当日成交合约总量（Kalshi主口径=公开日度总值T+1）",
            "auxiliary": "新增市场/新增合约条目（listing口径）",
            "note": "Kalshi展示最新已公布交易日整数contracts并标注日期；Robinhood=Kalshi×0.5（整数）；跨平台单位不可直接求和",
        },
        "platforms": platforms,
        "completeness": f"{sum(1 for v in platforms.values() if v['status'] != 'missing')}/3",
    }

    payload = json.dumps(report, ensure_ascii=False, indent=2)
    JSON_PATH.write_text(payload, encoding="utf-8")
    JSON_PATH_ROOT.write_text(payload, encoding="utf-8")
    HTML_PATH.write_text(render_html(report), encoding="utf-8")
    print(payload)


if __name__ == "__main__":
    main()
