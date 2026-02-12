#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import time
import urllib.request
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
            # curl更稳定，避免urllib在chunked响应下偶发卡住/IncompleteRead
            cmd = [
                "curl", "-sS", "--fail", "--max-time", str(timeout),
                "-A", UA["User-Agent"], "-H", "Accept-Encoding: identity", url,
            ]
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
    # 主指标降级口径：sum(volume24hr) across active & open markets, paginated.
    limit = 200
    max_pages = 4
    total_24h = 0.0
    market_count = 0
    nonzero_count = 0

    for page in range(max_pages):
        params = {
            "limit": limit,
            "offset": page * limit,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        u = "https://gamma-api.polymarket.com/markets?" + urllib.parse.urlencode(params)
        arr = http_get_json(u, retries=4)
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

    status = "partial" if market_count > 0 else "missing"
    value = round(total_24h, 2) if market_count > 0 else None
    note = (
        f"公开Gamma接口分页抓取{market_count}个活跃未结算市场，以volume24hr汇总作为“当日成交合约总量”近似。"
        f"该口径为24小时滚动窗口，可能与自然日(Asia/Shanghai)有偏差；若存在分页上限仍可能低估。"
    ) if market_count > 0 else "Polymarket接口多次重试仍失败，无法获取可用近似值。"

    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": value,
            "unit": "USDC(volume24hr sum, proxy)",
            "source_metric": "sum(markets.volume24hr)",
        },
        "derived": {
            "traded_contract_entries": nonzero_count,
            "traded_markets": nonzero_count,
        },
        "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
        "status": status,
        "note": note,
    }


def manifold(start_utc: datetime, _end_utc: datetime):
    # 主指标：sum(abs(shares))，按beforeTime分页尽可能覆盖今日。
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
        u = "https://api.manifold.markets/v0/bets?" + urllib.parse.urlencode(params)
        bets = http_get_json(u, retries=4)
        if not isinstance(bets, list) or not bets:
            break

        scanned += len(bets)
        min_ts = None
        stop = False
        for x in bets:
            t = int(x.get("createdTime") or 0)
            if min_ts is None or t < min_ts:
                min_ts = t
            dt = datetime.fromtimestamp(t / 1000, tz=UTC)
            if dt < start_utc:
                stop = True
                continue
            vol += abs(float(x.get("shares") or 0))
            cid = x.get("contractId")
            if cid:
                cset.add(cid)
        if min_ts is None:
            break
        before = min_ts - 1
        if stop:
            break

    # auxiliary from latest markets listing
    m = http_get_json("https://api.manifold.markets/v0/markets?limit=1000", retries=4)
    mkt = 0
    ctr = 0
    for x in m:
        t = int(x.get("createdTime") or 0)
        dt = datetime.fromtimestamp(t / 1000, tz=UTC)
        if dt >= start_utc:
            mkt += 1
            ctr += 2 if (x.get("outcomeType") or "").upper() == "BINARY" else 1

    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": round(vol, 2),
            "unit": "shares(today, paged)",
            "source_metric": "sum(abs(bets.shares))",
        },
        "derived": {"traded_contract_entries": len(cset), "bets_scanned": scanned},
        "auxiliary": {"new_market_count": mkt, "new_contract_listing_count": ctr},
        "status": "partial",
        "note": f"按beforeTime分页扫描{scanned}条bets，累加当日shares绝对值；为公开口径近似，非官方结算成交量。",
    }


def kalshi(start_utc: datetime, end_utc: datetime):
    # 主指标降级口径：sum(volume_24h) across paged markets.
    limit = 200
    max_pages = 10
    cursor = None
    total_24h = 0.0
    market_count = 0
    nonzero = 0
    new_markets = 0

    seen_cursors = set()
    for _ in range(max_pages):
        if cursor and cursor in seen_cursors:
            break
        if cursor:
            seen_cursors.add(cursor)
        params = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        u = "https://api.elections.kalshi.com/trade-api/v2/markets?" + urllib.parse.urlencode(params)
        data = http_get_json(u, retries=4)
        mkts = data.get("markets", []) if isinstance(data, dict) else []
        if not mkts:
            break
        market_count += len(mkts)

        for x in mkts:
            v24 = float(x.get("volume_24h") or x.get("volume_24h_fp") or 0)
            total_24h += v24
            if v24 > 0:
                nonzero += 1
            c = x.get("created_time")
            if c:
                dt = parse_iso_utc(c)
                if start_utc <= dt < end_utc:
                    new_markets += 1

        cursor = data.get("cursor") if isinstance(data, dict) else None
        if not cursor:
            break

    status = "partial" if market_count > 0 else "missing"
    value = round(total_24h, 2) if market_count > 0 else None
    note = (
        f"Kalshi分页抓取{market_count}个市场，按volume_24h汇总作为当日成交近似(24h滚动)。"
        f"该值并非严格自然日，且受分页覆盖影响。"
    ) if market_count > 0 else "Kalshi接口多次重试仍失败，无法获取可用近似值。"

    return {
        "primary": {
            "name": "当日成交合约总量",
            "value": value,
            "unit": "contracts(volume_24h sum, proxy)",
            "source_metric": "sum(markets.volume_24h)",
        },
        "derived": {"traded_contract_entries": nonzero, "traded_markets": nonzero},
        "auxiliary": {"new_market_count": new_markets, "new_contract_listing_count": new_markets * 2},
        "status": status,
        "note": note,
    }


def render_html(report):
    cards = ""
    for name in ["Polymarket", "Manifold", "Kalshi"]:
        d = report["platforms"][name]
        p = d["primary"]
        a = d["auxiliary"]
        val = "接口失败" if p["value"] is None else f"{p['value']:,}"
        cards += f"""
<section class='card'>
  <h3>{name} <span class='tag {d['status']}'>{d['status']}</span></h3>
  <div class='big'>{val}</div>
  <div class='sub'>{p['unit']}</div>
  <ul>
    <li><b>主指标来源：</b>{p['source_metric']}</li>
    <li><b>辅助-新增市场：</b>{'—' if a['new_market_count'] is None else a['new_market_count']}</li>
    <li><b>辅助-新增合约条目：</b>{'—' if a['new_contract_listing_count'] is None else a['new_contract_listing_count']}</li>
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
.big{{font-size:28px;font-weight:800;line-height:1.1}}
.sub{{font-size:12px;color:#666;margin-top:2px;margin-bottom:8px}}
ul{{padding-left:18px;margin:6px 0 0}} li{{margin:4px 0;font-size:13px;line-height:1.4}}
.tag{{font-size:11px;padding:2px 6px;border-radius:999px;border:1px solid #ddd;background:#f7f7f7}}
.tag.partial{{background:#fff6e8;border-color:#f1d39f}} .tag.missing{{background:#ffeef0;border-color:#ffccd5}} .tag.ok{{background:#eaf7ea;border-color:#bce0bc}}
@media (min-width: 860px){{.grid{{grid-template-columns:1fr 1fr 1fr}}}}
</style>
</head>
<body>
<div class='h'>预测市场日报：主指标=当日成交合约总量</div>
<div class='meta'>日期：<b>{report['date_shanghai']}</b> ｜ 生成：{report['generated_at']} ｜ 完整性：{report['completeness']}</div>
<div class='box'><b>口径说明</b>：
主指标为“当日成交合约总量”。若平台无严格自然日成交总量公开接口，则降级为可复现近似值（如 volume24hr/volume_24h 汇总或 bets.shares 累加），并在平台说明中标注偏差来源。不同平台单位不同，<b>不做跨平台直接加总</b>。
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
                "primary": {
                    "name": "当日成交合约总量",
                    "value": None,
                    "unit": "接口失败",
                    "source_metric": "n/a",
                },
                "derived": {"traded_contract_entries": None},
                "auxiliary": {"new_market_count": None, "new_contract_listing_count": None},
                "status": "missing",
                "note": f"接口重试后仍失败：{e}。降级说明：请参考该平台近24h公开榜单/官方页面进行人工补全。",
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
            "primary": "当日成交合约总量（无法直取时采用公开可复现近似）",
            "auxiliary": "新增市场/新增合约条目（listing口径）",
            "note": "避免与listing count混淆；跨平台单位不可直接求和",
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
