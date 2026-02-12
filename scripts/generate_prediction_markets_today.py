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


def kalshi(start_utc: datetime, end_utc: datetime):
    now_utc = datetime.now(UTC)
    end_cap = now_utc if now_utc < end_utc else end_utc
    start_s, end_s = int(start_utc.timestamp()), int(end_cap.timestamp())

    cursor = None
    pages = 0
    scanned = 0
    total = 0.0
    tickers = set()
    first_ts = None
    last_ts = None
    max_pages = 20

    for _ in range(max_pages):
        params = {"limit": 1000, "min_ts": start_s, "max_ts": end_s}
        if cursor:
            params["cursor"] = cursor
        data = http_get_json("https://api.elections.kalshi.com/trade-api/v2/markets/trades?" + urllib.parse.urlencode(params), retries=3, timeout=20)
        arr = data.get("trades", []) if isinstance(data, dict) else []
        if not arr:
            break
        pages += 1
        scanned += len(arr)
        if first_ts is None:
            first_ts = parse_iso_utc(arr[0]["created_time"])
        last_ts = parse_iso_utc(arr[-1]["created_time"])
        for t in arr:
            total += float(t.get("count_fp") or t.get("count") or 0)
            if t.get("ticker"):
                tickers.add(t["ticker"])
        cursor = data.get("cursor") if isinstance(data, dict) else None
        if not cursor:
            break

    m = http_get_json("https://api.elections.kalshi.com/trade-api/v2/markets?limit=200", retries=4)
    mkts = m.get("markets", []) if isinstance(m, dict) else []
    v24_nonzero = sum(1 for x in mkts if float(x.get("volume_24h_fp") or x.get("volume_24h") or 0) > 0)
    sum_v24 = round(sum(float(x.get("volume_24h_fp") or x.get("volume_24h") or 0) for x in mkts), 2)
    sum_oi = round(sum(float(x.get("open_interest_fp") or x.get("open_interest") or 0) for x in mkts), 2)
    new_markets = sum(1 for x in mkts if x.get("created_time") and start_utc <= parse_iso_utc(x["created_time"]) < end_utc)

    if scanned > 0 and first_ts and last_ts:
        if not cursor:
            value = round(total, 2)
            unit = "contracts(trades.count_fp, Shanghai day exact)"
            source = "sum(/markets/trades.count_fp, min_ts~max_ts)"
            method = "exact"
            note = f"Kalshi官方交易流精确汇总：{pages}页/{scanned}条成交，覆盖{len(tickers)}个ticker。"
            status = "ok"
        else:
            span = max(1.0, (first_ts - last_ts).total_seconds())
            elapsed = max(1.0, (end_cap - start_utc).total_seconds())
            est = total / span * elapsed
            value = round(est, 2)
            unit = "contracts(sampled trade-rate extrapolation)"
            source = "sum(first_20_pages.trades.count_fp) * elapsed_day/sample_span"
            method = "extrapolated_from_official_trade_feed"
            note = (
                f"今日全量交易页数过大，采用可验证替代：官方/markets/trades前{pages}页样本（{scanned}条）"
                f"覆盖{round(span/60,1)}分钟，按同速率外推至今日已过时长。"
            )
            status = "partial"
    else:
        value, unit, source, method, status = None, "missing", "n/a", "missing", "missing"
        note = "Kalshi接口异常。"

    note += f" 字段校验：yes_bid/no_bid为美分报价（见*_dollars）；open_interest/open_interest_fp为持仓合约数。markets抽样200个中volume_24h非零{v24_nonzero}个（sum={sum_v24}）。"

    return {
        "primary": {"name": "当日成交合约总量", "value": value, "unit": unit, "source_metric": source},
        "derived": {
            "traded_contract_entries": len(tickers),
            "trades_scanned": scanned,
            "trades_pages_sampled": pages,
            "markets_sampled": len(mkts),
            "volume24h_nonzero_markets": v24_nonzero,
            "open_interest_sum_markets_sample": sum_oi,
            "method": method,
        },
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
        cls = "card kalshi" if name == "Kalshi" else "card"
        focus = "<div class='focus'>Kalshi主值（优先展示）</div>" if name == "Kalshi" else ""
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
主指标为“当日成交合约总量”。Kalshi优先用官方交易流 /markets/trades（count_fp）口径；若当日全量分页过大，降级为“官方交易流样本速率外推”，并在卡片中明确标注。不同平台单位不同，<b>不做跨平台直接加总</b>。
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
