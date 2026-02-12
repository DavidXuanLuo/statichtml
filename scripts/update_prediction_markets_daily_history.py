#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import subprocess
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

BASE = Path(__file__).resolve().parent.parent
DATA_PATH = BASE / "data" / "prediction-markets-daily-history.json"
HTML_PATH = BASE / "prediction-markets-daily.html"
TZ = ZoneInfo("Asia/Shanghai")
UTC = timezone.utc
UA = "Mozilla/5.0 (OpenClaw prediction-markets-daily bot)"
PLATFORMS = ["Polymarket", "Manifold", "Kalshi"]


def http_get_json(url: str, timeout: int = 20, retries: int = 3):
    err = None
    for i in range(retries):
        try:
            out = subprocess.check_output([
                "curl", "-sS", "--fail", "--max-time", str(timeout), "-A", UA,
                "-H", "Accept-Encoding: identity", url
            ], timeout=timeout + 3)
            return json.loads(out.decode("utf-8"))
        except Exception as e:
            err = e
            if i < retries - 1:
                time.sleep(1.2 ** i)
    raise err


def get_polymarket_value():
    limit = 200
    max_pages = 4
    total_24h = 0.0
    count = 0
    for page in range(max_pages):
        params = {
            "limit": limit,
            "offset": page * limit,
            "active": "true",
            "closed": "false",
            "order": "volume24hr",
            "ascending": "false",
        }
        arr = http_get_json("https://gamma-api.polymarket.com/markets?" + urllib.parse.urlencode(params), retries=4)
        if not isinstance(arr, list) or not arr:
            break
        count += len(arr)
        for m in arr:
            total_24h += float(m.get("volume24hr") or 0)
        if len(arr) < limit:
            break
    if count == 0:
        return None, "missing", "Polymarket Gamma API failed"
    return round(total_24h, 2), "partial", f"sum(markets.volume24hr), {count} active unresolved markets"


def get_manifold_value(start_utc: datetime):
    limit = 500
    max_pages = 4
    before = None
    vol = 0.0
    scanned = 0
    for _ in range(max_pages):
        params = {"limit": limit}
        if before is not None:
            params["beforeTime"] = before
        bets = http_get_json("https://api.manifold.markets/v0/bets?" + urllib.parse.urlencode(params), retries=4)
        if not isinstance(bets, list) or not bets:
            break
        scanned += len(bets)
        min_ts = None
        should_stop = False
        for b in bets:
            ts = int(b.get("createdTime") or 0)
            if min_ts is None or ts < min_ts:
                min_ts = ts
            dt = datetime.fromtimestamp(ts / 1000, tz=UTC)
            if dt < start_utc:
                should_stop = True
                continue
            vol += abs(float(b.get("shares") or 0))
        if min_ts is None:
            break
        before = min_ts - 1
        if should_stop:
            break
    if scanned == 0:
        return None, "missing", "Manifold bets API failed"
    return round(vol, 2), "partial", f"sum(abs(bets.shares)) from paged recent bets, scanned={scanned}"


def get_kalshi_published(today_local_str: str):
    snapshots = http_get_json("https://www.kalshidata.com/api/analytics/historical-snapshots", retries=4, timeout=25)
    arr = snapshots.get("snapshots", []) if isinstance(snapshots, dict) else []
    published = [x for x in arr if isinstance(x, dict) and x.get("date") and x["date"] < today_local_str]
    published.sort(key=lambda x: x["date"])
    if not published:
        return None, None, "missing", "Kalshi historical snapshots unavailable"
    latest = published[-1]
    # Use trading_volume_change (USD) for cross-platform comparability with Polymarket USDC volume.
    daily = latest.get("trading_volume_change")
    if daily is None and len(published) >= 2:
        prev = published[-2]
        daily = float(latest.get("trading_volume") or 0) - float(prev.get("trading_volume") or 0)
    if daily is None:
        return latest.get("date"), None, "missing", "Kalshi trading volume daily change missing"
    return latest.get("date"), round(float(daily), 2), "ok", "published_daily_t_plus_1 from kalshidata historical-snapshots.trading_volume_change"


def load_existing_records():
    if not DATA_PATH.exists():
        return []
    try:
        d = json.loads(DATA_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(d, dict) and isinstance(d.get("records"), list) and d.get("records") and isinstance(d["records"][0], dict) and "platform" in d["records"][0]:
        return d["records"]

    # migrate old wide format
    migrated = []
    if isinstance(d, dict) and isinstance(d.get("records"), list):
        for r in d["records"]:
            dt = r.get("date")
            if not dt:
                continue
            migrated.append({
                "date": dt, "platform": "Polymarket", "daily_total_value": r.get("polymarket_daily"),
                "unit": "USDC(volume24hr sum, proxy)", "source": "https://gamma-api.polymarket.com/markets",
                "method": "sum(markets.volume24hr)", "status": "partial" if r.get("polymarket_daily") is not None else "missing"
            })
            migrated.append({
                "date": dt, "platform": "Manifold", "daily_total_value": r.get("manifold_daily"),
                "unit": "shares(today, paged)", "source": "https://api.manifold.markets/v0/bets",
                "method": "sum(abs(bets.shares))", "status": "partial" if r.get("manifold_daily") is not None else "missing"
            })
            migrated.append({
                "date": dt, "platform": "Kalshi", "daily_total_value": r.get("kalshi_daily_published"),
                "unit": "contracts", "source": "https://www.kalshidata.com/api/analytics/historical-snapshots",
                "method": "published_daily_t_plus_1", "status": "ok" if r.get("kalshi_daily_published") is not None else "missing"
            })
    return migrated


def upsert(records, rec):
    key = (rec["date"], rec["platform"])
    idx = None
    for i, r in enumerate(records):
        if (r.get("date"), r.get("platform")) == key:
            idx = i
            break
    if idx is None:
        records.append(rec)
    else:
        records[idx] = rec


def ensure_90_day_coverage(records, end_date_str):
    """Deprecated: do not backfill null placeholder rows.
    Keep function for compatibility but intentionally no-op.
    """
    return


def render_html():
    return """<!doctype html>
<html lang=\"zh-CN\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>预测市场多平台日度总值趋势</title>
  <style>
    :root{--bg:#f6f7fb;--card:#fff;--text:#111827;--muted:#6b7280;--border:#e5e7eb}
    body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'PingFang SC','Noto Sans CJK SC',sans-serif}
    .wrap{max-width:1100px;margin:0 auto;padding:14px}
    .card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:12px;margin:10px 0}
    h1{font-size:20px;margin:4px 0 8px}
    .meta{font-size:12px;color:var(--muted);margin-bottom:10px}
    .legend{display:flex;gap:10px;flex-wrap:wrap;font-size:12px;color:#374151;margin-bottom:8px}
    .dot{display:inline-block;width:10px;height:10px;border-radius:99px;margin-right:4px;vertical-align:middle}
    canvas{width:100%;height:280px;display:block;background:#fff;border-radius:8px}
    table{width:100%;border-collapse:collapse;font-size:12px}
    th,td{border-bottom:1px solid #eef0f3;padding:7px 6px;text-align:right;vertical-align:top}
    th:first-child,td:first-child{text-align:left;white-space:nowrap}
    .null{color:#9ca3af}
    .left{text-align:left}
  </style>
</head>
<body>
<div class=\"wrap\">
  <h1>预测市场多平台日度总值趋势（近90天）</h1>
  <div id=\"meta\" class=\"meta\">加载中…</div>
  <section class=\"card\">
    <div class=\"legend\">
      <span><i class=\"dot\" style=\"background:#2563eb\"></i>Polymarket</span>
      <span><i class=\"dot\" style=\"background:#16a34a\"></i>Manifold</span>
      <span><i class=\"dot\" style=\"background:#7c3aed\"></i>Kalshi（T+1公开日度）</span>
    </div>
    <canvas id=\"chart\" width=\"1060\" height=\"280\"></canvas>
  </section>
  <section class=\"card\">
    <h3 style=\"margin:0 0 8px;font-size:15px\">明细（按日期×平台）</h3>
    <div style=\"overflow:auto\"><table>
      <thead><tr><th>date</th><th>platform</th><th>daily_total_value</th><th>unit</th><th>status</th><th class=\"left\">method</th></tr></thead>
      <tbody id=\"tbody\"></tbody>
    </table></div>
  </section>
</div>
<script>
(async function(){
  const res = await fetch('./data/prediction-markets-daily-history.json?_='+Date.now());
  const data = await res.json();
  const priority = {Kalshi:0, Polymarket:1, Manifold:2};
  const rows = (data.records||[]).slice().sort((a,b)=> (a.date===b.date? (priority[a.platform]??9)-(priority[b.platform]??9) :a.date.localeCompare(b.date)));
  const recentCut = (data.coverage && data.coverage.start_date) ? data.coverage.start_date : rows[0]?.date;
  const recentRows = rows.filter(r => (!recentCut || r.date >= recentCut) && r.daily_total_value != null);

  const byDate = {};
  for (const r of recentRows){
    if(!byDate[r.date]) byDate[r.date] = {Polymarket:null, Manifold:null, Kalshi:null};
    byDate[r.date][r.platform] = r.daily_total_value;
  }
  const labels = Object.keys(byDate).sort();
  const p = labels.map(d=>byDate[d].Polymarket);
  const m = labels.map(d=>byDate[d].Manifold);
  const k = labels.map(d=>byDate[d].Kalshi);

  function drawLineChart(canvas, series, colors, labels){
    const ctx = canvas.getContext('2d');
    const W = canvas.width, H = canvas.height;
    const pad = {l:42,r:12,t:12,b:24};
    ctx.clearRect(0,0,W,H);
    const all = series.flat().filter(v=>v!=null);
    const min = all.length?Math.min(...all):0;
    const max = all.length?Math.max(...all):1;
    const yMin = min===max?0:min;
    const yMax = min===max?max*1.1:max*1.1;
    ctx.strokeStyle='#e5e7eb'; ctx.lineWidth=1;
    for(let i=0;i<4;i++){ const y=pad.t+(H-pad.t-pad.b)*i/3; ctx.beginPath(); ctx.moveTo(pad.l,y); ctx.lineTo(W-pad.r,y); ctx.stroke(); }
    const n = labels.length;
    const x = i => pad.l + (W-pad.l-pad.r)*(n<=1?0:i/(n-1));
    const y = v => pad.t + (H-pad.t-pad.b)*(1-(v-yMin)/(yMax-yMin||1));
    ctx.fillStyle='#6b7280'; ctx.font='11px sans-serif';
    labels.forEach((lb,i)=>{ if(i===0||i===n-1||i%7===0){ ctx.fillText(lb.slice(5), x(i)-12, H-6); } });
    series.forEach((arr,si)=>{
      ctx.strokeStyle=colors[si]; ctx.lineWidth=2; let started=false; ctx.beginPath();
      arr.forEach((v,i)=>{ if(v==null){started=false;return;} const px=x(i), py=y(v); if(!started){ctx.moveTo(px,py);started=true;} else {ctx.lineTo(px,py);} });
      ctx.stroke();
      arr.forEach((v,i)=>{ if(v==null) return; const px=x(i), py=y(v); ctx.fillStyle=colors[si]; ctx.beginPath(); ctx.arc(px,py,2.4,0,Math.PI*2); ctx.fill(); });
    });
  }

  drawLineChart(document.getElementById('chart'), [p,m,k], ['#2563eb','#16a34a','#7c3aed'], labels);

  const fmt = (v)=> v==null ? '<span class=\"null\">null</span>' : Number(v).toLocaleString(undefined,{maximumFractionDigits:2});
  document.getElementById('tbody').innerHTML = recentRows.map(r => `<tr><td>${r.date}</td><td>${r.platform}</td><td>${fmt(r.daily_total_value)}</td><td>${r.unit||''}</td><td>${r.status||''}</td><td class=\"left\">${r.method||''}</td></tr>`).join('');
  document.getElementById('meta').textContent = `覆盖：${data.coverage.start_date} ~ ${data.coverage.end_date} ｜ 生成：${data.generated_at}`;
})();
</script>
</body>
</html>
"""


def main():
    now_local = datetime.now(TZ)
    today_local = now_local.date().isoformat()
    start_local = datetime(now_local.year, now_local.month, now_local.day, tzinfo=TZ)
    start_utc = start_local.astimezone(UTC)

    records = load_existing_records()

    # Today snapshots for Polymarket/Manifold
    p_val, p_status, p_method_detail = get_polymarket_value()
    upsert(records, {
        "date": today_local,
        "platform": "Polymarket",
        "daily_total_value": p_val,
        "unit": "USDC(volume24hr sum, proxy)",
        "source": "https://gamma-api.polymarket.com/markets",
        "method": p_method_detail,
        "status": p_status,
    })

    m_val, m_status, m_method_detail = get_manifold_value(start_utc)
    upsert(records, {
        "date": today_local,
        "platform": "Manifold",
        "daily_total_value": m_val,
        "unit": "shares(today, paged)",
        "source": "https://api.manifold.markets/v0/bets",
        "method": m_method_detail,
        "status": m_status,
    })

    k_date, k_val, k_status, k_method_detail = get_kalshi_published(today_local)
    if k_date:
        upsert(records, {
            "date": k_date,
            "platform": "Kalshi",
            "daily_total_value": k_val,
            "unit": "USD(trading_volume_change, T+1)",
            "source": "https://www.kalshidata.com/api/analytics/historical-snapshots",
            "method": k_method_detail,
            "status": k_status,
        })

    # Keep at least recent 90-day coverage
    ensure_90_day_coverage(records, today_local)

    records.sort(key=lambda r: (r.get("date", ""), r.get("platform", "")))

    # trim to recent 90 days only
    cutoff = (datetime.strptime(today_local, "%Y-%m-%d").date() - timedelta(days=89)).isoformat()
    trimmed = [r for r in records if r.get("date") and r["date"] >= cutoff and r.get("daily_total_value") is not None]

    payload = {
        "dataset": "prediction_markets_daily_history",
        "timezone": "Asia/Shanghai",
        "generated_at": now_local.isoformat(timespec="seconds"),
        "coverage": {
            "start_date": cutoff,
            "end_date": today_local,
        },
        "records": trimmed,
    }

    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    DATA_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    HTML_PATH.write_text(render_html(), encoding="utf-8")

    # stdout for cron summary helpers
    latest = {
        "date": today_local,
        "polymarket": p_val,
        "manifold": m_val,
        "kalshi_published_date": k_date,
        "kalshi": k_val,
    }
    print(json.dumps(latest, ensure_ascii=False))


if __name__ == "__main__":
    main()
