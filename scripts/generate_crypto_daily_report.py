#!/usr/bin/env python3
import json
from datetime import datetime
from pathlib import Path

ROOT = Path('/Users/uc_bilin/.openclaw/workspace/statichtml')
DATA = ROOT / 'crypto-data.json'
OUT = ROOT / 'data' / 'crypto-daily-report-latest.txt'
PUBLIC_URL = 'https://davidxuanluo.github.io/statichtml/crypto-dashboard.html'

with DATA.open('r', encoding='utf-8') as f:
    d = json.load(f)

def fmt_price(symbol, price):
    if price is None:
        return '--'
    return f"{price:.4f}" if symbol in ['USDC', 'USDT'] else f"{price:,.2f}"

def fmt_num(v):
    if v is None:
        return '--'
    return f"{v:,.0f}"

lines = []
lines.append(f"# 四币日报 {datetime.now().strftime('%Y-%m-%d %H:%M')} (GMT+8)")
for symbol in ['USDC', 'USDT', 'BTC', 'ETH']:
    asset = d['assets'].get(symbol, {})
    recs = asset.get('records', [])
    if not recs:
        continue
    r = recs[-1]
    lines.append(
        f"- {symbol}: 价格 ${fmt_price(symbol, r.get('price'))}, 流通量/发行量 {fmt_num(r.get('circulatingSupply'))}, 24h交易额 ${fmt_num(r.get('volume24h'))}, 来源 {r.get('source')}"
    )

lines.append(f"- 看板链接: {PUBLIC_URL}")
text = '\n'.join(lines)
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(text, encoding='utf-8')
print(text)
