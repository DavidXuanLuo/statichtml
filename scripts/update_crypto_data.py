#!/usr/bin/env python3
import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path('/Users/uc_bilin/.openclaw/workspace/statichtml')
OUT_FILE = ROOT / 'crypto-data.json'
LEGACY_USDC_FILES = [ROOT / 'usdc-data.json', ROOT / 'data.json']

COINS = {
    'USDC': {'id': 'usd-coin', 'name': 'USD Coin', 'supplyMetric': 'circulating_supply（流通供应量）'},
    'USDT': {'id': 'tether', 'name': 'Tether', 'supplyMetric': 'circulating_supply（流通供应量）'},
    'BTC': {'id': 'bitcoin', 'name': 'Bitcoin', 'supplyMetric': 'circulating_supply（链上流通量，近似发行量）'},
    'ETH': {'id': 'ethereum', 'name': 'Ethereum', 'supplyMetric': 'circulating_supply（链上流通量，近似发行量）'},
}
SOURCE = 'CoinGecko'
SOURCE_URL = 'https://api.coingecko.com/api/v3/coins/markets'

def read_json(path: Path):
    if not path.exists():
        return None
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)

def load_or_init():
    data = read_json(OUT_FILE)
    if data and 'assets' in data:
        return data

    assets = {k: {'symbol': k, 'name': v['name'], 'source': SOURCE, 'records': []} for k, v in COINS.items()}

    legacy = None
    for f in LEGACY_USDC_FILES:
        legacy = read_json(f)
        if legacy and 'records' in legacy:
            break

    if legacy:
        for r in legacy.get('records', []):
            assets['USDC']['records'].append({
                'date': r.get('date'),
                'timestamp': r.get('timestamp'),
                'price': r.get('price'),
                'marketCap': r.get('marketCap'),
                'volume24h': r.get('volume24h'),
                'circulatingSupply': r.get('circulatingSupply'),
                'supplyMetric': COINS['USDC']['supplyMetric'],
                'source': r.get('source', SOURCE),
                'sourceUrl': SOURCE_URL,
            })

    return {
        'assets': assets,
        'lastUpdate': datetime.now(timezone.utc).isoformat(),
        'source': SOURCE,
    }

def fetch_market_data():
    ids = ','.join(v['id'] for v in COINS.values())
    params = urllib.parse.urlencode({'vs_currency': 'usd', 'ids': ids})
    url = f'{SOURCE_URL}?{params}'
    with urllib.request.urlopen(url, timeout=20) as r:
        return json.loads(r.read().decode('utf-8'))

def upsert_record(asset_records, new_record):
    for i, item in enumerate(asset_records):
        if item.get('date') == new_record['date']:
            asset_records[i] = new_record
            return
    asset_records.append(new_record)
    asset_records.sort(key=lambda x: x.get('date', ''))

def main():
    store = load_or_init()
    market = fetch_market_data()
    by_id = {m['id']: m for m in market}

    now = datetime.now(timezone.utc)
    date_str = datetime.now().strftime('%Y-%m-%d')
    ts = int(now.timestamp())

    for symbol, meta in COINS.items():
        m = by_id.get(meta['id'])
        if not m:
            continue
        record = {
            'date': date_str,
            'timestamp': ts,
            'price': m.get('current_price'),
            'marketCap': m.get('market_cap'),
            'volume24h': m.get('total_volume'),
            'circulatingSupply': m.get('circulating_supply'),
            'supplyMetric': meta['supplyMetric'],
            'source': SOURCE,
            'sourceUrl': SOURCE_URL,
        }
        upsert_record(store['assets'][symbol]['records'], record)
        store['assets'][symbol]['source'] = SOURCE

    store['lastUpdate'] = now.isoformat()
    store['source'] = SOURCE

    with OUT_FILE.open('w', encoding='utf-8') as f:
        json.dump(store, f, ensure_ascii=False, indent=2)

    # 兼容旧版USDC页面数据源
    usdc_legacy = {
        'records': store['assets']['USDC']['records'],
        'lastUpdate': store['lastUpdate'],
        'source': store['assets']['USDC'].get('source', SOURCE),
    }
    for legacy_file in LEGACY_USDC_FILES:
        with legacy_file.open('w', encoding='utf-8') as f:
            json.dump(usdc_legacy, f, ensure_ascii=False, indent=2)

    print(f"Updated {OUT_FILE} and legacy USDC files at {store['lastUpdate']}")

if __name__ == '__main__':
    main()
