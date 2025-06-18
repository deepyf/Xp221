import os
import pandas as pd
import time
import random
import yfinance as yf
import requests
from pathlib import Path

def get_random_ua():
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edg/115.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ]
    return random.choice(uas)

def fetch_symbol_info(symbol):
    backoff = 1
    for _ in range(5):
        try:
            ticker = yf.Ticker(symbol)
            session = requests.Session()
            session.headers.update({'User-Agent': get_random_ua()})
            ticker.session = session
            info = ticker.info
            return {
                'currentPrice': info.get('currentPrice') or info.get('regularMarketPrice'),
                'marketCap': info.get('marketCap'),
                'industry': info.get('industry'),
                'sector': info.get('sector')
            }
        except Exception:
            time.sleep(backoff)
            backoff *= 2
    return {'currentPrice': None, 'marketCap': None, 'industry': None, 'sector': None}

def main():
    script_dir = Path(__file__).parent
    data_path = script_dir / 'data.csv'
    print(f"Reading data from {data_path.resolve()}")
    try:
        df = pd.read_csv(data_path, dtype=str)
    except FileNotFoundError:
        print("data.csv not found at", data_path.resolve())
        return
    if 'lookup' not in df.columns:
        print("Column 'lookup' not found in data.csv")
        return
    symbols = df['lookup'].dropna().unique().tolist()
    results = {}
    for symbol in symbols:
        info = fetch_symbol_info(symbol)
        results[symbol] = info
        time.sleep(random.uniform(2, 2.5))
    output_rows = []
    for symbol in df['lookup'].tolist():
        info = results.get(symbol, {})
        output_rows.append({
            'lookup': symbol,
            'currentPrice': info.get('currentPrice') if info.get('currentPrice') is not None else '',
            'marketCap': info.get('marketCap') if info.get('marketCap') is not None else '',
            'industry': info.get('industry') or '',
            'sector': info.get('sector') or ''
        })
    out_df = pd.DataFrame(output_rows)
    output_path = script_dir / 'output.csv'
    out_df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Wrote output to {output_path.resolve()}")

if __name__ == '__main__':
    main()