import pandas as pd
import yfinance as yf
import time
import random
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def get_stock_info(symbol, session):
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)

    try:
        ticker = yf.Ticker(symbol, session=session)
        info = ticker.info
        
        current_price = info.get('currentPrice', '')
        market_cap = info.get('marketCap', '')
        industry = info.get('industry', '')
        sector = info.get('sector', '')
        
        return {
            'lookup': symbol,
            'currentPrice': current_price,
            'marketCap': market_cap,
            'industry': industry,
            'sector': sector
        }
    except Exception:
        return {
            'lookup': symbol,
            'currentPrice': '',
            'marketCap': '',
            'industry': '',
            'sector': ''
        }

def main():
    headers_list = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
        },
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8'
        }
    ]

    try:
        symbols_df = pd.read_csv('data.csv')
        symbols = symbols_df['lookup'].tolist()
    except FileNotFoundError:
        print("Error: data.csv not found.")
        return

    all_stock_data = []
    
    with requests.Session() as session:
        for symbol in symbols:
            session.headers.update(random.choice(headers_list))
            stock_data = get_stock_info(symbol, session)
            all_stock_data.append(stock_data)
            time.sleep(random.uniform(2, 2.5))

    output_df = pd.DataFrame(all_stock_data)
    output_df = output_df[['lookup', 'currentPrice', 'marketCap', 'industry', 'sector']]
    output_df.to_csv('output.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    main()
