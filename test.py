import csv
import time
import random
import requests
import yfinance as yf
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

headers_list = [
    {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15'},
    {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'},
    {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Mobile/15E148 Safari/604.1'}
]

session = requests.Session()
retry_strategy = Retry(
    total=5,
    backoff_factor=0.7,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_data(symbol):
    for attempt in range(5):
        try:
            session.headers = random.choice(headers_list)
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            current_price = info.get('currentPrice', "")
            market_cap = info.get('marketCap', "")
            industry = info.get('industry', "")
            sector = info.get('sector', "")
            return {
                'currentPrice': current_price,
                'marketCap': market_cap,
                'industry': industry if industry else "",
                'sector': sector if sector else ""
            }
        except Exception:
            time.sleep((1.5 ** attempt) + random.uniform(0.5, 1.0))
    return None

symbols = []
with open('data.csv', mode='r', encoding='utf-8') as file:
    reader = csv.DictReader(file)
    for row in reader:
        symbols.append(row['lookup'])

results = []
for symbol in symbols:
    data = fetch_data(symbol)
    if data is not None:
        results.append({
            'lookup': symbol,
            'currentPrice': data['currentPrice'],
            'marketCap': data['marketCap'],
            'industry': data['industry'],
            'sector': data['sector']
        })
    time.sleep(random.uniform(2.0, 2.5))

fieldnames = ['lookup', 'currentPrice', 'marketCap', 'industry', 'sector']
with open('out.csv', mode='w', encoding='utf-8', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)