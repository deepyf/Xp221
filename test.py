import csv
import time
import random
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter, Retry

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Mozilla/5.0 (X11; Linux x86_64)',
]

INPUT_FILE = 'data.csv'
UNCLEAN_FILE = 'unClean.csv'
CLEAN_FILE = 'yahooClean.csv'

session = requests.Session()
adapter = HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1, status_forcelist=[429,500,502,503,504]))
session.mount('https://', adapter)

with open(INPUT_FILE, newline='', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))
clean_rows = []
unclean_rows = []

for row in rows:
    symbol = row['T']
    session.headers.update({'User-Agent': random.choice(USER_AGENTS)})
    ticker = yf.Ticker(symbol, session=session)
    time.sleep(random.uniform(2,2.5))
    info = ticker.info or {}
    row['P'] = row['P'] or info.get('currentPrice','')
    row['B'] = row['B'] or info.get('bid','')
    row['A'] = row['A'] or info.get('ask','')
    row['M'] = row['M'] or info.get('targetMeanPrice','')
    row['O'] = row['O'] or info.get('numberOfAnalystOpinions','')
    row['C'] = row['C'] or info.get('marketCap','')
    row['I'] = row['I'] or info.get('industry','')
    row['S'] = row['S'] or info.get('sector','')
    if all(row[h] != '' for h in ['P','B','A','M','O','C','I','S']):
        clean_rows.append(row)
    else:
        unclean_rows.append(row)

for fname, data in [(CLEAN_FILE, clean_rows), (UNCLEAN_FILE, unclean_rows)]:
    with open(fname, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['T','P','B','A','M','O','C','I','S'])
        writer.writeheader()
        writer.writerows(data)