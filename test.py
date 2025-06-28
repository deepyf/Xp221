import csv
import time
import random
import itertools
import yfinance as yf
import requests
from requests.adapters import HTTPAdapter, Retry

def get_session(user_agents, backoff_factor):
    session = requests.Session()
    ua = random.choice(user_agents)
    session.headers.update({'User-Agent': ua})
    retries = Retry(
        total=3,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504]
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
    'Mozilla/5.0 (X11; Linux x86_64)',
]

INPUT_FILE = 'data.csv'
UNCLEAN_FILE = 'unClean.csv'
CLEAN_FILE = 'yahooClean.csv'

with open(INPUT_FILE, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    rows = list(reader)

clean_rows = []
unclean_rows = []

for row in rows:
    symbol = row['T']
    session = get_session(USER_AGENTS, backoff_factor=1)
    ticker = yf.Ticker(symbol, session=session)
    delay = random.uniform(2, 2.5)
    time.sleep(delay)
    info = ticker.info or {}

    current = info.get('currentPrice', '')
    bid = info.get('bid', '')
    ask = info.get('ask', '')
    target = info.get('targetMeanPrice', '')
    opinions = info.get('numberOfAnalystOpinions', '')
    mcap = info.get('marketCap', '')
    industry = info.get('industry', '')
    sector = info.get('sector', '')

    row['P'] = row['P'] or current
    row['B'] = row['B'] or bid
    row['A'] = row['A'] or ask
    row['M'] = row['M'] or target
    row['O'] = row['O'] or opinions
    row['C'] = row['C'] or mcap
    row['I'] = row['I'] or industry
    row['S'] = row['S'] or sector

    if all(row[h] != '' for h in ['P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']):
        clean_rows.append(row)
    else:
        unclean_rows.append(row)

with open(CLEAN_FILE, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    writer.writerows(clean_rows)

with open(UNCLEAN_FILE, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=['T','P','B','A','M','O','C','I','S'])
    writer.writeheader()
    writer.writerows(unclean_rows)

requirements.txt

yfinance
requests

.github/workflows/main.yml

name: Python CI

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.x'
    - name: Install dependencies
      run: |
        pip install --upgrade pip
        pip install -r requirements.txt
    - name: Run test script
      run: |
        python test.py
    - name: Upload clean CSV
      uses: actions/upload-artifact@v4
      with:
        name: yahooClean
        path: yahooClean.csv
    - name: Upload unclean CSV
      uses: actions/upload-artifact@v4
      with:
        name: unClean
        path: unClean.csv

