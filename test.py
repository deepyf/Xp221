import csv
import time
import random
import yfinance as yf
from curl_cffi import requests

def rotate_user_agent():
    agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0"
    ]
    return {'User-Agent': random.choice(agents)}

def fetch_data(symbol, attempt):
    delay = 2 + (attempt-1)*2 + random.uniform(0,0.5)
    time.sleep(delay)
    headers = rotate_user_agent()
    session = requests.Session(impersonate="chrome")
    session.headers.update(headers)
    ticker = yf.Ticker(symbol, session=session)
    try:
        info = ticker.info
        return {
            'currentPrice': info.get('currentPrice', ''),
            'bid': info.get('bid', ''),
            'ask': info.get('ask', ''),
            'targetMeanPrice': info.get('targetMeanPrice', ''),
            'numberOfAnalystOpinions': info.get('numberOfAnalystOpinions', ''),
            'marketCap': info.get('marketCap', ''),
            'industry': info.get('industry', ''),
            'sector': info.get('sector', '')
        }
    except Exception:
        return None

def main():
    with open('data.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    clean_rows = []
    unclean_rows = []
    fieldnames = ['T', 'P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']

    for row in rows:
        data = None
        for attempt in range(1, 5):
            data = fetch_data(row['T'], attempt)
            if data is not None:
                break
        
        if data:
            row['P'] = row['P'] or str(data.get('currentPrice', ''))
            row['B'] = row['B'] or str(data.get('bid', ''))
            row['A'] = row['A'] or str(data.get('ask', ''))
            row['M'] = row['M'] or str(data.get('targetMeanPrice', ''))
            row['O'] = row['O'] or str(data.get('numberOfAnalystOpinions', ''))
            row['C'] = row['C'] or str(data.get('marketCap', ''))
            row['I'] = row['I'] or data.get('industry', '')
            row['S'] = row['S'] or data.get('sector', '')

        if any(row[k] == '' for k in ['P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']):
            unclean_rows.append(row)
        else:
            clean_rows.append(row)

    with open('yahooClean.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)
    
    with open('unClean.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unclean_rows)

if __name__ == '__main__':
    main()