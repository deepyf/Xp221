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

def fetch_data(symbol):
    try:
        headers = rotate_user_agent()
        session = requests.Session(impersonate="chrome")
        session.headers.update(headers)
        ticker = yf.Ticker(symbol, session=session)
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
    last_call_time = 0
    with open('data.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    clean_rows = []
    unclean_rows = []
    fieldnames = ['T', 'P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']

    for row in rows:
        update_needed = False
        if row['P'] == '' or row['M'] == '' or row['O'] == '' or row['C'] == '' or row['I'] == '' or row['S'] == '':
            update_needed = True
        if row['B'] not in ['', '0'] and row['A'] not in ['', '0']:
            update_needed = True
            
        data = None
        if update_needed:
            for attempt in range(1, 5):
                required_delay = 2 * attempt + random.uniform(0, 0.5)
                now = time.time()
                time_to_wait = max(0, last_call_time + required_delay - now)
                time.sleep(time_to_wait)
                
                start_time = time.time()
                data = fetch_data(row['T'])
                last_call_time = start_time
                
                if data is not None:
                    if data['currentPrice'] != '':
                        row['P'] = str(data['currentPrice'])
                    if row['B'] not in ['', '0'] and row['A'] not in ['', '0']:
                        row['B'] = str(data['bid'])
                        row['A'] = str(data['ask'])
                    if data['targetMeanPrice'] != '':
                        row['M'] = str(data['targetMeanPrice'])
                    if data['numberOfAnalystOpinions'] != '':
                        row['O'] = str(data['numberOfAnalystOpinions'])
                    if data['marketCap'] != '':
                        row['C'] = str(data['marketCap'])
                    if data['industry'] != '':
                        row['I'] = data['industry']
                    if data['sector'] != '':
                        row['S'] = data['sector']
                    break

        if any(row[field] == '' for field in ['P', 'B', 'A', 'C', 'I', 'S']):
            unclean_rows.append(row)
        else:
            clean_rows.append(row)

    with open('yahooClean.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)
    
    with open('todo.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unclean_rows)

if __name__ == '__main__':
    main()