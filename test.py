import csv
import time
import random
import yfinance as yf
import pandas as pd
import requests

ua_list = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36'
]

def fetch_data(symbol, attempt):
    session = requests.Session()
    session.headers.update({'User-Agent': random.choice(ua_list)})
    ticker = yf.Ticker(symbol, session=session)
    try:
        info = ticker.info
        return {
            'currentPrice': info.get('currentPrice', ''),
            'lastPrice': info.get('previousClose', ''),
            'targetMeanPrice': info.get('targetMeanPrice', ''),
            'numberOfAnalystOpinions': info.get('numberOfAnalystOpinions', ''),
            'marketCap': info.get('marketCap', ''),
            'industry': info.get('industry', ''),
            'sector': info.get('sector', '')
        }
    except:
        return None

def process_row(row):
    attempt = 1
    while attempt <= 4:
        data = fetch_data(row['T'], attempt)
        if data:
            return data
        sleep_time = random.uniform(2 + 2*(attempt-1), 2.5 + 2*(attempt-1))
        time.sleep(sleep_time)
        attempt += 1
    return {}

def main():
    with open('data.csv', 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        todo_rows = []
        clean_rows = []
        fieldnames = reader.fieldnames
        
        for row in reader:
            data = process_row(row)
            
            if data.get('currentPrice', '') != '':
                row['P'] = data['currentPrice']
            if data.get('lastPrice', '') != '':
                row['PL'] = data['lastPrice']
            if data.get('targetMeanPrice', '') != '':
                row['M'] = data['targetMeanPrice']
            if data.get('numberOfAnalystOpinions', '') != '':
                row['O'] = data['numberOfAnalystOpinions']
            if data.get('marketCap', '') != '':
                row['C'] = data['marketCap']
            if data.get('industry', '') != '':
                row['I'] = data['industry']
            if data.get('sector', '') != '':
                row['S'] = data['sector']
            
            if '' in [row['P'], row['PL'], row['C'], row['I'], row['S']]:
                todo_rows.append(row)
            else:
                clean_rows.append(row)
            
            time.sleep(random.uniform(2, 2.5))
        
        with open('todo.csv', 'w', encoding='utf-8', newline='') as todo_file:
            writer = csv.DictWriter(todo_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(todo_rows)
        
        with open('yahooClean.csv', 'w', encoding='utf-8', newline='') as clean_file:
            writer = csv.DictWriter(clean_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(clean_rows)

if __name__ == "__main__":
    main()