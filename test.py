import pandas as pd
import yfinance as yf
import time
import random
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def get_session():
    session = requests.Session()
    retry = Retry(
        total=5,
        read=5,
        connect=5,
        backoff_factor=0.3,
        status_forcelist=(500, 502, 503, 504),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_user_agent():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    ]
    return random.choice(user_agents)

def process_data():
    try:
        data = pd.read_csv('data.csv', keep_default_na=False)
    except FileNotFoundError:
        print("Error: data.csv not found.")
        return

    headers = ['T', 'P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']
    clean_rows = []
    unclean_rows = []
    session = get_session()

    for _, row in data.iterrows():
        ticker_symbol = row['T']
        if not ticker_symbol:
            unclean_rows.append(row.tolist())
            continue

        try:
            session.headers.update({'User-Agent': get_user_agent()})
            ticker = yf.Ticker(ticker_symbol, session=session)
            info = ticker.info

            current_price = info.get('currentPrice', '')
            bid = info.get('bid', '')
            ask = info.get('ask', '')
            target_mean_price = info.get('targetMeanPrice', '')
            num_analyst_opinions = info.get('numberOfAnalystOpinions', '')
            market_cap = info.get('marketCap', '')
            industry = info.get('industry', '')
            sector = info.get('sector', '')

            row['P'] = row['P'] if row['P'] != '' else current_price
            row['B'] = row['B'] if row['B'] != '' else bid
            row['A'] = row['A'] if row['A'] != '' else ask
            row['M'] = row['M'] if row['M'] != '' else target_mean_price
            row['O'] = row['O'] if row['O'] != '' else num_analyst_opinions
            row['C'] = row['C'] if row['C'] != '' else market_cap
            row['I'] = row['I'] if row['I'] != '' else industry
            row['S'] = row['S'] if row['S'] != '' else sector

            if all(row[h] != '' for h in headers):
                clean_rows.append(row.tolist())
            else:
                unclean_rows.append(row.tolist())

            time.sleep(random.uniform(2, 2.5))

        except Exception as e:
            print(f"Error processing {ticker_symbol}: {e}")
            unclean_rows.append(row.tolist())
            initial_delay = 5
            for i in range(3):
                time.sleep(initial_delay * (2**i))
                try:
                    session.headers.update({'User-Agent': get_user_agent()})
                    ticker = yf.Ticker(ticker_symbol, session=session)
                    break
                except Exception:
                    continue


    pd.DataFrame(clean_rows, columns=headers).to_csv('yahooClean.csv', index=False, encoding='utf-8')
    pd.DataFrame(unclean_rows, columns=headers).to_csv('unClean.csv', index=False, encoding='utf-8')

if __name__ == "__main__":
    process_data()
