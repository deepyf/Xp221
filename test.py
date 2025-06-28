import pandas as pd
import yfinance as yf
import time
import random
import requests

# List of user agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Mobile Safari/537.36',
]

# Create a session for requests
session = requests.Session()

def fetch_info(symbol, session, max_retries=3):
    attempt = 0
    while attempt < max_retries:
        try:
            # Rotate user agent for each request
            user_agent = random.choice(USER_AGENTS)
            session.headers.update({'User-Agent': user_agent})
            
            ticker = yf.Ticker(symbol, session=session)
            info = ticker.info
            return info
        except Exception as e:
            attempt += 1
            if attempt < max_retries:
                sleep_time = random.uniform(2 + 2*attempt, 2.5 + 2*attempt)
                time.sleep(sleep_time)
            else:
                print(f"Failed to fetch info for {symbol} after {max_retries} attempts.")
                return None

# Load data from CSV
df = pd.read_csv('data.csv', dtype=str, keep_default_na=False)
clean_data = []
unclean_data = []

# Process each row
for _, row in df.iterrows():
    symbol = row['T']
    info = fetch_info(symbol, session)
    if info is None:
        fetched = {'currentPrice': '', 'bid': '', 'ask': '', 'targetMeanPrice': '', 'numberOfAnalystOpinions': '', 'marketCap': '', 'industry': '', 'sector': ''}
    else:
        fetched = {
            'currentPrice': str(info.get('currentPrice', '')),
            'bid': str(info.get('bid', '')),
            'ask': str(info.get('ask', '')),
            'targetMeanPrice': str(info.get('targetMeanPrice', '')),
            'numberOfAnalystOpinions': str(info.get('numberOfAnalystOpinions', '')),
            'marketCap': str(info.get('marketCap', '')),
            'industry': info.get('industry', ''),
            'sector': info.get('sector', ''),
        }
    new_row = row.copy()
    for col, key in zip(['P', 'B', 'A', 'M', 'O', 'C', 'I', 'S'], ['currentPrice', 'bid', 'ask', 'targetMeanPrice', 'numberOfAnalystOpinions', 'marketCap', 'industry', 'sector']):
        if new_row[col] == '':
            new_row[col] = fetched[key]
    if all(new_row[col] != '' for col in ['P', 'B', 'A', 'M', 'O', 'C', 'I', 'S']):
        clean_data.append(new_row.to_dict())
    else:
        unclean_data.append(new_row.to_dict())
    time.sleep(random.uniform(2, 2.5))

# Save results to CSV files
clean_df = pd.DataFrame(clean_data, columns=df.columns)
unclean_df = pd.DataFrame(unclean_data, columns=df.columns)
clean_df.to_csv('yahooClean.csv', index=False, encoding='utf-8')
unclean_df.to_csv('unClean.csv', index=False, encoding='utf-8')