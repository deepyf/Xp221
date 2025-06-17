import yfinance as yf
import pandas as pd
import time
import random
import requests

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
]

def retry(func, retries=3, delay=5, backoff=2):
    def wrapper(*args, **kwargs):
        for i in range(retries):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print(f"Attempt {i + 1} failed for {args[0]}: {e}")
                if i < retries - 1:
                    time.sleep(delay)
                    delay *= backoff
                else:
                    return None
    return wrapper

@retry
def get_stock_data(symbol, session):
    ticker = yf.Ticker(symbol, session=session)
    info = ticker.info
    return info

def main():
    try:
        df = pd.read_csv('data.csv')
    except FileNotFoundError:
        print("Error: data.csv not found.")
        return

    symbols = df['lookup'].tolist()
    results = []

    with requests.Session() as session:
        for symbol in symbols:
            session.headers.update({'User-Agent': random.choice(USER_AGENTS)})

            print(f"Fetching data for {symbol}...")
            info = get_stock_data(symbol, session)

            if info:
                data = {
                    'lookup': symbol,
                    'currentPrice': info.get('currentPrice', ""),
                    'marketCap': info.get('marketCap', ""),
                    'industry': info.get('industry', ""),
                    'sector': info.get('sector', "")
                }
                results.append(data)
            else:
                print(f"Failed to fetch data for {symbol} after multiple retries.")
                results.append({
                    'lookup': symbol,
                    'currentPrice': "",
                    'marketCap': "",
                    'industry': "",
                    'sector': ""
                })
            
            time.sleep(random.uniform(2, 2.5))

    output_df = pd.DataFrame(results)
    output_df.to_csv('out.csv', index=False, encoding='utf-8')
    print("Data fetching complete. Output saved to out.csv.")

if __name__ == "__main__":
    main()
