import pandas as pd
import yfinance as yf
import time, random
from requests import Session
from itertools import cycle

def get_session():
    headers_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Mozilla/5.0 (X11; Linux x86_64)'
    ]
    session = Session()
    session.headers.update({'User-Agent': random.choice(headers_list)})
    return session, cycle(headers_list)

session, ua_cycle = get_session()

def fetch_info(symbol):
    attempt = 0
    delays = [ (2,2.5), (4,4.5), (6,6.5), (8,8.5) ]
    while attempt < len(delays):
        try:
            session.headers.update({'User-Agent': next(ua_cycle)})
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
            low, high = delays[attempt]
            time.sleep(random.uniform(low, high))
            attempt += 1
    return {}

def main():
    df = pd.read_csv('data.csv', dtype=str).fillna('')
    cache = {}
    clean_rows, unclean_rows = [], []
    for _, row in df.iterrows():
        sym = row['T']
        if sym not in cache:
            cache[sym] = fetch_info(sym)
            time.sleep(random.uniform(2,2.5))
        info = cache.get(sym, {})
        out = {k: row[k] or info.get(field, '') for k, field in zip(
            ['T','P','B','A','M','O','C','I','S'],
            ['T','currentPrice','bid','ask','targetMeanPrice','numberOfAnalystOpinions','marketCap','industry','sector']
        )}
        if '' in [out['P'], out['B'], out['A'], out['M'], out['O'], out['C'], out['I'], out['S']]:
            unclean_rows.append(out)
        else:
            clean_rows.append(out)
    pd.DataFrame(clean_rows, columns=['T','P','B','A','M','O','C','I','S']).to_csv('yahooClean.csv', index=False, encoding='utf-8')
    pd.DataFrame(unclean_rows, columns=['T','P','B','A','M','O','C','I','S']).to_csv('unClean.csv', index=False, encoding='utf-8')

if __name__ == '__main__':
    main()