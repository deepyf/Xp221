import csv,random,time,requests,yfinance as yf,pandas as pd
from itertools import islice

ua_list=[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

session=requests.Session()
def batcher(iterable,n):
    it=iter(iterable)
    while True:
        batch=list(islice(it,n))
        if not batch:break
        yield batch

symbols,rows=[],[]
with open("data.csv",encoding="utf-8") as f:
    reader=csv.DictReader(f)
    for r in reader:
        symbols.append(r["T"])
        rows.append(r)

initial_delay= random.uniform(2,2.5)
clean,unclean=[],[]
for batch in batcher(symbols,50):
    session.headers={"User-Agent":random.choice(ua_list)}
    for attempt in range(4):
        try:
            tk= yf.Tickers(" ".join(batch))
            break
        except Exception:
            time.sleep(initial_delay*(2**attempt))
    for sym in batch:
        d= tk.tickers.get(sym)
        info=d.info if d else {}
        out={k:("" if not info.get(k) else info.get(k)) for k in ("currentPrice","bid","ask","targetMeanPrice","numberOfAnalystOpinions","marketCap","industry","sector")}
        for r in rows:
            if r["T"]==sym:
                for col,field in zip(["P","B","A","M","O","C","I","S"],out):
                    if not r[col]:r[col]= out[field] if isinstance(out[field],str) else ("" if out[field] is None else out[field])
                rec=[r[c] or "" for c in ["T","P","B","A","M","O","C","I","S"]]
                if all(r[c] for c in ["P","B","A","M","O","C","I","S"]):
                    clean.append(rec)
                else:
                    unclean.append(rec)
                break
    time.sleep(random.uniform(2,2.5))

hdr=["T","P","B","A","M","O","C","I","S"]
pd.DataFrame(clean,columns=hdr).to_csv("yahooClean.csv",index=False,encoding="utf-8")
pd.DataFrame(unclean,columns=hdr).to_csv("unClean.csv",index=False,encoding="utf-8")