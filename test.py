import yfinance as yf
from functools import wraps

# --- Helper function to cache stock data ---
# This part of the logic remains the same.
_stock_data_cache = {}

def cache_stock_data(func):
    """Decorator to cache stock data."""
    @wraps(func)
    def wrapper(symbol, *args, **kwargs):
        if symbol in _stock_data_cache:
            print(f"Returning cached data for {symbol}...")
            return _stock_data_cache[symbol]
        else:
            print(f"Fetching data for {symbol}...")
            # The session object is no longer passed here
            result = func(symbol, *args, **kwargs)
            _stock_data_cache[symbol] = result
            return result
    return wrapper

# --- Main data fetching function ---
@cache_stock_data
def get_stock_data(symbol):
    """
    Fetches stock data for a given symbol.
    The session parameter has been removed.
    """
    try:
        # Let yfinance handle its own session management.
        # Do NOT pass a session object here.
        ticker = yf.Ticker(symbol)
        
        # Attempt to get info, which is a common point of failure
        info = ticker.info
        if not info or 'symbol' not in info:
            print(f"Could not retrieve valid data for {symbol}. It may be delisted or an incorrect ticker.")
            return None
        return info
    except Exception as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return None

# --- Main execution block ---
def main():
    """Main function to run the script."""
    # List of stock symbols to fetch
    symbols = ["MSFT", "AAPL", "GOOGL", "QDT.PA", "BTC-USD"]

    # The requests.Session object is no longer needed for yfinance
    # session = requests.Session() # This line is removed

    for symbol in symbols:
        # The session object is no longer passed to the function
        info = get_stock_data(symbol)
        
        if info:
            short_name = info.get('shortName', 'N/A')
            market_cap = info.get('marketCap', 'N/A')
            print(f"--- Data for {symbol} ---")
            print(f"  Name: {short_name}")
            print(f"  Market Cap: {market_cap}")
            print("-" * 25)
        else:
            print(f"Could not retrieve information for {symbol}.")
            print("-" * 25)

if __name__ == "__main__":
    main()

