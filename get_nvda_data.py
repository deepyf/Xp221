import yfinance as yf
import csv
import os

def get_stock_info():
    """
    Fetches NVDA stock information and writes it to x.csv.
    """
    try:
        # Get the ticker data for NVDA
        nvda = yf.Ticker("NVDA")
        info = nvda.info

        # Define the headers and the data to be written
        headers = ['symbol', 'currentPrice', 'marketCap', 'industry', 'sector']
        data = {
            'symbol': info.get('symbol'),
            'currentPrice': info.get('currentPrice'),
            'marketCap': info.get('marketCap'),
            'industry': info.get('industry'),
            'sector': info.get('sector')
        }

        # Specify the filename
        filename = 'x.csv'

        # Write data to CSV
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerow(data)
        
        print(f"Data for NVDA written to {filename}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    get_stock_info()
