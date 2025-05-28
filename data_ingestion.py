import requests
import json
import os
from dotenv import load_dotenv
from mongodb_setup import get_mongo_client, DATABASE_NAME # Import from your mongodb_setup.py

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
BASE_URL = "https://www.alphavantage.co/query"

def fetch_daily_adjusted_data(symbol):
    """Fetches daily adjusted stock data for a given symbol from Alpha Vantage."""
    if not ALPHA_VANTAGE_API_KEY:
        print("Error: API Key not found. Please set ALPHA_VANTAGE_API_KEY in your .env file.")
        return None

    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
        "outputsize": "full" # Get up to 20 years of historical data
    }
    try:
        print(f"Fetching data for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
        data = response.json()

        if "Error Message" in data:
            print(f"Alpha Vantage Error for {symbol}: {data['Error Message']}")
            return None
        if "Time Series (Daily)" not in data:
            print(f"No daily time series data found for {symbol}.")
            return None

        print(f"Successfully fetched data for {symbol}.")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON response for {symbol}: {e}")
        print(f"Raw response: {response.text[:500]}...") # Print first 500 chars of response
        return None

def insert_stock_info(db, symbol, company_name=None, industry=None, sector=None):
    """Inserts or updates basic stock information into the 'stocks' collection."""
    stocks_collection = db["stocks"]
    stock_document = {
        "symbol": symbol.upper(),
        "company_name": company_name,
        "industry": industry,
        "sector": sector
    }
    # Use update_one with upsert=True to insert if not exists, or update if exists
    stocks_collection.update_one(
        {"symbol": symbol.upper()},
        {"$set": stock_document},
        upsert=True
    )
    print(f"Inserted/updated stock info for {symbol}.")

def insert_daily_prices_mongo(db, symbol, daily_data):
    """Inserts daily price data for a symbol into the 'daily_prices' collection."""
    daily_prices_collection = db["daily_prices"]
    prices_to_insert = []
    for date, values in daily_data.items():
        # Alpha Vantage adjusted close is '5. adjusted close'
        price_document = {
            "symbol": symbol.upper(),
            "trade_date": date, # Store date as string for simplicity, or convert to datetime object
            "open_price": float(values['1. open']),
            "high_price": float(values['2. high']),
            "low_price": float(values['3. low']),
            "close_price": float(values['5. adjusted close']), # Use adjusted close for consistency
            "volume": int(values['6. volume'])
        }
        prices_to_insert.append(price_document)

    # Use bulk write operation for efficiency
    # For each document, we want to insert if unique (symbol, trade_date) or replace if exists
    operations = []
    for doc in prices_to_insert:
        operations.append(
            # Replace document if it exists, otherwise insert it
            # The upsert=True is implicitly handled by replace_one if filter matches
            # For unique (symbol, trade_date) an upsert with filter and update would be better.
            # Here, we use replace_one to make sure we always have the latest data for that day
            # If we were doing batch inserts where we know they are new, insert_many is faster.
            # For updating existing and inserting new, update_many with upsert is common.
            # Given we have a unique index on symbol+trade_date, a simple insert_one/many
            # might fail on duplicates. update_one with upsert is robust for individual docs.
            # Let's optimize for bulk operations with replace_one.
            # A more efficient approach for large datasets might involve checking for existence
            # before inserting, or using MongoDB's bulk upsert capabilities more precisely.
            # For this example, we'll do individual updates for clarity, but a true bulk
            # upsert involves more complex array filters or specific BulkWrite operations.

            # Let's use insert_many and handle duplicates via the unique index settings.
            # If a duplicate key error occurs, it means the record already exists.
            # We can either catch this or adjust strategy. For simplicity, we'll just
            # try to insert and let the index handle uniqueness, knowing some might fail.
            # A common pattern is to use `bulk_write` with `ReplaceOne` or `UpdateOne` operations.
            # Example:
            # ReplaceOne({'symbol': doc['symbol'], 'trade_date': doc['trade_date']}, doc, upsert=True)
            # But for simplicity, we'll do insert_many and let the index error be noted.

            # A safer general approach is update_one with upsert=True for each document
            daily_prices_collection.update_one(
                {"symbol": doc["symbol"], "trade_date": doc["trade_date"]},
                {"$set": doc},
                upsert=True
            )
        )
    print(f"Inserted/updated {len(operations)} daily price records for {symbol}.")


if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        db = client[DATABASE_NAME]

        # Example usage: Fetch and store data for a few symbols
        symbols_to_fetch = ["IBM", "MSFT", "GOOGL"]

        for symbol in symbols_to_fetch:
            # Basic stock info insertion (you might fetch this from a different API later)
            # For now, just using placeholder info
            insert_stock_info(db, symbol, company_name=f"{symbol} Corporation", industry="Technology", sector="Information Technology")

            data = fetch_daily_adjusted_data(symbol)
            if data and "Time Series (Daily)" in data:
                insert_daily_prices_mongo(db, symbol, data["Time Series (Daily)"])

        client.close()
        print("Data ingestion process complete.")