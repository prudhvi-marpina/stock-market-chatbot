import requests
import json
import os
import time
import logging
import csv
from io import StringIO
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from mongodb_setup import get_mongo_client, DATABASE_NAME # Import from your mongodb_setup.py

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('stock_data_ingestion.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
if not ALPHA_VANTAGE_API_KEY:
    raise ValueError("ALPHA_VANTAGE_API_KEY not found in environment variables")

BASE_URL = "https://www.alphavantage.co/query"

# Rate limiting configuration
CALLS_PER_MINUTE = 5  # Free tier limit
DELAY_BETWEEN_CALLS = 60 / CALLS_PER_MINUTE  # Seconds between calls
BATCH_SIZE = 5  # Number of symbols to process in each batch

# Default symbols to process if API fails
DEFAULT_SYMBOLS = [
    # Technology
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC", "CRM",
    # Finance
    "JPM", "BAC", "WFC", "GS", "MS", "V", "MA", "AXP", "BLK", "C",
    # Healthcare
    "JNJ", "PFE", "UNH", "MRK", "ABT", "TMO", "DHR", "BMY", "AMGN", "LLY",
    # Consumer
    "PG", "KO", "PEP", "WMT", "MCD", "DIS", "NKE", "SBUX", "HD", "TGT",
    # Industrial
    "GE", "BA", "CAT", "MMM", "HON", "UPS", "FDX", "DE", "LMT", "RTX",
    # Energy
    "XOM", "CVX", "COP", "SLB", "EOG", "MPC", "PSX", "VLO", "OXY", "KMI",
    # Telecommunications
    "T", "VZ", "TMUS", "CMCSA", "CHTR", "ATUS", "BCE", "RCI", "AMX", "TEF",
    # Real Estate
    "AMT", "PLD", "CCI", "EQIX", "PSA", "O", "WELL", "SPG", "DLR", "SBAC",
    # Materials
    "LIN", "APD", "ECL", "SHW", "NEM", "FCX", "DOW", "NUE", "VMC", "MLM",
    # Utilities
    "NEE", "DUK", "SO", "D", "AEP", "EXC", "SRE", "XEL", "WEC", "ES"
]

# List of major stock exchanges
EXCHANGES = {
    'NYSE': 'New York Stock Exchange',
    'NASDAQ': 'NASDAQ Stock Market',
    'AMEX': 'American Stock Exchange'
}

def get_all_symbols() -> List[str]:
    """
    Fetches list of all available stock symbols from multiple sources.
    Returns a list of stock symbols.
    """
    symbols = set()  # Use set to avoid duplicates
    
    try:
        # First try: Alpha Vantage Listing Status API
        logger.info("Attempting to fetch symbols from Alpha Vantage Listing Status API...")
        listing_url = f"{BASE_URL}?function=LISTING_STATUS&apikey={ALPHA_VANTAGE_API_KEY}"
        response = requests.get(listing_url)
        response.raise_for_status()
        
        # Parse CSV response
        csv_reader = csv.reader(StringIO(response.text))
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            if len(row) >= 3 and row[2].strip() == 'Active':  # Check if stock is active
                symbols.add(row[0].strip())
        
        if symbols:
            logger.info(f"Retrieved {len(symbols)} symbols from Alpha Vantage API")
        else:
            logger.warning("No symbols found in Alpha Vantage API response, using default symbols")
            symbols.update(DEFAULT_SYMBOLS)
            
    except Exception as e:
        logger.warning(f"Failed to fetch symbols from Alpha Vantage API: {str(e)}")
        logger.info("Using default symbols list...")
        symbols.update(DEFAULT_SYMBOLS)
    
    # Always ensure we have at least the default symbols
    symbols.update(DEFAULT_SYMBOLS)
    
    # Convert set to sorted list
    symbol_list = sorted(list(symbols))
    
    if not symbol_list:
        logger.warning("No symbols retrieved from any source, falling back to default symbols only")
        return DEFAULT_SYMBOLS
    
    logger.info(f"Final symbol list contains {len(symbol_list)} unique symbols")
    return symbol_list

def load_processed_symbols(db) -> set:
    """
    Loads the list of already processed symbols from MongoDB.
    """
    try:
        processed = set()
        # Check all collections for existing symbols
        collections = [
            "daily_prices", "company_overview", "financial_statements",
            "earnings", "news_sentiment", "time_series", "real_time_quotes",
            "company_ratings", "social_sentiment", "sec_filings"
        ]
        
        for collection_name in collections:
            collection = db[collection_name]
            symbols = collection.distinct("symbol")
            processed.update(symbols)
        
        return processed
    except Exception as e:
        logger.error(f"Error loading processed symbols: {str(e)}")
        return set()

def save_progress(db, symbol: str, status: str):
    """
    Saves the processing status of a symbol.
    """
    try:
        progress_collection = db["ingestion_progress"]
        progress_collection.update_one(
            {"symbol": symbol},
            {
                "$set": {
                    "status": status,
                    "last_updated": datetime.now(),
                    "last_status": status
                }
            },
            upsert=True
        )
    except Exception as e:
        logger.error(f"Error saving progress for {symbol}: {str(e)}")

def handle_api_call(func):
    """Decorator to handle API rate limiting and errors."""
    def wrapper(*args, **kwargs):
        try:
            time.sleep(DELAY_BETWEEN_CALLS)  # Rate limiting
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            print(f"API call failed: {e}")
            return None
    return wrapper

@handle_api_call
def fetch_weekly_adjusted(symbol):
    """Fetches weekly adjusted time series data."""
    params = {
        "function": "TIME_SERIES_WEEKLY_ADJUSTED",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

@handle_api_call
def fetch_monthly_adjusted(symbol):
    """Fetches monthly adjusted time series data."""
    params = {
        "function": "TIME_SERIES_MONTHLY_ADJUSTED",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

@handle_api_call
def fetch_global_quote(symbol):
    """Fetches real-time quote."""
    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

@handle_api_call
def fetch_company_rating(symbol):
    """Fetches Fundamental Analysis Score."""
    params = {
        "function": "FUNDAMENTAL_ANALYSIS_SCORE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

@handle_api_call
def fetch_social_sentiment(symbol):
    """Fetches social media sentiment."""
    params = {
        "function": "SOCIAL_SENTIMENT",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

@handle_api_call
def fetch_sec_filings(symbol):
    """Fetches recent SEC filings."""
    params = {
        "function": "SEC_FILINGS",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    return response.json()

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

def fetch_company_overview(symbol):
    """Fetches company overview data from Alpha Vantage."""
    params = {
        "function": "OVERVIEW",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching company overview for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No company overview data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching company overview for {symbol}: {e}")
        return None

def fetch_income_statement(symbol):
    """Fetches annual and quarterly income statements."""
    params = {
        "function": "INCOME_STATEMENT",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching income statements for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No income statement data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching income statements for {symbol}: {e}")
        return None

def fetch_balance_sheet(symbol):
    """Fetches annual and quarterly balance sheets."""
    params = {
        "function": "BALANCE_SHEET",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching balance sheets for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No balance sheet data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching balance sheets for {symbol}: {e}")
        return None

def fetch_cash_flow(symbol):
    """Fetches annual and quarterly cash flow statements."""
    params = {
        "function": "CASH_FLOW",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching cash flow statements for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No cash flow data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching cash flow statements for {symbol}: {e}")
        return None

def fetch_earnings(symbol):
    """Fetches annual and quarterly earnings data."""
    params = {
        "function": "EARNINGS",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching earnings data for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No earnings data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching earnings data for {symbol}: {e}")
        return None

def fetch_news_sentiment(symbol):
    """Fetches news and sentiment data for a symbol."""
    params = {
        "function": "NEWS_SENTIMENT",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY
    }
    try:
        print(f"Fetching news and sentiment data for {symbol}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        if not data or "Error Message" in data:
            print(f"No news and sentiment data found for {symbol}")
            return None
            
        return data
    except Exception as e:
        print(f"Error fetching news and sentiment data for {symbol}: {e}")
        return None

def insert_company_overview(db, symbol, overview_data):
    """Inserts or updates company overview data in MongoDB."""
    if not overview_data:
        return
        
    overview_collection = db["company_overview"]
    overview_data["symbol"] = symbol.upper()
    
    overview_collection.update_one(
        {"symbol": symbol.upper()},
        {"$set": overview_data},
        upsert=True
    )
    print(f"Inserted/updated company overview for {symbol}")

def insert_financial_statements(db, symbol, income_data, balance_data, cash_flow_data):
    """Inserts or updates financial statements in MongoDB."""
    financials_collection = db["financial_statements"]
    
    # Process and store annual reports
    if income_data and "annualReports" in income_data:
        for annual_report in income_data["annualReports"]:
            fiscal_year = annual_report["fiscalDateEnding"]
            
            # Combine data from all statements
            financial_data = {
                "symbol": symbol.upper(),
                "fiscal_date_ending": fiscal_year,
                "report_type": "annual",
                "income_statement": annual_report
            }
            
            # Add balance sheet data if available
            if balance_data and "annualReports" in balance_data:
                matching_balance = next(
                    (report for report in balance_data["annualReports"] 
                     if report["fiscalDateEnding"] == fiscal_year),
                    None
                )
                if matching_balance:
                    financial_data["balance_sheet"] = matching_balance
                    
            # Add cash flow data if available
            if cash_flow_data and "annualReports" in cash_flow_data:
                matching_cash_flow = next(
                    (report for report in cash_flow_data["annualReports"] 
                     if report["fiscalDateEnding"] == fiscal_year),
                    None
                )
                if matching_cash_flow:
                    financial_data["cash_flow"] = matching_cash_flow
                    
            # Insert or update in MongoDB
            financials_collection.update_one(
                {
                    "symbol": symbol.upper(),
                    "fiscal_date_ending": fiscal_year,
                    "report_type": "annual"
                },
                {"$set": financial_data},
                upsert=True
            )
    
    # Process and store quarterly reports similarly
    if income_data and "quarterlyReports" in income_data:
        for quarterly_report in income_data["quarterlyReports"]:
            fiscal_quarter = quarterly_report["fiscalDateEnding"]
            
            financial_data = {
                "symbol": symbol.upper(),
                "fiscal_date_ending": fiscal_quarter,
                "report_type": "quarterly",
                "income_statement": quarterly_report
            }
            
            if balance_data and "quarterlyReports" in balance_data:
                matching_balance = next(
                    (report for report in balance_data["quarterlyReports"] 
                     if report["fiscalDateEnding"] == fiscal_quarter),
                    None
                )
                if matching_balance:
                    financial_data["balance_sheet"] = matching_balance
                    
            if cash_flow_data and "quarterlyReports" in cash_flow_data:
                matching_cash_flow = next(
                    (report for report in cash_flow_data["quarterlyReports"] 
                     if report["fiscalDateEnding"] == fiscal_quarter),
                    None
                )
                if matching_cash_flow:
                    financial_data["cash_flow"] = matching_cash_flow
                    
            financials_collection.update_one(
                {
                    "symbol": symbol.upper(),
                    "fiscal_date_ending": fiscal_quarter,
                    "report_type": "quarterly"
                },
                {"$set": financial_data},
                upsert=True
            )
    
    print(f"Inserted/updated financial statements for {symbol}")

def insert_earnings_data(db, symbol, earnings_data):
    """Inserts or updates earnings data in MongoDB."""
    if not earnings_data:
        return
        
    earnings_collection = db["earnings"]
    
    # Process annual earnings
    if "annualEarnings" in earnings_data:
        for annual_earning in earnings_data["annualEarnings"]:
            annual_earning["symbol"] = symbol.upper()
            annual_earning["report_type"] = "annual"
            
            earnings_collection.update_one(
                {
                    "symbol": symbol.upper(),
                    "fiscalDateEnding": annual_earning["fiscalDateEnding"],
                    "report_type": "annual"
                },
                {"$set": annual_earning},
                upsert=True
            )
    
    # Process quarterly earnings
    if "quarterlyEarnings" in earnings_data:
        for quarterly_earning in earnings_data["quarterlyEarnings"]:
            quarterly_earning["symbol"] = symbol.upper()
            quarterly_earning["report_type"] = "quarterly"
            
            earnings_collection.update_one(
                {
                    "symbol": symbol.upper(),
                    "fiscalDateEnding": quarterly_earning["fiscalDateEnding"],
                    "report_type": "quarterly"
                },
                {"$set": quarterly_earning},
                upsert=True
            )
    
    print(f"Inserted/updated earnings data for {symbol}")

def insert_news_sentiment(db, symbol, news_data):
    """Inserts news and sentiment data into MongoDB."""
    if not news_data:
        return
        
    news_collection = db["news_sentiment"]
    
    if "feed" in news_data:
        for article in news_data["feed"]:
            article["symbol"] = symbol.upper()
            article["time_published"] = article.get("time_published", "")
            
            # Use the article's URL as a unique identifier
            news_collection.update_one(
                {
                    "url": article["url"],
                    "symbol": symbol.upper()
                },
                {"$set": article},
                upsert=True
            )
    
    print(f"Inserted/updated news and sentiment data for {symbol}")

def insert_time_series_data(db, symbol, data, series_type):
    """Inserts time series data (weekly/monthly) into MongoDB."""
    if not data or f"Time Series ({series_type})" not in data:
        return

    time_series_collection = db["time_series"]
    series_data = data[f"Time Series ({series_type})"]

    for date, values in series_data.items():
        document = {
            "symbol": symbol.upper(),
            "date": date,
            "series_type": series_type,
            "open": float(values.get("1. open", 0)),
            "high": float(values.get("2. high", 0)),
            "low": float(values.get("3. low", 0)),
            "close": float(values.get("4. close", 0)),
            "adjusted_close": float(values.get("5. adjusted close", 0)),
            "volume": int(values.get("6. volume", 0)),
            "dividend_amount": float(values.get("7. dividend amount", 0))
        }

        time_series_collection.update_one(
            {
                "symbol": symbol.upper(),
                "date": date,
                "series_type": series_type
            },
            {"$set": document},
            upsert=True
        )
    
    print(f"Inserted/updated {series_type} time series data for {symbol}")

def insert_real_time_quote(db, symbol, quote_data):
    """Inserts real-time quote data into MongoDB."""
    if not quote_data or "Global Quote" not in quote_data:
        return

    quotes_collection = db["real_time_quotes"]
    quote = quote_data["Global Quote"]
    
    document = {
        "symbol": symbol.upper(),
        "timestamp": datetime.now(),
        "price": float(quote.get("05. price", 0)),
        "change": float(quote.get("09. change", 0)),
        "change_percent": quote.get("10. change percent", "0%").rstrip("%"),
        "volume": int(quote.get("06. volume", 0)),
        "latest_trading_day": quote.get("07. latest trading day", ""),
        "previous_close": float(quote.get("08. previous close", 0)),
        "open": float(quote.get("02. open", 0)),
        "high": float(quote.get("03. high", 0)),
        "low": float(quote.get("04. low", 0))
    }

    quotes_collection.insert_one(document)
    print(f"Inserted real-time quote for {symbol}")

def insert_company_rating(db, symbol, rating_data):
    """Inserts company rating data into MongoDB."""
    if not rating_data:
        return

    ratings_collection = db["company_ratings"]
    rating_data["symbol"] = symbol.upper()
    rating_data["timestamp"] = datetime.now()

    ratings_collection.insert_one(rating_data)
    print(f"Inserted company rating for {symbol}")

def insert_social_sentiment(db, symbol, sentiment_data):
    """Inserts social media sentiment data into MongoDB."""
    if not sentiment_data:
        return

    sentiment_collection = db["social_sentiment"]
    
    for platform in sentiment_data.get("data", []):
        sentiment_doc = {
            "symbol": symbol.upper(),
            "timestamp": datetime.now(),
            "platform": platform.get("platform", ""),
            "sentiment_score": float(platform.get("sentiment_score", 0)),
            "sentiment_label": platform.get("sentiment_label", ""),
            "mentions": int(platform.get("mentions", 0))
        }
        
        sentiment_collection.insert_one(sentiment_doc)
    
    print(f"Inserted social sentiment data for {symbol}")

def insert_sec_filings(db, symbol, filings_data):
    """Inserts SEC filings data into MongoDB."""
    if not filings_data or "filings" not in filings_data:
        return

    filings_collection = db["sec_filings"]
    
    for filing in filings_data["filings"]:
        filing["symbol"] = symbol.upper()
        
        filings_collection.update_one(
            {
                "symbol": symbol.upper(),
                "filing_date": filing["filing_date"],
                "form_type": filing["form_type"]
            },
            {"$set": filing},
            upsert=True
        )
    
    print(f"Inserted SEC filings for {symbol}")

def process_symbol(db, symbol: str) -> bool:
    """
    Processes a single symbol, fetching and storing all available data.
    Returns True if successful, False otherwise.
    """
    try:
        logger.info(f"Processing data for {symbol}...")
        save_progress(db, symbol, "processing")
        
        # Fetch company overview
        overview_data = fetch_company_overview(symbol)
        if overview_data:
            insert_company_overview(db, symbol, overview_data)
        
        # Fetch and store financial statements
        income_data = fetch_income_statement(symbol)
        balance_data = fetch_balance_sheet(symbol)
        cash_flow_data = fetch_cash_flow(symbol)
        if any([income_data, balance_data, cash_flow_data]):
            insert_financial_statements(db, symbol, income_data, balance_data, cash_flow_data)
        
        # Fetch and store earnings data
        earnings_data = fetch_earnings(symbol)
        if earnings_data:
            insert_earnings_data(db, symbol, earnings_data)
        
        # Fetch and store news & sentiment data
        news_data = fetch_news_sentiment(symbol)
        if news_data:
            insert_news_sentiment(db, symbol, news_data)
        
        # Fetch and store daily price data
        daily_data = fetch_daily_adjusted_data(symbol)
        if daily_data and "Time Series (Daily)" in daily_data:
            insert_daily_prices_mongo(db, symbol, daily_data["Time Series (Daily)"])

        # Fetch and store weekly data
        weekly_data = fetch_weekly_adjusted(symbol)
        if weekly_data:
            insert_time_series_data(db, symbol, weekly_data, "Weekly")

        # Fetch and store monthly data
        monthly_data = fetch_monthly_adjusted(symbol)
        if monthly_data:
            insert_time_series_data(db, symbol, monthly_data, "Monthly")

        # Fetch and store real-time quote
        quote_data = fetch_global_quote(symbol)
        if quote_data:
            insert_real_time_quote(db, symbol, quote_data)

        # Fetch and store company rating
        rating_data = fetch_company_rating(symbol)
        if rating_data:
            insert_company_rating(db, symbol, rating_data)

        # Fetch and store social sentiment
        sentiment_data = fetch_social_sentiment(symbol)
        if sentiment_data:
            insert_social_sentiment(db, symbol, sentiment_data)

        # Fetch and store SEC filings
        filings_data = fetch_sec_filings(symbol)
        if filings_data:
            insert_sec_filings(db, symbol, filings_data)

        save_progress(db, symbol, "completed")
        logger.info(f"Completed processing for {symbol}")
        return True
        
    except Exception as e:
        logger.error(f"Error processing {symbol}: {str(e)}")
        save_progress(db, symbol, f"error: {str(e)}")
        return False

def process_batch(db, symbols: List[str]) -> None:
    """
    Processes a batch of symbols.
    """
    for symbol in symbols:
        try:
            process_symbol(db, symbol)
        except Exception as e:
            logger.error(f"Failed to process {symbol}: {str(e)}")
        time.sleep(DELAY_BETWEEN_CALLS)  # Respect API rate limits

def main():
    """
    Main function to run the data ingestion process.
    """
    try:
        logger.info("Starting data ingestion process...")
        
        # Connect to MongoDB
    client = get_mongo_client()
        if not client:
            logger.error("Failed to connect to MongoDB")
            return
        
        db = client[DATABASE_NAME]

        # Get all available symbols
        all_symbols = get_all_symbols()
        if not all_symbols:
            logger.error("No symbols retrieved, using default symbol list")
            all_symbols = DEFAULT_SYMBOLS
            
        logger.info(f"Working with {len(all_symbols)} symbols")
        
        # Load already processed symbols
        processed_symbols = load_processed_symbols(db)
        logger.info(f"Found {len(processed_symbols)} already processed symbols")
        
        # Filter out already processed symbols
        symbols_to_process = [s for s in all_symbols if s not in processed_symbols]
        logger.info(f"Will process {len(symbols_to_process)} new symbols")
        
        if not symbols_to_process:
            logger.info("No new symbols to process")
            return
        
        # Process symbols in batches
        total_batches = (len(symbols_to_process) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(0, len(symbols_to_process), BATCH_SIZE):
            batch = symbols_to_process[i:i + BATCH_SIZE]
            current_batch = i // BATCH_SIZE + 1
            logger.info(f"Processing batch {current_batch} of {total_batches} ({len(batch)} symbols)")
            process_batch(db, batch)
            
            # Log progress after each batch
            processed_count = min(i + BATCH_SIZE, len(symbols_to_process))
            progress_percent = (processed_count / len(symbols_to_process)) * 100
            logger.info(f"Progress: {processed_count}/{len(symbols_to_process)} symbols ({progress_percent:.1f}%)")

        client.close()
        logger.info("Data ingestion process complete")
        
    except Exception as e:
        logger.error(f"Fatal error in main process: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    main()