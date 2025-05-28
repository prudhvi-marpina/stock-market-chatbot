from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import ConnectionFailure

# MongoDB connection details
MONGO_URI = "mongodb://localhost:27017/" # Default MongoDB URI
DATABASE_NAME = "stock_data" # Your database name

def get_mongo_client():
    """Attempts to establish a connection to MongoDB."""
    try:
        client = MongoClient(MONGO_URI)
        # The ismaster command is cheap and does not require auth.
        client.admin.command('ismaster')
        print("Successfully connected to MongoDB!")
        return client
    except ConnectionFailure as e:
        print(f"MongoDB connection failed: {e}")
        return None

def setup_collections(db):
    """Ensures necessary collections exist and sets up indexes."""
    # Collections in MongoDB don't strictly need to be "created" beforehand
    # They are created implicitly when you first insert data.
    # However, it's good practice to define them and set up indexes here.

    # For stock information (e.g., company name, industry)
    stocks_collection = db["stocks"]
    # Ensure unique index on symbol for fast lookup and to prevent duplicates
    stocks_collection.create_index("symbol", unique=True)
    print("Collection 'stocks' ready.")

    # For historical daily prices
    daily_prices_collection = db["daily_prices"]
    # Create a compound index for efficient querying by symbol and date
    daily_prices_collection.create_index([("symbol", ASCENDING), ("trade_date", DESCENDING)], unique=True)
    print("Collection 'daily_prices' ready.")

    # For company overview data
    overview_collection = db["company_overview"]
    overview_collection.create_index("symbol", unique=True)
    overview_collection.create_index("industry")
    overview_collection.create_index("sector")
    print("Collection 'company_overview' ready.")

    # For financial statements (income statement, balance sheet, cash flow)
    financials_collection = db["financial_statements"]
    financials_collection.create_index([
        ("symbol", ASCENDING),
        ("fiscal_date_ending", DESCENDING),
        ("report_type", ASCENDING)
    ], unique=True)
    financials_collection.create_index("report_type")
    print("Collection 'financial_statements' ready.")

    # For earnings data
    earnings_collection = db["earnings"]
    earnings_collection.create_index([
        ("symbol", ASCENDING),
        ("fiscalDateEnding", DESCENDING),
        ("report_type", ASCENDING)
    ], unique=True)
    earnings_collection.create_index("report_type")
    print("Collection 'earnings' ready.")

    # For news and sentiment data
    news_collection = db["news_sentiment"]
    news_collection.create_index([("url", ASCENDING), ("symbol", ASCENDING)], unique=True)
    news_collection.create_index("time_published")
    news_collection.create_index("symbol")
    news_collection.create_index([("symbol", ASCENDING), ("time_published", DESCENDING)])
    print("Collection 'news_sentiment' ready.")

    # For weekly and monthly time series data
    time_series_collection = db["time_series"]
    time_series_collection.create_index([
        ("symbol", ASCENDING),
        ("date", DESCENDING),
        ("series_type", ASCENDING)
    ], unique=True)
    time_series_collection.create_index("series_type")
    print("Collection 'time_series' ready.")

    # For real-time quotes
    quotes_collection = db["real_time_quotes"]
    quotes_collection.create_index([("symbol", ASCENDING), ("timestamp", DESCENDING)])
    quotes_collection.create_index("timestamp")
    print("Collection 'real_time_quotes' ready.")

    # For company ratings
    ratings_collection = db["company_ratings"]
    ratings_collection.create_index([("symbol", ASCENDING), ("timestamp", DESCENDING)])
    ratings_collection.create_index("timestamp")
    print("Collection 'company_ratings' ready.")

    # For social sentiment data
    sentiment_collection = db["social_sentiment"]
    sentiment_collection.create_index([("symbol", ASCENDING), ("timestamp", DESCENDING)])
    sentiment_collection.create_index("platform")
    sentiment_collection.create_index("sentiment_label")
    print("Collection 'social_sentiment' ready.")

    # For SEC filings
    filings_collection = db["sec_filings"]
    filings_collection.create_index([
        ("symbol", ASCENDING),
        ("filing_date", DESCENDING),
        ("form_type", ASCENDING)
    ], unique=True)
    filings_collection.create_index("form_type")
    print("Collection 'sec_filings' ready.")

    # For ingestion progress tracking
    progress_collection = db["ingestion_progress"]
    progress_collection.create_index("symbol", unique=True)
    progress_collection.create_index("status")
    progress_collection.create_index("last_updated")
    progress_collection.create_index([("status", ASCENDING), ("last_updated", DESCENDING)])
    print("Collection 'ingestion_progress' ready.")

    # For user queries and interaction logs
    user_queries_collection = db["user_queries"]
    user_queries_collection.create_index("timestamp")
    user_queries_collection.create_index("user_id")
    user_queries_collection.create_index([("user_id", ASCENDING), ("timestamp", DESCENDING)])
    print("Collection 'user_queries' ready.")

    print("All collections are set up with appropriate indexes.")

if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        db = client[DATABASE_NAME]
        setup_collections(db)
        client.close() # Close the client connection after setup
        print("MongoDB setup script finished.")