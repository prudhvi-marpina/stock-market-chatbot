from pymongo import MongoClient
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
    daily_prices_collection.create_index([("symbol", 1), ("trade_date", -1)], unique=True)
    print("Collection 'daily_prices' ready.")

    # For user queries
    user_queries_collection = db["user_queries"]
    user_queries_collection.create_index("timestamp") # Index for time-based queries
    print("Collection 'user_queries' ready.")

    print("All collections are set up with appropriate indexes.")

if __name__ == "__main__":
    client = get_mongo_client()
    if client:
        db = client[DATABASE_NAME]
        setup_collections(db)
        client.close() # Close the client connection after setup
        print("MongoDB setup script finished.")