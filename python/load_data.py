# ============================================
# load_data.py
# Reads raw stock CSVs, cleans them,
# and loads them into PostgreSQL
# ============================================

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load database credentials from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Build the database connection
engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# The 10 stocks we're analyzing
TICKERS = ["aapl", "msft", "googl", "amzn", "jpm", "gs", "bac", "xom", "wmt", "tsla"]

# Where your raw CSV files live
DATA_DIR = "data/raw"

def load_stock(ticker):
    filepath = os.path.join(DATA_DIR, f"{ticker}.us.txt")

    # Read the CSV
    df = pd.read_csv(filepath)

    # Standardize column names to lowercase
    df.columns = [col.strip().lower() for col in df.columns]

    # Rename columns to match our database schema
    df = df.rename(columns={
        "date":   "trade_date",
        "open":   "open_price",
        "high":   "high_price",
        "low":    "low_price",
        "close":  "close_price",
        "vol":    "volume"
    })

    # Add the ticker column
    df["ticker"] = ticker.upper()

    # Keep only the columns we need
    df = df[["ticker", "trade_date", "open_price",
             "high_price", "low_price", "close_price", "volume"]]

    # Drop any rows with missing values
    df = df.dropna()

    # Convert trade_date to proper date format
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # Remove any duplicate dates for this ticker
    df = df.drop_duplicates(subset=["ticker", "trade_date"])

    return df

def main():
    all_data = []

    for ticker in TICKERS:
        print(f"Loading {ticker.upper()}...")
        try:
            df = load_stock(ticker)
            all_data.append(df)
            print(f"  {len(df)} rows loaded")
        except FileNotFoundError:
            print(f"  WARNING: {ticker}.us.txt not found in data/raw/ — skipping")

    # Combine all stocks into one dataframe
    combined = pd.concat(all_data, ignore_index=True)
    print(f"\nTotal rows across all stocks: {len(combined)}")

    # Load into PostgreSQL
    print("\nWriting to database...")
    combined.to_sql(
        "stock_prices",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    print("Done! All stock data loaded into stock_prices table.")

if __name__ == "__main__":
    main()