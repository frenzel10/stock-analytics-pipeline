# ============================================
# analysis.py
# Pulls stock data from PostgreSQL, computes
# trading signals using Python and pandas,
# then writes results back to the database
# ============================================

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def fetch_prices():
    """Pull all stock prices from the database into a dataframe"""
    query = """
        SELECT ticker, trade_date, open_price, high_price,
               low_price, close_price, volume
        FROM stock_prices
        ORDER BY ticker, trade_date
    """
    df = pd.read_sql(query, engine, parse_dates=["trade_date"])
    print(f"Fetched {len(df)} rows from stock_prices")
    return df

def compute_signals(df):
    """Compute trading signals for each stock"""
    results = []

    for ticker, group in df.groupby("ticker"):
        print(f"Computing signals for {ticker}...")

        g = group.sort_values("trade_date").copy()

        # Daily return % — how much did the stock move each day?
        g["daily_return_pct"] = g["close_price"].pct_change() * 100

        # Daily volatility % — how wide was the price range vs low?
        g["volatility_pct"] = (
            (g["high_price"] - g["low_price"]) / g["low_price"] * 100
        )

        # 7-day moving average — short term trend
        g["moving_avg_7d"] = (
            g["close_price"].rolling(window=7).mean()
        )

        # 30-day moving average — longer term trend
        g["moving_avg_30d"] = (
            g["close_price"].rolling(window=30).mean()
        )

        results.append(g[[
            "ticker", "trade_date", "daily_return_pct",
            "volatility_pct", "moving_avg_7d", "moving_avg_30d"
        ]])

    combined = pd.concat(results, ignore_index=True)
    combined = combined.dropna()
    print(f"\nTotal signal rows computed: {len(combined)}")
    return combined

def write_signals(df):
    """Write computed signals back to PostgreSQL"""
    print("Writing signals to database...")
    df.to_sql(
        "stock_signals",
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )
    print("Done! Signals written to stock_signals table.")

def print_summary():
    """Print a quick summary of interesting findings"""
    print("\n=== SUMMARY FINDINGS ===\n")

    # Most volatile stock on average
    q1 = """
        SELECT ticker,
               ROUND(AVG(volatility_pct)::numeric, 4) AS avg_volatility
        FROM stock_signals
        GROUP BY ticker
        ORDER BY avg_volatility DESC
    """
    vol = pd.read_sql(q1, engine)
    print("Average Daily Volatility by Stock:")
    print(vol.to_string(index=False))

    # Best average daily return
    q2 = """
        SELECT ticker,
               ROUND(AVG(daily_return_pct)::numeric, 4) AS avg_daily_return
        FROM stock_signals
        GROUP BY ticker
        ORDER BY avg_daily_return DESC
    """
    ret = pd.read_sql(q2, engine)
    print("\nAverage Daily Return by Stock:")
    print(ret.to_string(index=False))

def main():
    df = fetch_prices()
    signals = compute_signals(df)
    write_signals(signals)
    print_summary()

if __name__ == "__main__":
    main()