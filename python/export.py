# ============================================
# export.py
# Exports analysis results from PostgreSQL
# to CSV files for Tableau visualization
# ============================================

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()

engine = create_engine(
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

def export_prices():
    """Export raw prices for Tableau"""
    df = pd.read_sql("""
        SELECT ticker, trade_date, open_price, high_price,
               low_price, close_price, volume
        FROM stock_prices
        ORDER BY ticker, trade_date
    """, engine, parse_dates=["trade_date"])

    df.to_csv("outputs/stock_prices.csv", index=False)
    print(f"Exported {len(df)} rows to outputs/stock_prices.csv")

def export_signals():
    """Export computed signals for Tableau"""
    df = pd.read_sql("""
        SELECT ticker, trade_date, daily_return_pct,
               volatility_pct, moving_avg_7d, moving_avg_30d
        FROM stock_signals
        ORDER BY ticker, trade_date
    """, engine, parse_dates=["trade_date"])

    df.to_csv("outputs/stock_signals.csv", index=False)
    print(f"Exported {len(df)} rows to outputs/stock_signals.csv")

def export_summary():
    """Export summary stats for Tableau"""
    df = pd.read_sql("""
        SELECT
            ticker,
            ROUND(AVG(daily_return_pct)::numeric, 4) AS avg_daily_return,
            ROUND(AVG(volatility_pct)::numeric, 4)   AS avg_volatility,
            ROUND(MAX(daily_return_pct)::numeric, 4)  AS best_day,
            ROUND(MIN(daily_return_pct)::numeric, 4)  AS worst_day,
            COUNT(*) AS trading_days
        FROM stock_signals
        GROUP BY ticker
        ORDER BY avg_daily_return DESC
    """, engine)

    df.to_csv("outputs/summary_stats.csv", index=False)
    print(f"Exported summary stats to outputs/summary_stats.csv")
    print("\nSummary Stats:")
    print(df.to_string(index=False))

def main():
    print("Exporting data for Tableau...\n")
    export_prices()
    export_signals()
    export_summary()
    print("\nAll exports complete! Check the outputs/ folder.")

if __name__ == "__main__":
    main()