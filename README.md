# Quantitative Trading Signal Pipeline

A full-stack data pipeline that ingests historical stock market data, computes 
quantitative trading signals, stores results in a relational database, and 
visualizes insights through an interactive dashboard.

**Live Dashboard:** [View on Tableau Public](https://public.tableau.com/app/profile/andrew.frenzel/viz/Book1_17739359688030/Dashboard1)

---

## Architecture
```
Raw CSV Data → Python ETL → PostgreSQL → Python Signal Engine → Tableau Dashboard
```

Each layer has a distinct responsibility — raw data is never modified, computed 
signals are stored separately, and the visualization layer is fully decoupled 
from the database.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Data Ingestion | Python (pandas) |
| Database | PostgreSQL |
| Signal Computation | Python (pandas, SQLAlchemy) |
| Visualization | Tableau Public |
| Query Layer | SQL (window functions, CTEs) |

---

## Project Structure
```
stock-analytics-pipeline/
├── data/raw/          # Raw CSV files (gitignored)
├── sql/
│   ├── schema.sql     # Database table definitions
│   └── queries.sql    # Analytical SQL queries
├── python/
│   ├── load_data.py   # ETL pipeline — cleans and loads CSVs into PostgreSQL
│   ├── analysis.py    # Computes trading signals, writes back to database
│   └── export.py      # Exports results to CSV for Tableau
├── outputs/           # Exported CSVs for Tableau (gitignored)
└── .env               # Database credentials (gitignored)
```

---

## Database Schema

**`stock_prices`** — Raw OHLCV data for 10 stocks (74,872 rows)
- ticker, trade_date, open_price, high_price, low_price, close_price, volume

**`stock_signals`** — Computed trading signals (74,582 rows)
- daily_return_pct, volatility_pct, moving_avg_7d, moving_avg_30d

---

## Trading Signals Computed

- **Daily Return %** — percentage price change day over day
- **Daily Volatility %** — intraday price range as % of low price  
- **7-day Moving Average** — short term trend signal
- **30-day Moving Average** — longer term trend signal; price above/below used as buy/sell indicator

---

## Key Findings

- **AMZN and TSLA** showed the highest average daily returns (0.20% and 0.19%) 
  but also the highest volatility — classic high risk / high reward profile
- **XOM** showed the lowest volatility (1.78%) and lowest average return (0.04%) — 
  consistent with energy stocks as defensive holdings
- **WMT** showed an anomalous best-day of 100% — likely a stock split event, 
  demonstrating the importance of data validation in financial pipelines
- Bank stocks (JPM, BAC, GS) clustered tightly in risk/return space, 
  reflecting their correlated exposure to credit and interest rate risk

---

## SQL Highlights

This project uses intermediate to advanced SQL including:
- Window functions (`PARTITION BY`, `ROWS BETWEEN`)
- CTEs for multi-step calculations
- JOINs across tables for signal enrichment
- Aggregate functions for summary statistics

---

## How to Run

1. Clone the repo
2. Create a PostgreSQL database called `stock_analysis`
3. Copy `.env.example` to `.env` and fill in your credentials
4. Download stock data from [Kaggle](https://www.kaggle.com/datasets/borismarjanovic/price-volume-data-for-all-us-stocks-etfs) and place in `data/raw/`
5. Run `psql stock_analysis -f sql/schema.sql`
6. Run `python3 python/load_data.py`
7. Run `python3 python/analysis.py`
8. Run `python3 python/export.py`
9. Connect `outputs/` CSVs to Tableau

---

*Built by Andrew Frenzel — [LinkedIn](https://linkedin.com/in/andrew-frenzel) | 
[GitHub](https://github.com/frenzel10)*