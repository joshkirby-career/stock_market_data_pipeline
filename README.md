# Stock Market Data Pipeline

An end-to-end data pipeline that ingests live daily stock market data from Alpha Vantage, transforms it through a layered dbt model, and serves it for analysis. Built with Python, DuckDB, and dbt.

## Architecture

```
Alpha Vantage API → Python Ingestion → DuckDB (raw) → dbt Transformations → Dashboard
```

The pipeline follows a **gated architecture** where each layer is tested before the next one builds:

```
Ingest → Test Raw → Build Staging → Test Staging → Build Intermediate → Test Intermediate → Build Marts → Test Marts
```

If any test fails, the pipeline stops. Raw data is never lost — it's already persisted before transformations begin.

## dbt Transformation Layers

| Layer | Model | Materialization | Purpose |
|-------|-------|-----------------|---------|
| **Staging** | `stg_market_prices` | view | Cleans and renames raw columns |
| **Intermediate** | `int_daily_returns` | view | Day-over-day return % via `LAG()` |
| **Intermediate** | `int_moving_averages` | view | 7-day and 30-day simple moving averages |
| **Mart** | `mart_daily_dashboard` | table | Joins returns + averages into one query-ready table |

**34 data tests** run across all layers — not_null checks, uniqueness constraints, value range validations, and expression-based assertions using dbt-utils.

## Tech Stack

- **Python** — ingestion, orchestration
- **DuckDB** — local analytical database (no server needed)
- **dbt** (dbt-duckdb adapter) — SQL transformations and testing
- **Alpha Vantage** — free-tier stock market API

## Project Structure

```
stock_market_data_pipeline/
├── ingestion/
│   ├── client.py              # Alpha Vantage API wrapper (rate-limit aware)
│   └── loader.py              # DuckDB upsert logic + run metadata tracking
├── scheduler/
│   └── run_pipeline.py        # Orchestrates: ingest → dbt run → dbt test (gated)
├── dbt/
│   ├── dbt_project.yml        # dbt project config
│   ├── profiles.yml           # DuckDB connection (in-repo, self-contained)
│   ├── packages.yml           # dbt-utils dependency
│   └── models/
│       ├── staging/           # Column renaming, source declaration, data sanity tests
│       ├── intermediate/      # Window function calculations (returns, moving averages)
│       └── marts/             # Final joined table for dashboard consumption
├── data/                      # Local DuckDB file (gitignored)
└── .env                       # API key (gitignored)
```

## Setup

### Prerequisites

- Python 3.10+
- A free [Alpha Vantage API key](https://www.alphavantage.co/support/#api-key)

### Installation

```bash
# Clone the repo
git clone https://github.com/your-username/stock-market-data-pipeline.git
cd stock-market-data-pipeline/stock_market_data_pipeline

# Create and activate a virtual environment
python -m venv ../venv
source ../venv/bin/activate  # or ..\venv\Scripts\activate on Windows

# Install dependencies
pip install -r requirements.txt

# Install dbt packages
cd dbt && dbt deps --profiles-dir . --project-dir . && cd ..

# Set up your API key
echo 'ALPHA_VANTAGE_API_KEY="your_key_here"' > .env
```

## Usage

### Run the full pipeline

From the `stock_market_data_pipeline/` directory:

```bash
python -m scheduler.run_pipeline
```

This will:
1. Fetch daily OHLCV data for 7 symbols (SPY, QQQ, IWM, AAPL, MSFT, NVDA, TSLA)
2. Upsert records into DuckDB with idempotent conflict handling
3. Run 4 gated dbt phases — each layer tested before the next builds

### Run dbt independently

```bash
cd dbt
dbt run --profiles-dir . --project-dir .     # Build all models
dbt test --profiles-dir . --project-dir .    # Run all 34 tests
```

### Launch the dashboard

From the `stock_market_data_pipeline/` directory:

```bash
streamlit run dashboard/app.py
```

This opens a Streamlit app in your browser with five sections:

- **Latest Prices** — summary cards showing each symbol's most recent close and daily change
- **Price & Moving Averages** — line chart with 7-day and 30-day MA overlays (single symbol selector)
- **Daily Returns (%)** — bar chart of day-over-day returns color-coded red/green (single symbol selector)
- **Normalized Performance** — compare relative performance across multiple symbols rebased to 100
- **Pipeline Health** — last pipeline run status, timestamp, and record count

Each chart has its own symbol and date range filters. Data is cached for 5 minutes and refreshes automatically when the pipeline runs.

## Design Decisions

- **Idempotent upserts** — the pipeline is safe to re-run for the same date without creating duplicates. Uses `INSERT ... ON CONFLICT DO UPDATE`.
- **Run metadata** — every ingestion run is logged to an `ingest_runs` table with timestamps, status, and record counts for freshness monitoring.
- **Rate-limit awareness** — API calls are spaced 15 seconds apart to stay within Alpha Vantage's free tier (5 calls/min, 25 calls/day).
- **In-repo dbt profiles** — `profiles.yml` lives in the repo rather than `~/.dbt/`, making the project fully self-contained and portable.
- **Gated testing** — tests at each layer act as quality gates. Bad data in the raw layer won't propagate to the mart that a dashboard reads from.

## DuckDB Schema

### Raw Tables (created by Python ingestion)

**`raw_market_prices`** — Primary key: `(symbol, date)`

| Column | Type | Description |
|--------|------|-------------|
| symbol | VARCHAR | Ticker symbol (SPY, AAPL, etc.) |
| date | DATE | Trading date |
| open | DOUBLE | Opening price |
| high | DOUBLE | Intraday high |
| low | DOUBLE | Intraday low |
| close | DOUBLE | Closing price |
| volume | BIGINT | Shares traded |
| inserted_datetime | TIMESTAMP | First ingestion timestamp |
| updated_datetime | TIMESTAMP | Last upsert timestamp |

**`ingest_runs`** — Run metadata for monitoring

| Column | Type | Description |
|--------|------|-------------|
| run_id | INTEGER | Auto-incrementing ID |
| started_at | TIMESTAMP | Run start time |
| ended_at | TIMESTAMP | Run end time |
| status | VARCHAR | running / success / failed |
| symbols | VARCHAR | Comma-separated list of symbols fetched |
| record_count | INTEGER | Total records processed |
| error_message | VARCHAR | Error details (if failed) |

### Staging View (created by dbt)

**`stg_market_prices`** — Cleaned pass-through with renamed columns

| Column | Type | Description |
|--------|------|-------------|
| symbol | VARCHAR | Ticker symbol |
| trading_date | DATE | Trading date (renamed from `date`) |
| open_price | DOUBLE | Opening price (renamed from `open`) |
| high_price | DOUBLE | Intraday high |
| low_price | DOUBLE | Intraday low |
| close_price | DOUBLE | Closing price |
| volume | BIGINT | Shares traded |
| inserted_datetime | TIMESTAMP | First ingestion timestamp |
| updated_datetime | TIMESTAMP | Last upsert timestamp |

### Intermediate Views (created by dbt)

**`int_daily_returns`** — Day-over-day price change per symbol

| Column | Type | Description |
|--------|------|-------------|
| symbol | VARCHAR | Ticker symbol |
| trading_date | DATE | Trading date |
| close_price | DOUBLE | Closing price |
| prev_close_price | DOUBLE | Previous trading day's close (NULL for first row per symbol) |
| daily_return_pct | DOUBLE | Percentage change from previous close (NULL for first row) |

**`int_moving_averages`** — Rolling averages per symbol

| Column | Type | Description |
|--------|------|-------------|
| symbol | VARCHAR | Ticker symbol |
| trading_date | DATE | Trading date |
| close_price | DOUBLE | Closing price |
| ma_7d | DOUBLE | 7-day simple moving average (NULL until 7 days of history) |
| ma_30d | DOUBLE | 30-day simple moving average (NULL until 30 days of history) |

### Mart Table (created by dbt)

**`mart_daily_dashboard`** — Final wide table for dashboard consumption

| Column | Type | Description |
|--------|------|-------------|
| symbol | VARCHAR | Ticker symbol |
| trading_date | DATE | Trading date |
| close_price | DOUBLE | Closing price |
| prev_close_price | DOUBLE | Previous trading day's close |
| daily_return_pct | DOUBLE | Day-over-day return percentage |
| ma_7d | DOUBLE | 7-day simple moving average |
| ma_30d | DOUBLE | 30-day simple moving average |
