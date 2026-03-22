"""Quick script to inspect the DuckDB database.

Usage:
    python query_db.py

Interactive querying:
    Run from the project root (stock_market_data_pipeline/):

    >>> import duckdb
    >>> conn = duckdb.connect('data/stock_ticker_data.duckdb', read_only=True)
    >>> conn.sql('SELECT * FROM stg_market_prices LIMIT 5')
    >>> conn.sql('SELECT symbol, COUNT(*) FROM stg_market_prices GROUP BY symbol')
    >>> conn.close()
"""

import duckdb

DB_PATH = "data/stock_ticker_data.duckdb"


def query(conn: duckdb.DuckDBPyConnection, label: str, sql: str) -> None:
    print(f"=== {label} ===")
    result = conn.sql(sql)
    columns = result.columns
    rows = result.fetchall()
    print("  ".join(columns))
    print("-" * (len("  ".join(columns)) + 10))
    for row in rows:
        print("  ".join(str(v) for v in row))
    print()


def main() -> None:
    conn = duckdb.connect(DB_PATH, read_only=True)

    query(conn, "Row counts by symbol",
          "SELECT symbol, count(*) AS rows FROM raw_market_prices GROUP BY symbol ORDER BY symbol")

    query(conn, "Date range by symbol",
          "SELECT symbol, min(date) AS earliest, max(date) AS latest "
          "FROM raw_market_prices GROUP BY symbol ORDER BY symbol")

    query(conn, "Most recent 5 rows",
          "SELECT symbol, date, open, high, low, close, volume "
          "FROM raw_market_prices ORDER BY date DESC, symbol LIMIT 5")

    query(conn, "Audit timestamps (sample)",
          "SELECT symbol, date, inserted_datetime, updated_datetime "
          "FROM raw_market_prices ORDER BY date DESC LIMIT 3")

    query(conn, "Ingest runs",
          "SELECT * FROM ingest_runs ORDER BY run_id")

    conn.close()


if __name__ == "__main__":
    main()
