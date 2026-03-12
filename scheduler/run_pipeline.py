"""Pipeline entry point.

Orchestrates: fetch from Alpha Vantage -> upsert to DuckDB -> (future: dbt run).

Usage:
    python -m scheduler.run_pipeline
"""

from dotenv import load_dotenv

load_dotenv()

from ingestion.client import fetch_symbols, SYMBOLS
from ingestion.loader import get_connection, load_records, log_run_start, log_run_end


def main() -> None:
    conn = get_connection()
    run_id = log_run_start(conn, SYMBOLS)

    try:
        print(f"Fetching data for: {SYMBOLS}")
        records = fetch_symbols(SYMBOLS)
        print(f"Fetched {len(records)} records across {len(SYMBOLS)} symbols")

        count = load_records(records, conn=conn)
        print(f"Loaded {count} records into DuckDB")

        log_run_end(conn, run_id, status="success", record_count=count)
    except Exception as e:
        log_run_end(conn, run_id, status="failed", error_message=str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
