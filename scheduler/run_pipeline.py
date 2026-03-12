"""Pipeline entry point.

Currently: fetches OHLCV data from Alpha Vantage and prints a summary.
Future steps (not yet implemented): upsert to DuckDB, run dbt, run dbt tests.

Usage:
    python -m scheduler.run_pipeline
"""

from dotenv import load_dotenv

load_dotenv()

from ingestion.client import fetch_symbols, SYMBOLS


def main() -> None:
    print(f"Fetching data for: {SYMBOLS}")
    records = fetch_symbols(SYMBOLS)
    print(f"Fetched {len(records)} records across {len(SYMBOLS)} symbols")
    if records:
        print("Sample record:", records[0])

        # Temp for dev. Will write to DuckDB eventually
        import csv
        with open("sample_data.csv", "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print("Written to sample_data.csv")


if __name__ == "__main__":
    main()
