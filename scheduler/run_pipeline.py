"""Pipeline entry point.

Orchestrates a gated pipeline where each layer is validated before the next is built:

    1. Ingest raw data from Alpha Vantage into DuckDB
    2. Test raw source   → gate: stop if raw data is bad
    3. Build staging     → test staging   → gate: stop if staging fails
    4. Build intermediate → test intermediate → gate: stop if intermediate fails
    5. Build marts       → test marts     → gate: stop if marts fail

Usage:
    python -m scheduler.run_pipeline
"""

import subprocess
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from ingestion.client import fetch_symbols, SYMBOLS
from ingestion.loader import get_connection, load_records, log_run_start, log_run_end

# dbt commands run from the dbt/ directory so that profiles.yml paths resolve correctly.
DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def run_dbt(command: str) -> None:
    """Run a dbt command from the dbt/ directory. Exits on failure."""
    full_cmd = f"dbt {command} --profiles-dir . --project-dir ."
    print(f"\n--- Running: {full_cmd} ---")
    result = subprocess.run(full_cmd, shell=True, cwd=DBT_DIR)
    if result.returncode != 0:
        print(f"\nFAILED: dbt {command} (exit code {result.returncode})")
        sys.exit(1)


def run_dbt_phase(name: str, selector: str, build: bool = True) -> None:
    """Build and/or test a single dbt layer. Stops the pipeline on failure."""
    print(f"\n=== {name} ===")
    if build:
        run_dbt(f"run -s {selector}")
    run_dbt(f"test -s {selector}")


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
        # Close the connection before dbt opens it — DuckDB only allows one writer.
        conn.close()

    # Each phase gates the next: test raw → build+test staging → ... → build+test marts.
    run_dbt_phase("Phase 1: Test raw source data",       "source:raw",   build=False)
    run_dbt_phase("Phase 2: Build and test staging",      "staging")
    run_dbt_phase("Phase 3: Build and test intermediate", "intermediate")
    run_dbt_phase("Phase 4: Build and test marts",        "marts")

    print("\nPipeline complete: all 4 phases passed.")


if __name__ == "__main__":
    main()
