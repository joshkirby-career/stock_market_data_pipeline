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
from ingestion.loader import get_connection, load_records, log_run_start, log_run_update

# dbt commands run from the dbt/ directory so that profiles.yml paths resolve correctly.
DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


class DbtPhaseError(Exception):
    """Raised when a dbt phase fails."""

    def __init__(self, phase: str, command: str, returncode: int) -> None:
        self.phase = phase
        self.command = command
        self.returncode = returncode
        super().__init__(f"dbt {command} failed (exit code {returncode}) in {phase}")


def run_dbt(command: str, phase: str) -> None:
    """Run a dbt command from the dbt/ directory. Raises DbtPhaseError on failure."""
    full_cmd = f"dbt {command} --profiles-dir . --project-dir ."
    print(f"\n--- Running: {full_cmd} ---")
    result = subprocess.run(full_cmd, shell=True, cwd=DBT_DIR)
    if result.returncode != 0:
        print(f"\nFAILED: dbt {command} (exit code {result.returncode})")
        raise DbtPhaseError(phase, command, result.returncode)


def run_dbt_phase(name: str, selector: str, build: bool = True) -> None:
    """Build and/or test a single dbt layer. Raises DbtPhaseError on failure."""
    print(f"\n=== {name} ===")
    if build:
        run_dbt(f"run -s {selector}", name)
    run_dbt(f"test -s {selector}", name)


def main() -> None:
    conn = get_connection()
    run_id = log_run_start(conn, SYMBOLS)

    # --- Ingestion phase ---
    try:
        print(f"Fetching data for: {SYMBOLS}")
        records = fetch_symbols(SYMBOLS)
        print(f"Fetched {len(records)} records across {len(SYMBOLS)} symbols")

        count = load_records(records, conn=conn)
        print(f"Loaded {count} records into DuckDB")

        log_run_update(conn, run_id, ingest_status="success", record_count=count)
    except Exception as e:
        log_run_update(
            conn, run_id,
            overall_status="failed",
            ingest_status="failed",
            failed_phase="ingestion",
            error_message=str(e),
            ended=True,
        )
        conn.close()
        raise
    finally:
        # Close the connection before dbt opens it — DuckDB only allows one writer.
        conn.close()

    # --- dbt phases ---
    # Re-open a connection for status updates between dbt phases.
    # Each dbt phase runs as a subprocess with its own connection, so there's
    # no conflict as long as we close ours before calling run_dbt_phase.
    dbt_phases = [
        ("Phase 1: Test raw source data",       "source:raw",   False),
        ("Phase 2: Build and test staging",      "staging",      True),
        ("Phase 3: Build and test intermediate", "intermediate", True),
        ("Phase 4: Build and test marts",        "marts",        True),
    ]

    try:
        for name, selector, build in dbt_phases:
            run_dbt_phase(name, selector, build)
    except DbtPhaseError as e:
        conn = get_connection()
        log_run_update(
            conn, run_id,
            overall_status="failed",
            dbt_status="failed",
            failed_phase=e.phase,
            error_message=str(e),
            ended=True,
        )
        conn.close()
        print(f"\nPipeline failed during: {e.phase}")
        sys.exit(1)

    # --- All phases passed ---
    conn = get_connection()
    log_run_update(
        conn, run_id,
        overall_status="success",
        dbt_status="success",
        ended=True,
    )
    conn.close()

    print("\nPipeline complete: all 4 phases passed.")


if __name__ == "__main__":
    main()
