"""DuckDB loader for OHLCV data.

Handles connection management, schema initialization, idempotent upserts
into raw_market_prices, and pipeline run logging.
"""

from datetime import datetime, timezone
from pathlib import Path

import duckdb

DB_DIR = Path(__file__).resolve().parent.parent / "data"
DB_PATH = DB_DIR / "stock_ticker_data.duckdb"


def get_connection(db_path: Path | None = None) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating the data directory and schema if needed."""
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    _init_schema(conn)
    return conn


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Create tables if they don't already exist."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_market_prices (
            symbol            VARCHAR NOT NULL,
            date              DATE NOT NULL,
            open              DOUBLE NOT NULL,
            high              DOUBLE NOT NULL,
            low               DOUBLE NOT NULL,
            close             DOUBLE NOT NULL,
            volume            BIGINT NOT NULL,
            inserted_datetime TIMESTAMP NOT NULL DEFAULT current_timestamp,
            updated_datetime  TIMESTAMP NOT NULL DEFAULT current_timestamp,
            PRIMARY KEY (symbol, date)
        )
    """)
    conn.execute("CREATE SEQUENCE IF NOT EXISTS pipeline_runs_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            pipeline_run_id INTEGER PRIMARY KEY DEFAULT nextval('pipeline_runs_seq'),
            started_at      TIMESTAMP NOT NULL,
            ended_at        TIMESTAMP,
            overall_status  VARCHAR NOT NULL,
            ingest_status   VARCHAR,
            dbt_status      VARCHAR,
            failed_phase    VARCHAR,
            error_message   VARCHAR,
            record_count    INTEGER,
            symbols         VARCHAR NOT NULL
        )
    """)


def load_records(records: list[dict], conn: duckdb.DuckDBPyConnection) -> int:
    """Upsert OHLCV records into raw_market_prices.

    On conflict (same symbol + date), updates price/volume columns and
    updated_datetime while preserving the original inserted_datetime.

    Returns:
        Number of records processed.
    """
    if not records:
        return 0

    now = datetime.now(timezone.utc)
    conn.executemany(
        """
        INSERT INTO raw_market_prices
            (symbol, date, open, high, low, close, volume, inserted_datetime, updated_datetime)
        VALUES (?, CAST(? AS DATE), ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            updated_datetime = EXCLUDED.updated_datetime
        """,
        [
            (r["symbol"], r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"], now, now)
            for r in records
        ],
    )
    return len(records)


def log_run_start(conn: duckdb.DuckDBPyConnection, symbols: list[str]) -> int:
    """Insert a new pipeline_runs row with status 'running'. Returns the pipeline_run_id."""
    result = conn.execute(
        "INSERT INTO pipeline_runs (started_at, overall_status, symbols) "
        "VALUES (current_timestamp, 'running', ?) RETURNING pipeline_run_id",
        [",".join(symbols)],
    )
    return result.fetchone()[0]


def log_run_update(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    *,
    overall_status: str | None = None,
    ingest_status: str | None = None,
    dbt_status: str | None = None,
    failed_phase: str | None = None,
    error_message: str | None = None,
    record_count: int | None = None,
    ended: bool = False,
) -> None:
    """Update a pipeline_runs row. Only sets fields that are provided (non-None)."""
    sets: list[str] = []
    params: list = []

    if ended:
        sets.append("ended_at = current_timestamp")
    if overall_status is not None:
        sets.append("overall_status = ?")
        params.append(overall_status)
    if ingest_status is not None:
        sets.append("ingest_status = ?")
        params.append(ingest_status)
    if dbt_status is not None:
        sets.append("dbt_status = ?")
        params.append(dbt_status)
    if failed_phase is not None:
        sets.append("failed_phase = ?")
        params.append(failed_phase)
    if error_message is not None:
        sets.append("error_message = ?")
        params.append(error_message)
    if record_count is not None:
        sets.append("record_count = ?")
        params.append(record_count)

    if not sets:
        return

    params.append(run_id)
    conn.execute(
        f"UPDATE pipeline_runs SET {', '.join(sets)} WHERE pipeline_run_id = ?",
        params,
    )
