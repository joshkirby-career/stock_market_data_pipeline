"""DuckDB loader for OHLCV data.

Handles connection management, schema initialization, idempotent upserts
into raw_market_prices, and ingestion run logging.
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
    conn.execute("CREATE SEQUENCE IF NOT EXISTS ingest_runs_seq START 1")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_runs (
            run_id        INTEGER PRIMARY KEY DEFAULT nextval('ingest_runs_seq'),
            started_at    TIMESTAMP NOT NULL,
            ended_at      TIMESTAMP,
            status        VARCHAR NOT NULL,
            symbols       VARCHAR NOT NULL,
            record_count  INTEGER,
            error_message VARCHAR
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
    """Insert a new ingest_runs row with status 'running'. Returns the run_id."""
    result = conn.execute(
        "INSERT INTO ingest_runs (started_at, status, symbols) "
        "VALUES (current_timestamp, 'running', ?) RETURNING run_id",
        [",".join(symbols)],
    )
    return result.fetchone()[0]


def log_run_end(
    conn: duckdb.DuckDBPyConnection,
    run_id: int,
    status: str,
    record_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Update an ingest_runs row with final status and timing."""
    conn.execute(
        "UPDATE ingest_runs SET ended_at = current_timestamp, status = ?, "
        "record_count = ?, error_message = ? WHERE run_id = ?",
        [status, record_count, error_message, run_id],
    )
