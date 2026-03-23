"""Stock Market Data Pipeline — Streamlit Dashboard.

Reads from the dbt mart table (mart_daily_dashboard) and the pipeline
metadata table (ingest_runs) to display interactive charts and metrics.

Run from the project root:
    streamlit run dashboard/app.py
"""

from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Resolve the database path relative to this file so the dashboard works
# regardless of the working directory. dashboard/app.py is one level below
# the project root, so parent.parent reaches stock_market_data_pipeline/.
DB_PATH = Path(__file__).resolve().parent.parent / "data" / "stock_ticker_data.duckdb"

# Symbols available in the dataset.
SYMBOLS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA"]

# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------
# Streamlit re-runs this entire script top-to-bottom on every user
# interaction (filter change, button click, etc.). Without caching, each
# interaction would re-query DuckDB even though the underlying data hasn't
# changed.
#
# @st.cache_data stores a serialized copy of the returned DataFrame in
# memory. On subsequent runs, Streamlit returns the cached copy instead of
# executing the function body. The ttl=300 (time-to-live) parameter expires
# the cache after 5 minutes, so fresh pipeline data is picked up
# automatically without a manual refresh.


@st.cache_data(ttl=300)
def load_dashboard_data() -> pd.DataFrame:
    """Load the full mart table. With ~7 symbols x ~100 trading days each,
    this is only ~700 rows — small enough to load entirely and filter
    in-memory with pandas rather than pushing filters down to SQL."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.sql(
        "SELECT * FROM mart_daily_dashboard ORDER BY symbol, trading_date"
    ).df()
    conn.close()
    # DuckDB returns trading_date as datetime.date; convert to pandas
    # Timestamp for consistent filtering and Plotly axis formatting.
    df["trading_date"] = pd.to_datetime(df["trading_date"])
    return df


@st.cache_data(ttl=300)
def load_ingest_runs() -> pd.DataFrame:
    """Load the most recent pipeline runs for the health footer."""
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    df = conn.sql("SELECT * FROM ingest_runs ORDER BY run_id DESC LIMIT 5").df()
    conn.close()
    return df


# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Stock Market Dashboard", layout="wide")

# A small amount of custom CSS for formatting
st.markdown(
    """
    <style>
    /* Padding so charts don't stretch edge-to-edge on wide screens. */
    .block-container {
        padding-left: 8rem;
        padding-right: 8rem;
    }
    /* Tone down the multi-select tag background, which is bright red by default */
    span[data-baseweb="tag"] {
        background-color: #3a3a4a !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Stock Market Data Pipeline Dashboard")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
# Gracefully handle the case where the database doesn't exist yet (e.g.
# someone cloned the repo but hasn't run the pipeline). st.stop() halts
# rendering so the layout code below doesn't try to use a missing DataFrame.

try:
    df = load_dashboard_data()
    runs_df = load_ingest_runs()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.info(f"Expected database at: {DB_PATH}")
    st.stop()

if df.empty:
    st.warning("No data found. Run the pipeline first: python -m scheduler.run_pipeline")
    st.stop()

# Global date boundaries used by per-section date filters.
min_date = df["trading_date"].min().date()
max_date = df["trading_date"].max().date()

# ---------------------------------------------------------------------------
# Section 1 — Summary Cards
# ---------------------------------------------------------------------------
# Shows every symbol's most recent closing price and daily change. These
# always reflect the latest available data — they act as a "current
# snapshot" while the charts below are for historical exploration.

st.subheader("Latest Prices")

cols = st.columns(len(SYMBOLS))
for col, symbol in zip(cols, SYMBOLS):
    sym_df = df[df["symbol"] == symbol]
    if sym_df.empty:
        continue
    # Data is sorted by trading_date, so the last row is the most recent.
    latest = sym_df.iloc[-1]
    daily_ret = latest["daily_return_pct"]
    delta_str = f"{daily_ret:.2f}%" if pd.notna(daily_ret) else None
    # st.metric renders the delta in green (positive) or red (negative)
    # automatically, which gives an instant visual read on daily movement.
    col.metric(
        label=symbol,
        value=f"${latest['close_price']:.2f}",
        delta=delta_str,
    )

# ---------------------------------------------------------------------------
# Section 2 — Price + Moving Averages
# ---------------------------------------------------------------------------
# Plots a single symbol's closing price alongside its 7-day and 30-day
# simple moving averages. The three price columns are "melted" from wide
# format into long format so Plotly can use line_dash= to distinguish the
# close line (solid) from the MA lines (dashed).

st.subheader("Price & Moving Averages")

# Each chart section has its own inline filters instead of a global sidebar.
# Streamlit's key= parameter ensures each widget has a unique identity so
# they don't interfere with each other across sections.
price_col1, price_col2, price_col3 = st.columns(3)
price_symbol = price_col1.selectbox("Symbol", options=SYMBOLS, key="price_symbol")
price_start = price_col2.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date, key="price_start")
price_end = price_col3.date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="price_end")

# Filter to the single selected symbol and date range.
price_df = df[
    (df["symbol"] == price_symbol)
    & (df["trading_date"] >= pd.Timestamp(price_start))
    & (df["trading_date"] <= pd.Timestamp(price_end))
]

# Melt close_price, ma_7d, ma_30d from columns into rows so Plotly can
# draw each as a separate line distinguished by dash style.
melted = price_df.melt(
    id_vars=["symbol", "trading_date"],
    value_vars=["close_price", "ma_7d", "ma_30d"],
    var_name="metric",
    value_name="price",
).dropna(subset=["price"])  # MA columns are NULL for early dates

# Replace internal column names with readable labels for the chart legend.
metric_labels = {"close_price": "Close", "ma_7d": "7-Day MA", "ma_30d": "30-Day MA"}
melted["metric"] = melted["metric"].map(metric_labels)

# Assign distinct colors so the three lines are easy to tell apart.
line_colors = {"Close": "#2196F3", "7-Day MA": "#FF9800", "30-Day MA": "#E91E63"}
line_dashes = {"Close": "solid", "7-Day MA": "dash", "30-Day MA": "dash"}

fig_price = px.line(
    melted,
    x="trading_date",
    y="price",
    color="metric",
    line_dash="metric",
    color_discrete_map=line_colors,
    line_dash_map=line_dashes,
    labels={"price": "Price ($)", "trading_date": "Date", "metric": ""},
)
fig_price.update_layout(hovermode="x unified", legend_title_text="")
st.plotly_chart(fig_price, use_container_width=True)

# ---------------------------------------------------------------------------
# Section 3 — Daily Returns
# ---------------------------------------------------------------------------
# Bar chart of day-over-day percentage returns for a single symbol. The
# color scale maps negative returns to red and positive to green, with a
# midpoint at zero

st.subheader("Daily Returns (%)")

ret_col1, ret_col2, ret_col3 = st.columns(3)
ret_symbol = ret_col1.selectbox("Symbol", options=SYMBOLS, key="ret_symbol")
ret_start = ret_col2.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date, key="ret_start")
ret_end = ret_col3.date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="ret_end")

returns_df = df[
    (df["symbol"] == ret_symbol)
    & (df["trading_date"] >= pd.Timestamp(ret_start))
    & (df["trading_date"] <= pd.Timestamp(ret_end))
].dropna(subset=["daily_return_pct"])

fig_returns = px.bar(
    returns_df,
    x="trading_date",
    y="daily_return_pct",
    color="daily_return_pct",
    color_continuous_scale=["red", "lightgray", "green"],
    color_continuous_midpoint=0,
    labels={"daily_return_pct": "Return %", "trading_date": "Date"},
)
fig_returns.update_layout(showlegend=False)
st.plotly_chart(fig_returns, use_container_width=True)

# ---------------------------------------------------------------------------
# Section 4 — Normalized Comparison
# ---------------------------------------------------------------------------
# Rebases every selected symbol's closing price to 100 at the start of the
# chosen date range. This makes it easy to compare relative performance
# across symbols with very different price levels. 
# A symbol at 115 on the chart means it's up 15% from the start date.

st.subheader("Normalized Performance (rebased to 100)")

norm_col1, norm_col2, norm_col3 = st.columns(3)
norm_symbols = norm_col1.multiselect("Symbols", options=SYMBOLS, default=["AAPL", "MSFT", "NVDA"], key="norm_symbols")
norm_start = norm_col2.date_input("Start date", value=min_date, min_value=min_date, max_value=max_date, key="norm_start")
norm_end = norm_col3.date_input("End date", value=max_date, min_value=min_date, max_value=max_date, key="norm_end")

if norm_symbols:
    norm_df = df[
        df["symbol"].isin(norm_symbols)
        & (df["trading_date"] >= pd.Timestamp(norm_start))
        & (df["trading_date"] <= pd.Timestamp(norm_end))
    ]

    # Get each symbol's closing price on its first available date in the range.
    # This becomes the "base price" that everything is divided by.
    first_rows = norm_df.groupby("symbol").first().reset_index()[["symbol", "close_price"]]
    first_rows = first_rows.rename(columns={"close_price": "base_price"})

    # Join base prices back and compute the indexed value.
    normalized = norm_df.merge(first_rows, on="symbol")
    normalized["normalized_price"] = (normalized["close_price"] / normalized["base_price"]) * 100

    fig_norm = px.line(
        normalized,
        x="trading_date",
        y="normalized_price",
        color="symbol",
        labels={"normalized_price": "Indexed Price", "trading_date": "Date"},
    )
    # Horizontal reference line at 100 for a quick visual anchor.
    fig_norm.add_hline(y=100, line_dash="dash", line_color="gray", opacity=0.5)
    fig_norm.update_layout(hovermode="x unified")
    st.plotly_chart(fig_norm, use_container_width=True)
else:
    st.info("Select at least one symbol.")

# ---------------------------------------------------------------------------
# Section 5 — Pipeline Health
# ---------------------------------------------------------------------------
# Shows metadata from the ingest_runs table so you can see at a glance
# whether the pipeline is running successfully and when data was last
# refreshed. Surfaces the error message if the most recent run failed.

st.divider()
st.subheader("Pipeline Health")

if runs_df.empty:
    st.warning("No pipeline runs found.")
else:
    latest_run = runs_df.iloc[0]  # Already sorted DESC by run_id
    status = latest_run.get("status", "unknown")

    col1, col2, col3 = st.columns(3)
    col1.metric("Last Run Status", status.upper())
    col2.metric(
        "Timestamp",
        str(latest_run.get("ended_at") or latest_run.get("started_at", "N/A"))[:19],
    )
    col3.metric("Records Loaded", latest_run.get("record_count", "N/A"))

    if status == "failed" and latest_run.get("error_message"):
        st.error(f"Error: {latest_run['error_message']}")
