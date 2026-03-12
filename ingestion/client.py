"""Alpha Vantage API client.

Fetches daily OHLCV data with rate-limit awareness.
Free tier limits: 5 requests/minute, 25 requests/day.
"""

import os
import time
import requests


BASE_URL = "https://www.alphavantage.co/query"
RATE_LIMIT_DELAY = 15  # seconds between calls to stay within 5/min

SYMBOLS = ["SPY", "QQQ", "IWM", "AAPL", "MSFT", "NVDA", "TSLA"]


def get_daily_ohlcv(symbol: str, api_key: str | None = None) -> list[dict]:
    """Fetch full daily OHLCV data for a symbol from Alpha Vantage.

    Args:
        symbol: Ticker symbol (e.g. "AAPL").
        api_key: Alpha Vantage API key. Defaults to ALPHA_VANTAGE_API_KEY env var.

    Returns:
        List of dicts with keys: symbol, date, open, high, low, close, volume.

    Raises:
        ValueError: If the API returns an error message.
        requests.HTTPError: On non-2xx responses.
    """
    key = api_key or os.environ["ALPHA_VANTAGE_API_KEY"]

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": "compact",  # last 100 trading days. More history only available with premium (paid) API key.
        "apikey": key,
    }

    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "Error Message" in data:
        raise ValueError(f"Alpha Vantage error for {symbol!r}: {data['Error Message']}")
    if "Note" in data:
        raise ValueError(f"Alpha Vantage rate limit hit: {data['Note']}")

    time_series = data.get("Time Series (Daily)", {})
    records = []
    for date_str, values in time_series.items():
        records.append({
            "symbol": symbol,
            "date": date_str,
            "open": float(values["1. open"]),
            "high": float(values["2. high"]),
            "low": float(values["3. low"]),
            "close": float(values["4. close"]),
            "volume": int(values["5. volume"]),
        })

    return records


def fetch_symbols(symbols: list[str], api_key: str | None = None) -> list[dict]:
    """Fetch OHLCV data for multiple symbols, respecting rate limits.

    Args:
        symbols: List of ticker symbols.
        api_key: Alpha Vantage API key. Defaults to ALPHA_VANTAGE_API_KEY env var.

    Returns:
        Flat list of OHLCV records across all symbols.
    """
    all_records = []
    for i, symbol in enumerate(symbols):
        if i > 0:
            time.sleep(RATE_LIMIT_DELAY)
        records = get_daily_ohlcv(symbol, api_key=api_key)
        all_records.extend(records)
    return all_records
