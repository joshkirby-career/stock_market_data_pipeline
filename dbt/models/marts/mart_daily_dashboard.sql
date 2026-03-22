-- Final wide table that the Streamlit dashboard reads from.
-- Joins daily returns and moving averages into one row per symbol/date.
-- Materialized as a table so dashboard queries are fast.

with returns as (

    select * from {{ ref('int_daily_returns') }}

),

averages as (

    select * from {{ ref('int_moving_averages') }}

),

joined as (

    select
        returns.symbol,
        returns.trading_date,
        returns.close_price,
        returns.prev_close_price,
        returns.daily_return_pct,
        averages.ma_7d,
        averages.ma_30d
    from returns
    inner join averages
        on returns.symbol = averages.symbol
        and returns.trading_date = averages.trading_date

)

select * from joined
