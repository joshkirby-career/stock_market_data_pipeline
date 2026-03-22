-- Calculates day-over-day return for each ticker.
-- Uses LAG() to grab the previous trading day's close price,
-- then computes the percentage change.

with prices as (

    select * from {{ ref('stg_market_prices') }}

),

daily_returns as (

    select
        symbol,
        trading_date,
        close_price,
        lag(close_price) over (
            partition by symbol
            order by trading_date
        ) as prev_close_price,
        round(
            (close_price - lag(close_price) over (
                partition by symbol
                order by trading_date
            )) / lag(close_price) over (
                partition by symbol
                order by trading_date
            ) * 100,
            4
        ) as daily_return_pct
    from prices

)

select * from daily_returns
