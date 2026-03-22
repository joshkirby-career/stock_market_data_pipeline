-- Computes 7-day and 30-day simple moving averages of close price.
-- Window frames use ROWS BETWEEN so only trading days with enough
-- history produce a value; earlier rows will be NULL.

with prices as (

    select * from {{ ref('stg_market_prices') }}

),

moving_averages as (

    select
        symbol,
        trading_date,
        close_price,
        -- Only compute the 7-day MA once we have a full 7 rows of history.
        case
            when row_number() over (
                partition by symbol order by trading_date
            ) >= 7
            then round(
                avg(close_price) over (
                    partition by symbol
                    order by trading_date
                    rows between 6 preceding and current row
                ),
                4
            )
        end as ma_7d,
        -- Only compute the 30-day MA once we have a full 30 rows of history.
        case
            when row_number() over (
                partition by symbol order by trading_date
            ) >= 30
            then round(
                avg(close_price) over (
                    partition by symbol
                    order by trading_date
                    rows between 29 preceding and current row
                ),
                4
            )
        end as ma_30d
    from prices

)

select * from moving_averages
