with source as (

    select * from {{ source('raw', 'raw_market_prices') }}

),

renamed as (

    select
        symbol,
        date           as trading_date,
        open           as open_price,
        high           as high_price,
        low            as low_price,
        close          as close_price,
        volume,
        inserted_datetime,
        updated_datetime
    from source

)

select * from renamed
