with source as (
    select * from {{ source('global_electronics', 'raw_exchange_rate_global_electronics') }}
),

renamed as (
    select
        cast("Date" as date)            as rate_date,
        "Currency"                      as currency_code,
        cast("Exchange" as decimal(10,6)) as exchange_rate
    from source
)

select * from renamed