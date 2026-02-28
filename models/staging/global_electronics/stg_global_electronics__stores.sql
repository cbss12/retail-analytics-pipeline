with source as (
    select * from {{ source('global_electronics', 'raw_stores_global_electronics') }}
),

renamed as (
    select
        "StoreKey"                          as store_id,
        "Country"                           as country,
        "State"                             as store_state,
        cast("Square Meters" as integer)    as square_meters,
        cast("Open Date" as date)           as store_open_date
    from source
)

select * from renamed