with source as (
    select * from {{ source('global_electronics', 'raw_customers_global_electronics') }}
),

renamed as (
    select
        "CustomerKey"                       as customer_id,
        "Gender"                            as gender,
        "Name"                              as customer_name,
        "City"                              as city,
        "State Code"                        as state_code,
        "State"                             as state,
        "Zip Code"                          as zip_code,
        "Country"                           as country,
        "Continent"                         as continent,
        cast("Birthday" as date)            as birthday
    from source
)

select * from renamed