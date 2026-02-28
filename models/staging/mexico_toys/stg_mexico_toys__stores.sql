with source as (
    select * from {{ source('mexico_toys', 'raw_stores_mexico_toys') }}
),

renamed as (
    select
        Store_ID                            as store_id,
        Store_Name                          as store_name,
        Store_City                          as store_city,
        Store_Location                      as store_location,
        cast(Store_Open_Date as date)       as store_open_date,
        'Mexico'                            as country
    from source
)

select * from renamed