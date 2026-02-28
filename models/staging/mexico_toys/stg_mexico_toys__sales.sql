with source as (
    select * from {{ source('mexico_toys', 'raw_sales_mexico_toys') }}
),

renamed as (
    select
        Sale_ID                         as sale_id,
        cast(Date as date)              as sale_date,
        Store_ID                        as store_id,
        Product_ID                      as product_id,
        cast(Units as integer)          as units,
        'mexico_toys'                   as source
    from source
)

select * from renamed
