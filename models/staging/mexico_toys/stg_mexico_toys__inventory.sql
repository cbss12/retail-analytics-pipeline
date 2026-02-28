with source as (
    select * from {{ source('mexico_toys', 'raw_inventory_mexico_toys') }}
),

renamed as (
    select
        Store_ID                        as store_id,
        Product_ID                      as product_id,
        cast(Stock_On_Hand as integer)  as stock_on_hand
    from source
)

select * from renamed