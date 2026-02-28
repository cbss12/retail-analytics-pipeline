with source as (
    select * from {{ source('mexico_toys', 'raw_products_mexico_toys') }}
),

renamed as (
    select
        Product_ID                                                                          as product_id,
        Product_Name                                                                        as product_name,
        Product_Category                                                                    as product_category,
        cast(replace(replace(trim(Product_Cost), '$', ''), ',', '') as decimal(10,2))      as product_cost,
        cast(replace(replace(trim(Product_Price), '$', ''), ',', '') as decimal(10,2))     as product_price
    from source
)

select * from renamed