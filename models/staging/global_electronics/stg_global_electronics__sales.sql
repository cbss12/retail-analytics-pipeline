with source as (
    select * from {{ source('global_electronics', 'raw_sales_global_electronics') }}
),

renamed as (
    select
        "Order Number"                          as order_number,
        cast("Line Item" as integer)            as line_item,
        cast("Order Date" as date)              as order_date,
        cast("Delivery Date" as date)           as delivery_date,
        "CustomerKey"                           as customer_id,
        "StoreKey"                              as store_id,
        "ProductKey"                            as product_id,
        cast("Quantity" as integer)             as units,
        "Currency Code"                         as currency_code,
        'global_electronics'                    as source
    from source
)

select * from renamed