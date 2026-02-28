with mx_products as (
    select
        'MX-' || cast(product_id as varchar)    as product_key,
        product_id,
        product_name,
        product_category,
        null                                     as subcategory,
        null                                     as brand,
        null                                     as color,
        product_cost,
        product_price,
        'mexico_toys'                            as source
    from {{ ref('stg_mexico_toys__products') }}
),

gl_products as (
    select
        'GL-' || cast(product_id as varchar)    as product_key,
        product_id,
        product_name,
        product_category,
        subcategory,
        brand,
        color,
        product_cost,
        product_price,
        'global_electronics'                     as source
    from {{ ref('stg_global_electronics__products') }}
),

final as (
    select * from mx_products
    union all
    select * from gl_products
)

select * from final