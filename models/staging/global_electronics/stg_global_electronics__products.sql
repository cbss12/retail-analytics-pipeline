with source as (
    select * from {{ source('global_electronics', 'raw_products_global_electronics') }}
),

renamed as (
    select
        "ProductKey"                                                                            as product_id,
        "Product Name"                                                                          as product_name,
        "Brand"                                                                                 as brand,
        "Color"                                                                                 as color,
        cast(replace(replace(trim("Unit Cost USD"), '$', ''), ',', '') as decimal(10,2))       as product_cost,
        cast(replace(replace(trim("Unit Price USD"), '$', ''), ',', '') as decimal(10,2))      as product_price,
        "SubcategoryKey"                                                                        as subcategory_id,
        "Subcategory"                                                                           as subcategory,
        "CategoryKey"                                                                           as category_id,
        "Category"                                                                              as product_category
    from source
)

select * from renamed