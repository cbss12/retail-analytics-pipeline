with mx_sales as (
    select
        s.sale_id                                       as sale_id,
        s.sale_date                                     as sale_date,
        'MX-' || cast(s.store_id as varchar)            as store_key,
        'MX-' || cast(s.product_id as varchar)          as product_key,
        null                                            as customer_id,
        s.units,
        p.product_cost,
        p.product_price,
        s.units * p.product_cost                        as total_cost,
        s.units * p.product_price                       as total_revenue,
        s.units * (p.product_price - p.product_cost)   as total_profit,
        'USD'                                           as currency_code,
        1.0                                             as exchange_rate,
        s.units * p.product_price                       as total_revenue_usd,
        s.units * p.product_cost                        as total_cost_usd,
        s.units * (p.product_price - p.product_cost)   as total_profit_usd,
        'mexico_toys'                                   as source
    from {{ ref('stg_mexico_toys__sales') }} s
    left join {{ ref('stg_mexico_toys__products') }} p
        on s.product_id = p.product_id
),

gl_sales as (
    select
        cast(s.order_number as varchar) || '-' || cast(s.line_item as varchar)  as sale_id,
        s.order_date                                                             as sale_date,
        'GL-' || cast(s.store_id as varchar)                                    as store_key,
        'GL-' || cast(s.product_id as varchar)                                  as product_key,
        s.customer_id,
        s.units,
        p.product_cost,
        p.product_price,
        s.units * p.product_cost                                                as total_cost,
        s.units * p.product_price                                               as total_revenue,
        s.units * (p.product_price - p.product_cost)                           as total_profit,
        s.currency_code,
        coalesce(e.exchange_rate, 1.0)                                          as exchange_rate,
        (s.units * p.product_price) / coalesce(e.exchange_rate, 1.0)           as total_revenue_usd,
        (s.units * p.product_cost) / coalesce(e.exchange_rate, 1.0)            as total_cost_usd,
        (s.units * (p.product_price - p.product_cost)) / coalesce(e.exchange_rate, 1.0) as total_profit_usd,
        'global_electronics'                                                    as source
    from {{ ref('stg_global_electronics__sales') }} s
    left join {{ ref('stg_global_electronics__products') }} p
        on s.product_id = p.product_id
    left join {{ ref('stg_global_electronics__exchange_rates') }} e
        on s.order_date = e.rate_date
        and s.currency_code = e.currency_code
),

final as (
    select * from mx_sales
    union all
    select * from gl_sales
)

select * from final