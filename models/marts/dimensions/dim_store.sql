with mx_stores as (
    select
        'MX-' || cast(store_id as varchar)      as store_key,
        store_id,
        store_name,
        store_city                               as city,
        store_location                           as location_type,
        null                                     as state,
        country,
        null                                     as square_meters,
        store_open_date,
        'mexico_toys'                            as source
    from {{ ref('stg_mexico_toys__stores') }}
),

gl_stores as (
    select
        'GL-' || cast(store_id as varchar)      as store_key,
        store_id,
        null                                     as store_name,
        null                                     as city,
        null                                     as location_type,
        store_state                              as state,
        country,
        square_meters,
        store_open_date,
        'global_electronics'                     as source
    from {{ ref('stg_global_electronics__stores') }}
),

final as (
    select * from mx_stores
    union all
    select * from gl_stores
)

select * from final