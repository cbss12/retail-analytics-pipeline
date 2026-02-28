with final as (
    select
        customer_id,
        customer_name,
        gender,
        city,
        state,
        state_code,
        zip_code,
        country,
        continent,
        birthday,
        date_diff('year', birthday, current_date) as age
    from {{ ref('stg_global_electronics__customers') }}
)

select * from final