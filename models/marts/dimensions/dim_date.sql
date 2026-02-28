with date_spine as (
    select unnest(
        generate_series(
            cast('2015-01-01' as date),
            cast('2023-12-31' as date),
            interval '1 day'
        )
    ) as date_day
),

final as (
    select
        cast(strftime(date_day, '%Y%m%d') as integer)   as date_id,
        date_day                                         as full_date,
        year(date_day)                                   as year,
        quarter(date_day)                                as quarter,
        month(date_day)                                  as month_number,
        strftime(date_day, '%B')                         as month_name,
        week(date_day)                                   as week_number,
        dayofweek(date_day)                              as day_of_week,
        strftime(date_day, '%A')                         as day_name,
        year(date_day) || '-Q' || quarter(date_day)      as year_quarter,
        strftime(date_day, '%Y-%m')                      as year_month
    from date_spine
)

select * from final