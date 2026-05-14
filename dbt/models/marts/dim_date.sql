with date_spine as (
    select generate_series(
        (select min(purchased_at)::date from {{ ref('stg_orders') }}),
        (select max(purchased_at)::date from {{ ref('stg_orders') }}),
        '1 day'::interval
    )::date as date_day
)

select
    to_char(date_day, 'YYYYMMDD')::int                        as date_id,
    date_day,
    extract(year    from date_day)::int                       as year,
    extract(month   from date_day)::int                       as month,
    extract(day     from date_day)::int                       as day,
    extract(quarter from date_day)::int                       as quarter,
    extract(isodow  from date_day)::int                       as day_of_week,
    trim(to_char(date_day, 'Day'))                            as day_name,
    trim(to_char(date_day, 'Month'))                          as month_name,
    case when extract(isodow from date_day) in (6, 7)
        then true else false end                              as is_weekend,
    case when
        (extract(month from date_day)::int = 1  and extract(day from date_day)::int = 1 ) or
        (extract(month from date_day)::int = 4  and extract(day from date_day)::int = 21) or
        (extract(month from date_day)::int = 5  and extract(day from date_day)::int = 1 ) or
        (extract(month from date_day)::int = 9  and extract(day from date_day)::int = 7 ) or
        (extract(month from date_day)::int = 10 and extract(day from date_day)::int = 12) or
        (extract(month from date_day)::int = 11 and extract(day from date_day)::int = 2 ) or
        (extract(month from date_day)::int = 11 and extract(day from date_day)::int = 15) or
        (extract(month from date_day)::int = 12 and extract(day from date_day)::int = 25)
        then true else false end                              as is_holiday_brazil,
    'FQ' || extract(quarter from date_day)::text              as fiscal_quarter
from date_spine
