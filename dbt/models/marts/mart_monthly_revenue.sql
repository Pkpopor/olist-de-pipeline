{{
    config(materialized='table')
}}

with items as (
    select * from {{ ref('fact_order_items') }}
),

payments as (
    select * from {{ ref('fact_order_payments') }}
),

reviews as (
    select * from {{ ref('fact_order_reviews') }}
),

dates as (
    select * from {{ ref('dim_date') }}
),

monthly_items as (
    select
        dd.year,
        dd.month,
        dd.fiscal_quarter,
        count(distinct fi.order_id)     as total_orders,
        count(fi.order_item_key)        as total_items,
        sum(fi.price)                   as total_revenue,
        sum(fi.freight_value)           as total_freight
    from items fi
    inner join dates dd on fi.order_date_id = dd.date_id
    group by dd.year, dd.month, dd.fiscal_quarter
),

monthly_reviews as (
    select
        dd.year,
        dd.month,
        avg(fr.review_score)            as avg_review_score
    from reviews fr
    inner join dates dd on fr.order_date_id = dd.date_id
    group by dd.year, dd.month
),

payment_counts as (
    select
        dd.year,
        dd.month,
        fp.payment_type,
        count(*)                        as payment_count
    from payments fp
    inner join dates dd on fp.order_date_id = dd.date_id
    group by dd.year, dd.month, fp.payment_type
),

top_payment as (
    select year, month, payment_type as top_payment_type
    from (
        select
            year,
            month,
            payment_type,
            row_number() over (
                partition by year, month
                order by payment_count desc
            ) as rn
        from payment_counts
    ) ranked
    where rn = 1
)

select
    mi.year * 100 + mi.month                        as year_month_key,
    mi.year,
    mi.month,
    mi.fiscal_quarter,
    mi.total_orders,
    mi.total_items,
    round(mi.total_revenue::numeric,    2)          as total_revenue,
    round(mi.total_freight::numeric,    2)          as total_freight,
    round(mr.avg_review_score::numeric, 2)          as avg_review_score,
    tp.top_payment_type
from monthly_items mi
left join monthly_reviews mr on mi.year = mr.year and mi.month = mr.month
left join top_payment     tp on mi.year = tp.year and mi.month = tp.month
order by mi.year, mi.month
