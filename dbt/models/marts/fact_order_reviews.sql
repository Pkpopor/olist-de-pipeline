with reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customers as (
    select customer_key, customer_id
    from {{ ref('dim_customers') }}
    where is_current = true
),

dim_date as (
    select date_id, date_day from {{ ref('dim_date') }}
)

select
    r.review_id,
    r.order_id,
    dc.customer_key,
    dd.date_id          as order_date_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.created_at        as review_created_at,
    r.answered_at       as review_answered_at,
    o.purchased_at
from reviews r
left join orders        o  on r.order_id     = o.order_id
left join dim_customers dc on o.customer_id  = dc.customer_id
left join dim_date      dd on o.purchased_at::date = dd.date_day
