with payments as (
    select * from {{ ref('stg_order_payments') }}
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
    md5(p.order_id || '-' || p.payment_sequential::text)  as payment_key,
    p.order_id,
    p.payment_sequential,
    dc.customer_key,
    dd.date_id                                             as order_date_id,
    p.payment_type,
    p.payment_installments,
    p.payment_value
from payments p
left join orders        o  on p.order_id     = o.order_id
left join dim_customers dc on o.customer_id  = dc.customer_id
left join dim_date      dd on o.purchased_at::date = dd.date_day
