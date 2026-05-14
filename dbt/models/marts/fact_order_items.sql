with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customers as (
    select customer_key, customer_id
    from {{ ref('dim_customers') }}
    where is_current = true
),

dim_sellers as (
    select seller_key, seller_id
    from {{ ref('dim_sellers') }}
    where is_current = true
),

dim_products as (
    select product_key, product_id from {{ ref('dim_products') }}
),

dim_date as (
    select date_id, date_day from {{ ref('dim_date') }}
)

select
    md5(oi.order_id || '-' || oi.order_item_id::text)  as order_item_key,
    oi.order_id,
    oi.order_item_id,
    dc.customer_key,
    ds.seller_key,
    dp.product_key,
    dd.date_id                                          as order_date_id,
    o.order_status,
    oi.price,
    oi.freight_value,
    oi.price + oi.freight_value                         as total_amount,
    oi.shipping_limit_at,
    o.purchased_at,
    o.approved_at,
    o.estimated_delivery_at,
    o.delivered_customer_at
from order_items oi
left join orders        o  on oi.order_id    = o.order_id
left join dim_customers dc on o.customer_id  = dc.customer_id
left join dim_sellers   ds on oi.seller_id   = ds.seller_id
left join dim_products  dp on oi.product_id  = dp.product_id
left join dim_date      dd on o.purchased_at::date = dd.date_day
