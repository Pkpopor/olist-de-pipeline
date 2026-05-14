{{
    config(materialized='table')
}}

with items as (
    select * from {{ ref('fact_order_items') }}
),

sellers as (
    select seller_key, seller_id, city, state
    from {{ ref('dim_sellers') }}
    where is_current = true
),

reviews as (
    select
        order_id,
        avg(review_score) as avg_review_score
    from {{ ref('fact_order_reviews') }}
    group by order_id
),

-- roll up to order level per seller first to avoid inflating averages
order_agg as (
    select
        seller_key,
        order_id,
        count(order_item_id)                                                as items_in_order,
        sum(price)                                                          as order_revenue,
        max(purchased_at)                                                   as purchased_at,
        max(delivered_customer_at)                                          as delivered_customer_at,
        max(estimated_delivery_at)                                          as estimated_delivery_at
    from items
    group by seller_key, order_id
),

seller_agg as (
    select
        oa.seller_key,
        count(distinct oa.order_id)                                         as total_orders,
        sum(oa.items_in_order)                                              as total_items_sold,
        sum(oa.order_revenue)                                               as total_revenue,
        avg(oa.order_revenue)                                               as avg_order_value,
        avg(
            case when oa.delivered_customer_at is not null
                 then extract(epoch from (oa.delivered_customer_at - oa.purchased_at)) / 86400.0
            end
        )                                                                   as avg_delivery_days,
        sum(
            case when oa.delivered_customer_at <= oa.estimated_delivery_at then 1 else 0 end
        )::float / nullif(
            sum(
                case when oa.delivered_customer_at is not null
                      and oa.estimated_delivery_at is not null then 1 else 0 end
            ), 0
        )                                                                   as on_time_delivery_rate,
        avg(r.avg_review_score)                                             as avg_review_score
    from order_agg oa
    left join reviews r on oa.order_id = r.order_id
    group by oa.seller_key
)

select
    s.seller_id,
    s.city                                          as seller_city,
    s.state                                         as seller_state,
    sa.total_orders,
    sa.total_items_sold,
    round(sa.total_revenue::numeric,      2)        as total_revenue,
    round(sa.avg_order_value::numeric,    2)        as avg_order_value,
    round(sa.avg_review_score::numeric,   2)        as avg_review_score,
    round(sa.avg_delivery_days::numeric,  1)        as avg_delivery_days,
    round(sa.on_time_delivery_rate::numeric, 4)     as on_time_delivery_rate
from seller_agg sa
inner join sellers s on sa.seller_key = s.seller_key
