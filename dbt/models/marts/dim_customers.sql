with customers as (
    select * from {{ ref('stg_customers') }}
),

geolocation as (
    select * from {{ ref('stg_geolocation') }}
)

select
    md5(c.customer_id)           as customer_key,
    c.customer_id,
    c.customer_unique_id,
    c.zip_code_prefix,
    coalesce(g.city, c.city)     as city,
    c.state,
    g.latitude,
    g.longitude,
    current_timestamp            as valid_from,
    null::timestamp              as valid_to,
    true                         as is_current
from customers c
left join geolocation g on c.zip_code_prefix = g.zip_code_prefix
