with sellers as (
    select * from {{ ref('stg_sellers') }}
),

geolocation as (
    select * from {{ ref('stg_geolocation') }}
)

select
    md5(s.seller_id)             as seller_key,
    s.seller_id,
    s.zip_code_prefix,
    coalesce(g.city, s.city)     as city,
    s.state,
    g.latitude,
    g.longitude,
    current_timestamp            as valid_from,
    null::timestamp              as valid_to,
    true                         as is_current
from sellers s
left join geolocation g on s.zip_code_prefix = g.zip_code_prefix
