with products as (
    select * from {{ source('raw', 'products') }}
),

translation as (
    select * from {{ source('raw', 'product_category_name_translation') }}
)

select
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name) as product_category,
    p.product_name_lenght::int         as product_name_length,
    p.product_description_lenght::int  as product_description_length,
    p.product_photos_qty::int          as product_photos_qty,
    p.product_weight_g::numeric        as product_weight_g,
    p.product_length_cm::numeric       as product_length_cm,
    p.product_height_cm::numeric       as product_height_cm,
    p.product_width_cm::numeric        as product_width_cm
from products p
left join translation t on p.product_category_name = t.product_category_name
