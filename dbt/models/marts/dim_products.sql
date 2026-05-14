with products as (
    select * from {{ ref('stg_products') }}
)

select
    md5(product_id)         as product_key,
    product_id,
    product_category,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
from products
