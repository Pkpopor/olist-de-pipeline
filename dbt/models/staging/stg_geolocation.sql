with source as (
    select * from {{ source('raw', 'geolocation') }}
),

deduplicated as (
    select distinct on (geolocation_zip_code_prefix)
        -- Hash to match zip_code_prefix in customers/sellers (both SHA-256 hashed at ingest)
        encode(sha256(geolocation_zip_code_prefix::bytea), 'hex') as zip_code_prefix,
        geolocation_lat::numeric(10, 6)  as latitude,
        geolocation_lng::numeric(10, 6)  as longitude,
        geolocation_city                 as city,
        geolocation_state                as state
    from source
    order by geolocation_zip_code_prefix
)

select * from deduplicated
