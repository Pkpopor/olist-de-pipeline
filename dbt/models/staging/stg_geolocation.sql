SELECT
    -- Hash to match zip_code_prefix in customers/sellers (both SHA-256 hashed at ingest)
    encode(sha256(geolocation_zip_code_prefix::bytea), 'hex') as zip_code_prefix,
    AVG(geolocation_lat)::numeric(10, 6)                      as latitude,
    AVG(geolocation_lng)::numeric(10, 6)                      as longitude,
    MODE() WITHIN GROUP (ORDER BY geolocation_city)           as city,
    MODE() WITHIN GROUP (ORDER BY geolocation_state)          as state
FROM {{ source('raw', 'geolocation') }}
GROUP BY geolocation_zip_code_prefix
