with source as (
    select * from {{ source('raw', 'order_reviews') }}
)

select
    review_id,
    order_id,
    review_score::int                  as review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date::timestamp    as created_at,
    review_answer_timestamp::timestamp as answered_at
from source
