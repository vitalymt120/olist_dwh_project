-- models/staging/stg_order_reviews.sql

-- Используем CTE (Common Table Expression), чтобы сначала вычислить номер строки,
-- а потом отфильтровать по нему. Это стандартный способ для PostgreSQL.
with source_with_rownum as (
    select
        order_id,
        review_score,
        row_number() over (partition by order_id order by review_creation_date desc) as rn
    from {{ source('raw_olist', 'olist_order_reviews_dataset') }}
)

select
    order_id,
    review_score::numeric as review_score
    from source_with_rownum
where rn = 1