-- models/marts/mart_product_performance.sql

select
    p.product_category_name,

    -- --- Вычисляемые метрики ---
    sum(oi.price) as total_revenue,
    count(oi.order_item_id) as items_sold,
    count(distinct o.order_id) as number_of_orders,
    avg(r.review_score) as average_review_score,
    min(r.review_score) as min_review_score,
    max(r.review_score) as max_review_score

from {{ ref('stg_products') }} p
left join {{ ref('stg_order_items') }} oi on p.product_id = oi.product_id
left join {{ ref('stg_orders') }} o on oi.order_id = o.order_id
left join {{ ref('stg_order_reviews') }} r on o.order_id = r.order_id

-- Исключаем товары без категории, чтобы не портить статистику
where p.product_category_name is not null

group by 1