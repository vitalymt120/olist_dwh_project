-- models/marts/mart_orders_details.sql

select
    -- Ключи
    oi.order_id,
    oi.product_id,
    o.customer_id, -- Берем из fct_orders
    s.seller_id,   -- Берем из dim_sellers

    -- Данные о заказе (теперь из fct_orders)
    o.order_status,
    o.purchase_at,
    o.approved_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,

    -- Данные о товаре в заказе (остаются из stg_order_items, т.к. это самая детальная таблица)
    oi.price,
    oi.freight_value,

    -- Данные о продукте (из dim_products)
    p.product_category_name,
    p.product_category_name_en,

    -- Данные о покупателе (из dim_customers)
    c.customer_city,
    c.customer_state,
    c.customer_state_iso,

    -- Данные о продавце (из dim_sellers)
    s.seller_city,
    s.seller_state,

    -- Данные об оценке (из fct_orders)
    o.review_score,

    -- --- Вычисляемые метрики ---
    -- Эта метрика уже посчитана в fct_orders, просто берем ее
    o.delivery_duration_days,

    -- Эту можно оставить здесь или перенести в fct_orders, если она нужна часто
    (o.delivered_to_customer_at::date - o.estimated_delivery_at::date) as delivery_vs_estimated_days

-- Базовая таблица остается stg_order_items, т.к. нам нужна детализация по товарам
from {{ ref('stg_order_items') }} oi

-- Присоединяем готовую модель фактов по заказам. Она уже содержит данные из stg_orders и stg_order_reviews.
left join {{ ref('fct_orders') }} o on oi.order_id = o.order_id

-- Присоединяем готовые модели измерений
left join {{ ref('dim_products') }} p on oi.product_id = p.product_id
left join {{ ref('dim_sellers') }} s on oi.seller_id = s.seller_id

-- Для присоединения покупателя нам нужен customer_id из заказа (который есть в fct_orders)
left join {{ ref('stg_customers') }} stg_c on o.customer_id = stg_c.customer_id
left join {{ ref('dim_customers') }} c on stg_c.customer_unique_id = c.customer_unique_id