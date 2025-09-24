-- models/core/fct_orders.sql

with
orders as (
    select * from {{ ref('stg_orders') }}
),

reviews as (
    select * from {{ ref('stg_order_reviews') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchase_at,
    o.approved_at,
    o.delivered_to_carrier_at,
    o.delivered_to_customer_at,
    o.estimated_delivery_at,

    -- Вычисляем длительность доставки в днях для PostgreSQL.
    -- Приводим к типу ::date, чтобы результат был в целых днях, а не в интервале.
    (o.delivered_to_customer_at::date - o.purchase_at::date) as delivery_duration_days,

    r.review_score,

    oi.total_price,
    oi.total_freight_value

from orders o

left join reviews r on o.order_id = r.order_id

left join (
    -- Агрегируем данные по товарам до уровня одного заказа,
    -- так как в fct_orders одна строка = один заказ.
    select
        order_id,
        sum(price) as total_price,
        sum(freight_value) as total_freight_value
    from order_items
    group by 1
) oi on o.order_id = oi.order_id