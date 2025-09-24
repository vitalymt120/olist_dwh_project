-- models/marts/mart_customer_lifetime_value.sql

select
    c.customer_unique_id,
    c.customer_state,

    -- --- Вычисляемые метрики ---
    min(o.purchase_at) as first_order_date,
    max(o.purchase_at) as last_order_date,
    count(distinct o.order_id) as number_of_orders,
    -- Берем total_price напрямую из fct_orders
    sum(o.total_price) as total_spent

from {{ ref('dim_customers') }} c

-- ИСПРАВЛЕНИЕ ЗДЕСЬ: добавляем "мост" через stg_customers
-- 1. Соединяем dim_customers с stg_customers по уникальному ID покупателя
left join {{ ref('stg_customers') }} stg_c on c.customer_unique_id = stg_c.customer_unique_id

-- 2. Соединяем stg_customers с fct_orders по ID заказа
left join {{ ref('fct_orders') }} o on stg_c.customer_id = o.customer_id

-- Группируем по уникальному покупателю, чтобы посчитать метрики для него
group by 1, 2