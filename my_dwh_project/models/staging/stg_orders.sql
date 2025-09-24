-- models/staging/stg_orders.sql
select
    order_id,
    customer_id,
    order_status,
    -- Явно преобразуем все колонки с датами в формат timestamp
    order_purchase_timestamp::timestamp as purchase_at,
    order_approved_at::timestamp as approved_at,
    order_delivered_carrier_date::timestamp as delivered_to_carrier_at,
    order_delivered_customer_date::timestamp as delivered_to_customer_at,
    order_estimated_delivery_date::timestamp as estimated_delivery_at
from {{ source('raw_olist', 'olist_orders_dataset') }}