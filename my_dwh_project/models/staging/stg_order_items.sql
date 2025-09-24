-- models/staging/stg_order_items.sql
select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date::timestamp,
    -- Явно преобразуем текстовые колонки в числовые
    price::numeric as price,
    freight_value::numeric as freight_value
from {{ source('raw_olist', 'olist_order_items_dataset') }}