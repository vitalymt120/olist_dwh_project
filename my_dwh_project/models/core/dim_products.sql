-- models/core/dim_products.sql
select
    p.product_id,
    p.product_category_name,
    t.product_category_name_english as product_category_name_en -- Добавляем перевод
from {{ ref('stg_products') }} p
left join {{ source('raw_olist', 'product_category_name_translation') }} t
  on p.product_category_name = t.product_category_name