select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state

from {{ source('raw_olist', 'olist_customers_dataset') }}