-- models/core/dim_customers.sql

with customers as (
    select * from {{ ref('stg_customers') }}
),
state_map as (
    select * from {{ ref('brazil_states_iso_map') }}
)
select
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    sm.iso_code as customer_state_iso
from customers c
-- ИСПРАВЛЕНИЕ ЗДЕСЬ: делаем JOIN нечувствительным к регистру и пробелам
left join state_map sm on upper(trim(c.customer_state)) = upper(trim(sm.state_code))