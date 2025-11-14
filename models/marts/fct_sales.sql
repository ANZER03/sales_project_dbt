{{ config(materialized='incremental', unique_key='sale_id') }}

with src as (
    select
        id,
        property_id,
        client_id,
        sale_date,
        means_of_sales,
        payment_status
    from {{ ref('stg_sales') }}
)

select
    id as sale_id,
    property_id,
    client_id,
    sale_date,
    means_of_sales,
    payment_status,
    current_timestamp as loaded_at
from src

{% if is_incremental() %}
where sale_date > (select coalesce(max(sale_date), '1970-01-01'::timestamp) from {{ this }})
{% endif %}
