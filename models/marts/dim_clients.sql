{{ config(materialized='table') }}

select
    id as client_key,
    name as full_name,
    occupation,
    img_link,
    current_timestamp as loaded_at
from {{ ref('stg_clients') }}
