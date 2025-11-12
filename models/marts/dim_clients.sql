{{ config(materialized='table') }}

select
    id as client_key,
    name as full_name,
    occupation,
    img_link,
    get_current_time() as loaded_at
from {{ ref('stg_clients') }}
