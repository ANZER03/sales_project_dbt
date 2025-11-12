{{ config(materialized='table') }}

select
    id as property_key,
    country,
    property_type,
    bedrooms,
    bathrooms,
    square_footage,
    price,
    listed_date,
    status,
    img_link,
    location,
    get_current_time() as loaded_at
from {{ ref('stg_property') }}
