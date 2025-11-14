{{ config(materialized='incremental', unique_key='expense_id') }}

select
    id as expense_id,
    property_id,
    expense_type,
    amount,
    expense_date,
    current_timestamp as loaded_at
from {{ ref('stg_expense') }}

{% if is_incremental() %}
where expense_date > (select coalesce(max(expense_date), '1970-01-01'::timestamp) from {{ this }})
{% endif %}
