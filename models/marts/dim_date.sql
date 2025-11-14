{{ config(materialized='table') }}

with dates as (
    select distinct cast(date_trunc('day', sale_date) as date) as date
    from {{ ref('stg_sales') }}
    where sale_date is not null
    union
    select distinct cast(date_trunc('day', listed_date) as date) as date
    from {{ ref('stg_property') }}
    where listed_date is not null
    union
    select distinct cast(date_trunc('day', expense_date) as date) as date
    from {{ ref('stg_expense') }}
    where expense_date is not null
)

select
    date as date,
    extract(year from date) as year,
    extract(month from date) as month,
    extract(day from date) as day,
    to_char(date, 'YYYY-MM-DD') as date_str
from dates
order by date
