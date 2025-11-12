{{ config(materialized='view') }}

select
	ExpenseID as id,
	PropertyID as property_id,
	ExpenseType as expense_type,
	cast(Amount as numeric) as amount,
	cast(Date as timestamp) as expense_date
from {{ source('raw_data', 'Expense_Table') }}
