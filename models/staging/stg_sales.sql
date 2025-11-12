{{ config(materialized='view') }}

select
	SaleID as id,
	PropertyID as property_id,
	cast(SaleDate as timestamp) as sale_date,
	"Means of Sales" as means_of_sales,
	ClientID as client_id,
	lower("Payment Status") as payment_status
from {{ source('raw_data', 'Sales_Table') }}

