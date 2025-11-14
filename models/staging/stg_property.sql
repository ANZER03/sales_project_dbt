{{ config(materialized='view') }}

select
	"PropertyID" as id,
	"Country" as country,
	"Type" as property_type,
	cast("Bedrooms" as integer) as bedrooms,
	cast("Bathrooms" as integer) as bathrooms,
	cast("SquareFootage" as integer) as square_footage,
	cast("Price" as numeric) as price,
	cast("ListedDate" as timestamp) as listed_date,
	"Status" as status,
	"Img" as img_link,
	"Location" as location
from {{ source('raw_data', 'Property_Table') }}
