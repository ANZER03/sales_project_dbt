{{ config(materialized='view') }}

select 
    ClientID as id,
    Name as name,
    Occupation as occupation,
    Img as img_link
from {{ source('raw_data', 'Client_Table') }}





