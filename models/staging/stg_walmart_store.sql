{{ config(materialized='view') }}

select
    cast(store_id as integer) as store_id,
    cast(store_type as varchar) as store_type,
    cast(store_size as integer) as store_size,
    current_timestamp as insert_date,
    current_timestamp as update_date
from {{ source('raw', 'walmart_store') }}
