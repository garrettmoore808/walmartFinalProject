{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id']
) }}

with store_source as (
    select * 
    from {{ ref('stg_walmart_store') }}
),

date_source as (
    select * 
    from {{ ref('stg_walmart_date') }}
),

mapped as (
    select 
        s.store_id as store_id,
        d.dept_id as dept_id,
        s.store_type as store_type,
        s.store_size as store_size,
        CURRENT_TIMESTAMP as insert_date,
        CURRENT_TIMESTAMP as update_date
    from store_source s
    left join date_source d
    on d.store_id = s.store_id
)

select * from mapped
