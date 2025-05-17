{{ config(
    materialized='incremental',
    unique_key=['store_id', 'dept_id']
) }}

with store_source as (
    select 
        store_id,
        store_type,
        store_size
    from {{ ref('stg_walmart_store') }}
),

date_source as (
    select 
        store_id,
        dept_id,
    from {{ ref('stg_walmart_date') }}
),

ranked as (
    select 
        s.store_id as store_id,
        d.dept_id as dept_id,
        s.store_type as store_type,
        s.store_size as store_size,
        CURRENT_TIMESTAMP as insert_date,
        CURRENT_TIMESTAMP as update_date,
        row_number() over (
            partition by s.store_id, d.dept_id 
            order by update_date desc, insert_date desc
        ) as rn
    from store_source s
    left join date_source d
        on d.store_id = s.store_id
),

mapped as (
    select 
        store_id,
        dept_id,
        store_type,
        store_size,
        insert_date,
        update_date
    from ranked
    where rn = 1
)

select * from mapped
