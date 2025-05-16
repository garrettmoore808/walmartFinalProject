{{ config(
    materialized='incremental',
    unique_key='date_id'
) }}

with source as (
    select * 
    from {{ ref('stg_walmart_date') }}
),

mapped as (
    select 
        cast(
            to_char(store_date, 'YYYYMMDD') || 
            lpad(cast(store_id as varchar), 5, '0') || 
            lpad(cast(dept_id as varchar), 3, '0')
        as bigint) as date_id,
        cast(store_date as date) as store_date,
        case 
            when is_holiday = true then 'Y'
            else 'N'
        end as is_holiday,
        CURRENT_TIMESTAMP as insert_date,
        CURRENT_TIMESTAMP as update_date
    from source
)

select * from mapped
