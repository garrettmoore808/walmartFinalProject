{{ config(
    materialized='table',
    unique_key='date_id'
) }}

with source as (
    select * 
    from {{ ref('stg_walmart_date') }}
),

mapped as (
    select 
        cast(to_char(store_date, 'YYYYMMDD') as integer) as date_id,
        store_date,
        case 
            when is_holiday = true then 'Y'
            else 'N'
        end as is_holiday,
        CURRENT_TIMESTAMP as insert_date,
        CURRENT_TIMESTAMP as update_date
    from source
)

select * from mapped
