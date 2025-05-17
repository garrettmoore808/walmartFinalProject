{{ config(
    materialized='incremental',
    unique_key=['date_id', 'store_id', 'dept_id']
) }}

with store_dim as (
    select *
    from {{ ref('walmart_store_dim') }}
),

sales_source as (
    select *
    from {{ ref('stg_walmart_sales') }}
),

stg_date as (
    select *
    from {{ ref('stg_walmart_date') }}
),

joined as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.store_id', 's.dept_id', 'ss.date_id']) }} as date_id,
        s.store_id,
        s.dept_id,
        s.store_size,
        ss.fuel_price,
        ss.store_temperature,
        ss.unemployment,
        ss.CPI,
        ss.Markdown1,
        ss.Markdown2,
        ss.Markdown3,
        ss.Markdown4,
        ss.Markdown5
    from sales_source ss
    left join store_dim s
        on s.store_id = ss.store_id
),

final as (
    select 
        j.*,
        d.weekly_sales, 
        current_timestamp as vrsn_start_date,
        current_timestamp as vrsn_end_date,
        current_timestamp as insert_date,
        current_timestamp as update_date
    from joined j
    left join stg_date d
        on j.date_id = {{ dbt_utils.generate_surrogate_key(['d.store_id', 'd.dept_id', 'd.store_date']) }}
)

select * from final
