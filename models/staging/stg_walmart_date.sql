with source as (
    select * 
    from {{ source('raw', 'walmart_date') }}
),

renamed as (
    select
        store_id,
        dept_id 
        store_date,
        weekly_sales,
        is_holiday,
        CURRENT_TIMESTAMP as insert_date,
        CURRENT_TIMESTAMP as update_date
    from source
)

select * from renamed
