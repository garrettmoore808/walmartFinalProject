WITH source AS (
    SELECT *
    FROM {{ source('raw', 'walmart_sales') }}
),

renamed AS (
    select
        cast(store_date as date) as date_id,
        cast(store_id as integer) as store_id,
        cast(fuel_price as decimal(10,4)) as fuel_price,
        cast(temperature as decimal(10,2)) as store_temperature,
        cast(unemployment as decimal(10,2)) as unemployment,
        cast(cpi as decimal(10,2)) as cpi,
        cast(markdown1 as decimal(10,2)) as markdown1,
        cast(markdown2 as decimal(10,2)) as markdown2,
        cast(markdown3 as decimal(10,2)) as markdown3,
        cast(markdown4 as decimal(10,2)) as markdown4,
        cast(markdown5 as decimal(10,2)) as markdown5,
        is_holiday,
        CURRENT_TIMESTAMP as insert_date, 
        CURRENT_TIMESTAMP as update_date 
    from source
)

SELECT * FROM renamed