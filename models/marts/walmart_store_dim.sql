{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
) }}

with incoming_data as (
    select
        store_id,
        store_type,
        store_size,
        insert_date,
        update_date,
        vrsn_start_date,
        vrsn_end_date
    from {{ ref('stg_walmart_store') }}
),

existing_records as (
    select *
    from {{ this }}
    where vrsn_end_date is null
),

changed_records as (
    select
        inc.*
    from incoming_data inc
    left join existing_records ex
        on inc.store_id = ex.store_id
    where
        ex.store_id is null  -- new store
        or inc.store_type != ex.store_type
        or inc.store_size != ex.store_size
)

-- Final output
select * from changed_records
