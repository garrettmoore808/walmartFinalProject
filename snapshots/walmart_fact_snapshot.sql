{% snapshot walmart_fact_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key=['store_id', 'dept_id', 'date_id'],
        strategy='check',
        check_cols=[
            'weekly_sales',
            'fuel_price',
            'store_temperature',
            'unemployment',
            'CPI',
            'Markdown1',
            'Markdown2',
            'Markdown3',
            'Markdown4',
            'Markdown5'
        ]
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['s.store_id', 's.dept_id', 'd.store_date']) }} as date_id,
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
    ss.Markdown5,
    d.weekly_sales
from {{ ref('stg_walmart_sales') }} ss
left join {{ ref('walmart_store_dim') }} s
    on s.store_id = ss.store_id
left join {{ ref('stg_walmart_date') }} d
    on {{ dbt_utils.generate_surrogate_key(['d.store_id', 'd.dept_id', 'd.store_date']) }} = {{ dbt_utils.generate_surrogate_key(['s.store_id', 's.dept_id', 'ss.date_id']) }}

{% endsnapshot %}
