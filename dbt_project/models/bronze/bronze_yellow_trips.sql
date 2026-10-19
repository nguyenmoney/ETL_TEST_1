{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='source_month'
) }}

select
    *,
    'raw.yellow_trips'  as _source_table,
    current_timestamp   as _ingested_at
from {{ source('raw', 'yellow_trips') }}
where source_month = '{{ var("batch_month") }}'