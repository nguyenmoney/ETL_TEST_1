

select
    *,
    'raw.yellow_trips'  as _source_table,
    current_timestamp   as _ingested_at
from "nyc_taxi"."raw"."yellow_trips"
where source_month = '2024-01'