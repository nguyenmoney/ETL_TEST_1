{{ config(
    materialized='incremental',
    file_format='iceberg',
    incremental_strategy='insert_overwrite',
    partition_by=['source_month']
) }}

with src as (
    select * from {{ source('lake_bronze', 'yellow_trips') }}
    where source_month = '{{ var("batch_month") }}'
),

typed as (
    select
        cast(vendorid as int)             as vendor_id,
        tpep_pickup_datetime              as pickup_ts,
        tpep_dropoff_datetime             as dropoff_ts,
        cast(passenger_count as int)      as passenger_count,
        cast(trip_distance as double)     as trip_distance,
        cast(pulocationid as int)         as pickup_zone_id,
        cast(dolocationid as int)         as dropoff_zone_id,
        cast(payment_type as int)         as payment_type,
        cast(fare_amount as double)       as fare_amount,
        cast(tip_amount as double)        as tip_amount,
        cast(total_amount as double)      as total_amount,
        source_month,
        md5(concat_ws('|',
            cast(vendorid as string),
            cast(tpep_pickup_datetime as string),
            cast(tpep_dropoff_datetime as string),
            cast(pulocationid as string),
            cast(dolocationid as string),
            cast(trip_distance as string),
            cast(fare_amount as string),
            cast(total_amount as string)
        )) as trip_hash
    from src
),

deduped as (
    select *,
        row_number() over (partition by trip_hash order by pickup_ts) as dup_rank
    from typed
),

flagged as (
    select *,
        array_compact(array(
            case when fare_amount < 0                              then 'negative_fare' end,
            case when trip_distance <= 0                           then 'zero_distance' end,
            case when trip_distance > 200                          then 'distance_too_long' end,
            case when dropoff_ts <= pickup_ts                      then 'bad_duration' end,
            case when date_format(pickup_ts, 'yyyy-MM') <> source_month
                                                                   then 'out_of_period' end,
            case when passenger_count is null or passenger_count = 0
                                                                   then 'no_passengers' end,
            case when dup_rank > 1                                 then 'duplicate' end
        )) as reject_reasons
    from deduped
)

select
    concat(trip_hash, '-', cast(dup_rank as string)) as trip_id,
    vendor_id, pickup_ts, dropoff_ts, passenger_count, trip_distance,
    pickup_zone_id, dropoff_zone_id, payment_type,
    fare_amount, tip_amount, total_amount,
    reject_reasons,
    size(reject_reasons) = 0 as is_valid,
    source_month
from flagged