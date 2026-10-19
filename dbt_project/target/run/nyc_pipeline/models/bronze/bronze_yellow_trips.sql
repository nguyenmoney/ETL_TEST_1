
      
        
        
        delete from "nyc_taxi"."bronze"."bronze_yellow_trips" as DBT_INTERNAL_DEST
        where (source_month) in (
            select distinct source_month
            from "bronze_yellow_trips__dbt_tmp180927064913" as DBT_INTERNAL_SOURCE
        );

    

    insert into "nyc_taxi"."bronze"."bronze_yellow_trips" ("vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", "source_month", "loaded_at", "_source_table", "_ingested_at")
    (
        select "vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "ratecodeid", "store_and_fwd_flag", "pulocationid", "dolocationid", "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge", "airport_fee", "source_month", "loaded_at", "_source_table", "_ingested_at"
        from "bronze_yellow_trips__dbt_tmp180927064913"
    )
  