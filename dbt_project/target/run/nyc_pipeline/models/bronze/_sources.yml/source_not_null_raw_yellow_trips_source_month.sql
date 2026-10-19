
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source_month
from "nyc_taxi"."raw"."yellow_trips"
where source_month is null



  
  
      
    ) dbt_internal_test