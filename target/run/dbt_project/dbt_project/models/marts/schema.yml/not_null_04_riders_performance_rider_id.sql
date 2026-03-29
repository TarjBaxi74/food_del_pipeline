
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rider_id
from "dev"."main"."04_riders_performance"
where rider_id is null



  
  
      
    ) dbt_internal_test