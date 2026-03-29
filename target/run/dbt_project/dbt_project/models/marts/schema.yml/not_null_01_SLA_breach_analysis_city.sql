
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select city
from "dev"."main"."01_SLA_breach_analysis"
where city is null



  
  
      
    ) dbt_internal_test