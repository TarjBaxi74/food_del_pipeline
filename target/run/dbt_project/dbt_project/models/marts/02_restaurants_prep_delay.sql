
  
    
    

    create  table
      "dev"."main"."02_restaurants_prep_delay__dbt_tmp"
  
    as (
      SELECT
    restaurant_id,
    city,
    total_orders,
    avg_prep_delay,
    avg_delivery_delay,
    breach_rate
FROM "dev"."main"."int_restaurant_metrics"
ORDER BY breach_rate DESC
    );
  
  