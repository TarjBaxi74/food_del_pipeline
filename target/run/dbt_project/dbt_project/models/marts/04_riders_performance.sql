
  
  create view "dev"."main"."04_riders_performance__dbt_tmp" as (
    SELECT
    rider_id,
    total_orders,
    late_orders,
    successful_orders,
    avg_delivery_time,
    avg_delay_minutes,
    late_orders * 1.0 / total_orders AS late_rate
FROM "dev"."main"."int_rider_metrics"
ORDER BY total_orders DESC
  );
