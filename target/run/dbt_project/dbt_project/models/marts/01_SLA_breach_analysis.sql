
  
    
    

    create  table
      "dev"."main"."01_SLA_breach_analysis__dbt_tmp"
  
    as (
      SELECT
    city,
    time_slot,
    COUNT(order_id) AS total_orders,
    SUM(sla_breach_flag) AS breached_orders,
    SUM(sla_breach_flag) * 1.0 / COUNT(order_id) AS breach_rate
FROM "dev"."main"."int_sla_metrics"
GROUP BY city, time_slot
    );
  
  