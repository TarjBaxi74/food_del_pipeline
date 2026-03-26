
  
  create view "dev"."main"."int_restaurants_metrics__dbt_tmp" as (
    SELECT
    restaurant_id,
    city,
    COUNT(order_id) AS total_orders,
    AVG(prep_delay_minutes) AS avg_prep_delay,
    AVG(delay_minutes) AS avg_delivery_delay,
    SUM(sla_breach_flag) * 1.0 / COUNT(order_id) AS breach_rate
FROM "dev"."main"."stg_order_facts"
GROUP BY restaurant_id, city
  );
