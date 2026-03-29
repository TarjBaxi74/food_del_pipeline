
  
    
    

    create  table
      "dev"."main"."05_weekly_trends__dbt_tmp"
  
    as (
      SELECT
    DATE_TRUNC('week', order_date) AS week,
    COUNT(order_id) AS total_orders,
    SUM(delivery_success_flag) AS completed_orders,
    SUM(has_refund_flag) AS refunded_orders,
    SUM(sla_breach_flag) AS breached_orders,
    SUM(refund_amount) AS total_refund_amount
FROM "dev"."main"."stg_order_facts"
GROUP BY week
ORDER BY week
    );
  
  