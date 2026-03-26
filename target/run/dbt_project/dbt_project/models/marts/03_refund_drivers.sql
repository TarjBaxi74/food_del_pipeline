
  
  create view "dev"."main"."03_refund_drivers__dbt_tmp" as (
    SELECT
    CASE
        WHEN sla_breach_flag = 1 THEN 'DELAY'
        WHEN delivery_success_flag = 0 THEN 'CANCELLATION'
        ELSE 'OTHER'
    END AS refund_reason,
    COUNT(order_id) AS total_orders,
    SUM(has_refund_flag) AS refunded_orders,
    SUM(has_refund_flag) * 1.0 / COUNT(order_id) AS refund_rate
FROM "dev"."main"."stg_order_facts"
GROUP BY refund_reason
  );
