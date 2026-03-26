SELECT
    rider_id,
    COUNT(order_id) AS total_orders,
    SUM(sla_breach_flag) AS late_orders,
    SUM(delivery_success_flag) AS successful_orders,
    AVG(actual_delivery_minutes) AS avg_delivery_time,
    AVG(delay_minutes) AS avg_delay_minutes
FROM "dev"."main"."stg_order_facts"
WHERE rider_id IS NOT NULL
GROUP BY rider_id