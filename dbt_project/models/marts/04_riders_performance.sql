SELECT
    rider_id,
    total_orders,
    late_orders,
    successful_orders,
    avg_delivery_time,
    avg_delay_minutes,
    late_orders * 1.0 / total_orders AS late_rate
FROM {{ ref('int_rider_metrics') }}
ORDER BY total_orders DESC