SELECT
    restaurant_id,
    city,
    total_orders,
    avg_prep_delay,
    avg_delivery_delay,
    breach_rate
FROM {{ ref('int_restaurant_metrics') }}
ORDER BY breach_rate DESC