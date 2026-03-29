
  
    
    

    create  table
      "dev"."main"."int_sla_metrics__dbt_tmp"
  
    as (
      WITH base AS (
    SELECT
        order_id,
        city,
        order_hour,
        actual_delivery_minutes,
        delay_minutes,
        sla_breach_flag,
        prep_delay_minutes
    FROM "dev"."main"."stg_order_facts"
)
SELECT
    *,
    
    CASE
        WHEN order_hour BETWEEN 11 AND 14 THEN 'LUNCH'
        WHEN order_hour BETWEEN 18 AND 22 THEN 'DINNER'
        ELSE 'OFF_PEAK'
    END AS time_slot,
    CASE
        WHEN delay_minutes <= 0 THEN 'ON_TIME'
        WHEN delay_minutes <= 10 THEN 'MINOR_DELAY'
        ELSE 'MAJOR_DELAY'
    END AS delay_bucket
FROM base
    );
  
  