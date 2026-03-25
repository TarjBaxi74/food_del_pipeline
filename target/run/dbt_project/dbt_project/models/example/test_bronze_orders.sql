
  
  create view "dev"."main"."test_bronze_orders__dbt_tmp" as (
    select *
from read_parquet('data/bronze/orders.parquet')
limit 5
  );
