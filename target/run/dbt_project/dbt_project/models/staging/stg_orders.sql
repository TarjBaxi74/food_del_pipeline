
  
  create view "dev"."main"."stg_orders__dbt_tmp" as (
    select *
from read_parquet('data/bronze/orders.parquet')
  );
