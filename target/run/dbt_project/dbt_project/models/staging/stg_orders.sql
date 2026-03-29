
  
  create view "dev"."main"."stg_orders__dbt_tmp" as (
    SELECT *
FROM read_parquet('../data/silver/orders.parquet')
  );
