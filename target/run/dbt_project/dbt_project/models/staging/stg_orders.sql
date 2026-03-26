
  
  create view "dev"."main"."stg_orders__dbt_tmp" as (
    SELECT *
FROM read_parquet('C:/DE_projects/food_del_pipeline/data/silver/orders.parquet')
  );
