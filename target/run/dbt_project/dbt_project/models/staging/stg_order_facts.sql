
  
  create view "dev"."main"."stg_order_facts__dbt_tmp" as (
    SELECT *
FROM read_parquet('../data/silver/order_facts.parquet')
  );
