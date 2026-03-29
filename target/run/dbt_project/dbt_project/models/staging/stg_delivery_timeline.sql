
  
    
    

    create  table
      "dev"."main"."stg_delivery_timeline__dbt_tmp"
  
    as (
      SELECT *
FROM read_parquet('../data/silver/delivery_timeline.parquet')
    );
  
  