select *
from read_parquet('data/bronze/orders.parquet')
limit 5