from pyspark.sql.types import *


orders_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("restaurant_id", IntegerType()),
    StructField("city", StringType()),
    StructField("order_ts", TimestampType()),
    StructField("promised_delivery_ts", TimestampType()),
    StructField("status", StringType()),
    StructField("order_value", DoubleType()),
    StructField("payment_mode", StringType())
])


delivery_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("rider_id", IntegerType()),
    StructField("event_type", StringType()),
    StructField("event_ts", TimestampType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType())
])