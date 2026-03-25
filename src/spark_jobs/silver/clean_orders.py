import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.settings import BRONZE_DIR, SILVER_DIR


def run():

    spark = (
        SparkSession.builder
        .appName("silver-orders")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    orders = spark.read.parquet(str(BRONZE_DIR / "orders.parquet"))
    restaurants = spark.read.parquet(str(BRONZE_DIR / "restaurants.parquet"))

    w = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())

    orders = (
        orders
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    orders = (
        orders
        .join(
            restaurants.select(
                "restaurant_id",
                col("city").alias("restaurant_city")
            ),
            "restaurant_id",
            "left"
        )
    )

    orders = orders.withColumn(
        "payment_mode",
        when(col("payment_mode").isNull(), "UNKNOWN")
        .otherwise(col("payment_mode"))
    )

    orders = orders.withColumn(
        "payment_mode_missing_flag",
        when(col("payment_mode") == "UNKNOWN", 1).otherwise(0)
    )

    orders = (
        orders
        .withColumn("order_date", to_date("order_ts"))
        .withColumn("order_hour", hour("order_ts"))
        .withColumn(
            "order_value_bucket",
            when(col("order_value") < 200, "LOW")
            .when(col("order_value") < 500, "MEDIUM")
            .otherwise("HIGH")
        )
    )

    output_path = str(SILVER_DIR / "orders.parquet")

    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)

    # Convert to pandas and write with timestamp conversion
    orders_pd = orders.toPandas()
    # Convert timestamp columns to microsecond precision for compatibility
    for col_name in orders_pd.columns:
        if str(orders_pd[col_name].dtype) == 'datetime64[ns]':
            orders_pd[col_name] = orders_pd[col_name].astype('datetime64[us]')
    orders_pd.to_parquet(output_path, index=False, engine='pyarrow')

    print("✅ Silver orders parquet written")

    spark.stop()


if __name__ == "__main__":
    run()