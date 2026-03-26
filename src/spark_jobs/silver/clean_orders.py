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

    # ⭐ Deduplicate latest record per order
    w = Window.partitionBy("order_id").orderBy(col("ingestion_ts").desc())

    orders = (
        orders
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ⭐ Join restaurant city
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

    # ⭐ FIX — Replace NULL city with restaurant_city
    orders = orders.withColumn(
        "city",
        col("restaurant_city")
    )

    orders = orders.drop("restaurant_city")

    # ⭐ Payment mode fix
    orders = orders.withColumn(
        "payment_mode",
        when(col("payment_mode").isNull(), "UNKNOWN")
        .otherwise(col("payment_mode"))
    )

    orders = orders.withColumn(
        "payment_mode_missing_flag",
        when(col("payment_mode") == "UNKNOWN", 1).otherwise(0)
    )

    # ⭐ Derived features
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
        .withColumn(
            "load_date",
            to_date("ingestion_ts")
        )
    )   

    # ⭐ Stable parquet write
    output_path = str(SILVER_DIR / "orders.parquet")

    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)

    orders_pd = orders.toPandas()

    for c in orders_pd.columns:
        if str(orders_pd[c].dtype) == 'datetime64[ns]':
            orders_pd[c] = orders_pd[c].astype('datetime64[us]')

    orders_pd.to_parquet(output_path, index=False, engine='pyarrow')

    print("✅ Silver orders parquet written (city fixed)")

    spark.stop()


if __name__ == "__main__":
    run()