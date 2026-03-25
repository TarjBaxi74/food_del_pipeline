from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from config.settings import SILVER_DIR, BRONZE_DIR


def run():

    spark = (
        SparkSession.builder
        .appName("silver-order-facts")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    orders = spark.read.parquet(str(SILVER_DIR / "orders.parquet"))
    timeline = spark.read.parquet(str(SILVER_DIR / "delivery_timeline.parquet"))
    refunds = spark.read.parquet(str(BRONZE_DIR / "refunds.parquet"))

    # ⭐ Join orders + timeline
    df = orders.join(timeline, "order_id", "left")

    # ⭐ Actual delivery duration
    df = df.withColumn(
        "actual_delivery_minutes",
        (unix_timestamp("delivered_ts") - unix_timestamp("order_ts")) / 60
    )

    # ⭐ SLA Delay
    df = df.withColumn(
        "delay_minutes",
        (unix_timestamp("delivered_ts") - unix_timestamp("promised_delivery_ts")) / 60
    )

    df = df.withColumn(
        "sla_breach_flag",
        when(col("delay_minutes") > 0, 1).otherwise(0)
    )

    # ⭐ Refund Aggregation
    refund_agg = (
        refunds
        .groupBy("order_id")
        .agg(
            sum("refund_amount").alias("refund_amount")
        )
        .withColumn("has_refund_flag", lit(1))
    )

    df = df.join(refund_agg, "order_id", "left")

    df = df.fillna({
        "refund_amount": 0,
        "has_refund_flag": 0
    })

    # ⭐ Delivery success
    df = df.withColumn(
        "delivery_success_flag",
        when(col("delivered_ts").isNull(), 0).otherwise(1)
    )

    # ⭐ Rider assigned
    df = df.withColumn(
        "rider_assigned_flag",
        when(col("rider_id").isNull(), 0).otherwise(1)
    )

    # ⭐ Write via pandas with timestamp conversion
    output_path = str(SILVER_DIR / "order_facts.parquet")

    df_pd = df.toPandas()
    # Convert timestamp columns to microsecond precision for compatibility
    for col_name in df_pd.columns:
        if str(df_pd[col_name].dtype) == 'datetime64[ns]':
            df_pd[col_name] = df_pd[col_name].astype('datetime64[us]')
    df_pd.to_parquet(output_path, index=False, engine='pyarrow')

    print("✅ Silver order facts created")

    spark.stop()


if __name__ == "__main__":
    run()