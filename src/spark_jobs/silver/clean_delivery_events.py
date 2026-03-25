from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from config.settings import BRONZE_DIR, SILVER_DIR


def run():

    spark = (
        SparkSession.builder
        .appName("silver-delivery-events")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    events = spark.read.parquet(str(BRONZE_DIR / "delivery_events.parquet"))

    # ⭐ Deduplicate per order + event
    w = Window.partitionBy("order_id", "event_type").orderBy(col("event_ts"))

    events = (
        events
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # ⭐ Pivot lifecycle
    timeline = (
        events
        .groupBy("order_id")
        .pivot("event_type")
        .agg(min("event_ts"))
    )

    # ⭐ Rename columns based on your event naming
    timeline = (
        timeline
        .withColumnRenamed("ASSIGNED", "accepted_ts")
        .withColumnRenamed("PICKED_UP", "picked_ts")
        .withColumnRenamed("DELIVERED", "delivered_ts")
    )

    # ⭐ Latest rider assignment
    rider_df = (
        events
        .filter(col("rider_id").isNotNull())
        .withColumn(
            "rn",
            row_number().over(
                Window.partitionBy("order_id").orderBy(col("event_ts").desc())
            )
        )
        .filter(col("rn") == 1)
        .select("order_id", "rider_id")
    )

    timeline = timeline.join(rider_df, "order_id", "left")

    # ⭐ Duration Metrics
    timeline = (
        timeline
        .withColumn(
            "rider_wait_minutes",
            (unix_timestamp("picked_ts") - unix_timestamp("accepted_ts")) / 60
        )
        .withColumn(
            "pickup_to_delivery_minutes",
            (unix_timestamp("delivered_ts") - unix_timestamp("picked_ts")) / 60
        )
    )

    # ⭐ Delivery Status
    timeline = timeline.withColumn(
        "delivery_status",
        when(col("delivered_ts").isNull(), "INCOMPLETE")
        .otherwise("COMPLETED")
    )

    # ⭐ Write via pandas with timestamp conversion
    output_path = str(SILVER_DIR / "delivery_timeline.parquet")

    timeline_pd = timeline.toPandas()
    # Convert timestamp columns to microsecond precision for compatibility
    for col_name in timeline_pd.columns:
        if str(timeline_pd[col_name].dtype) == 'datetime64[ns]':
            timeline_pd[col_name] = timeline_pd[col_name].astype('datetime64[us]')
    timeline_pd.to_parquet(output_path, index=False, engine='pyarrow')

    print("✅ Silver delivery timeline created")

    spark.stop()


if __name__ == "__main__":
    run()