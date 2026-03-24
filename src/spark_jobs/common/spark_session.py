from pyspark.sql import SparkSession


def get_spark():

    spark = (
        SparkSession.builder
        .appName("food-delivery-bronze")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark