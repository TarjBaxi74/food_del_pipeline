from pyspark.sql import SparkSession
import os
import sys

os.environ['HADOOP_USER_NAME'] = 'user'
os.environ['PYSPARK_JAVA_OPTS'] = '--enable-native-access=ALL-UNNAMED'
os.environ['PYSPARK_PYTHON'] = sys.executable

spark = (
    SparkSession.builder
    .appName("food-delivery-test")
    .master("local[*]")
    .config("spark.hadoop.security.authentication", "simple")
    .config("spark.hadoop.security.authorize", "false")
    .getOrCreate()
)

df = spark.createDataFrame(
    [(1, "tarj"), (2, "pipeline")],
    ["id", "name"]
)

df.show()

spark.stop()