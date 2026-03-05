from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Retail Data Pipeline") \
    .getOrCreate()

df = spark.read.csv(
    "dbfs:/Volumes/workspace/default/input/orders/orders1_week20.txt",
    header=True,
    inferSchema=True
)

df.show()