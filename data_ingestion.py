from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Retail Data Pipeline") \
    .getOrCreate()
import sys

input_path = sys.argv[1]
print("File path value:", input_path)

df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

df.write.format("delta") \
  .mode("overwrite") \
  .save("dbfs:/Volumes/workspace/default/input/bronze")

df.show()