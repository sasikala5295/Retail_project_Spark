from pyspark.sql.functions import col

df = spark.read.format("delta").load("dbfs:/Volumes/workspace/default/input/bronze")
df_clean = df.dropna()

df_transformed = df_clean.withColumn(
    "revenue",
    col("quantity") * col("price")
)

df_transformed.write.format('delta') \
       .mode('overwrite') \
        .save("dbfs:/Volumes/workspace/default/input/silver")

df_transformed.show()
 