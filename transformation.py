from pyspark.sql.functions import col

df_clean = df.dropna()

df_transformed = df_clean.withColumn(
    "revenue",
    col("quantity") * col("price")
)

df_transformed.show()