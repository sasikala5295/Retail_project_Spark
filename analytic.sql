  df_transformed.createOrReplaceTempView("sales")

  result = spark.sql("""
SELECT product,
       SUM(revenue) as total_revenue
FROM sales
GROUP BY product
ORDER BY total_revenue DESC
""")

result.show()

result.write \
    .mode("overwrite") \
    .parquet("dbfs:/Volumes/workspace/default/input/output")