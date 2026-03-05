df_transformed = df_transformed.repartition(4)

df_transformed.cache()