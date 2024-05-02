from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, explode, count
from pyspark.sql import Window

spark = SparkSession.builder.appName("Gold").getOrCreate()

#read silver file 
df = spark.read.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_silver')

# Create a window specification for the running average calculation
windowSpec = Window.orderBy("Date Ingested").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_price_metrics = df.withColumn("run_avg_price", avg(col("price")).over(windowSpec))

df_price_metrics = df_price_metrics.withColumn("run_avg_land_size", avg(col("land_sizes")).over(windowSpec))


df_price_metrics = df_price_metrics.withColumn("price_per_land_size", col("Price") / col("Land Size"))


df_price_metrics = df_price_metrics.withColumn("price_per_bldg_size", col("Price") / col("Building Size"))

df_final = df_price_metrics

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_gold')
