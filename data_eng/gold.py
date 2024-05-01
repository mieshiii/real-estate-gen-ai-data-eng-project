from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit
from pyspark.sql import Window

spark = SparkSession.builder.appName("Gold").getOrCreate()

#read silver file 
df = spark.read.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_silver')

# Create a window specification for the running average calculation
windowSpec = Window.orderBy("Date Ingested").rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("run_avg_price", avg(col("price")).over(windowSpec))

df = df.withColumn("run_avg_land_size", avg(col("land_sizes")).over(windowSpec))

df_final = df

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_gold')


