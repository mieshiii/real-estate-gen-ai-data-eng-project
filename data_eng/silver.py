from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze").getOrCreate()

#read bronze file 
df = spark.read.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees')

#do cleaning and dedup
df_cleaned = df

df_deduped = df_cleaned

df_final = df_deduped

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_silver')


