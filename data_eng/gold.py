from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit, explode, count, current_timestamp, radians, acos, cos, sin, lit, toDegrees, expr
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

df_price_metrics = df_price_metrics.withColumn("time_of_processing", lit(current_timestamp()))

df_final = df_price_metrics

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_gold')

df_exploded = df.select(col("category"), explode(col("subcategory")).alias("subcategory"))

df_cat_counts = df.groupBy("category").agg(count(lit(1)).alias("count"))
df_cat_counts = df_cat_counts.withColumn("time_of_processing", lit(current_timestamp()))

# Compute the count of each subcategory
df_subcat_counts = df_exploded.groupBy("cubcategory").agg(count(lit(1)).alias("count"))
df_subcat_counts = df_subcat_counts.withColumn("time_of_processing", lit(current_timestamp()))

df_cat_counts.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_gold_cat')

df_subcat_counts.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_gold_subcat')

reference_latitude_rad = radians(reference_latitude)
reference_longitude_rad = radians(reference_longitude)

# Convert latitude and longitude columns to radians
df = df.withColumn("Latitude_rad", radians(col("Latitude")))
df = df.withColumn("Longitude_rad", radians(col("Longitude")))

# Compute the distance between each property and the reference location using the Haversine formula
distance_expr = acos(
    sin(reference_latitude_rad) * sin(col("Latitude_rad")) +
    cos(reference_latitude_rad) * cos(col("Latitude_rad")) *
    cos(col("Longitude_rad") - reference_longitude_rad)
) * lit(6371)  # Earth radius in kilometers

# Filter properties within 100km radius
df_filtered = df.filter(distance_expr <= 100)

# Compute the average price of properties within the radius
average_price_within_radius = df_filtered.agg(avg(col("Price")).alias("AveragePrice"))