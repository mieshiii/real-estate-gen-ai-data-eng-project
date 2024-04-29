from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id, current_date, current_timestamp
from pyspark.sql import Window

spark = SparkSession.builder.appName("Silver").getOrCreate()

#read bronze file 
df = spark.read.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees')

#add index and date ingested to the original df
df = df.withColumn(
        "index",
        row_number().over(Window.orderBy(monotonically_increasing_id()))-1
      )

df = df.withColumn(
 "date_ingested", current_date()
)
df = df.withColumn(
  "time_of_ingestion", current_timestamp()
)

#df cleaning and dedup
df_cleaned = df.withColumn("Price", df["Price"].cast("int"))
df_cleaned = df.withColumn("Building Size", df["Building Size"].cast("int"))
df_cleaned = df.withColumn("Land Size", df["Land Size"].cast("int"))

df_cleaned = df_cleaned \
    .withColumnRenamed("Price", "price") \
    .withColumnRenamed("Category", "category") \
    .withColumnRenamed("Subcategory", "subcategory") \
    .withColumnRenamed("Year Built", "year_built") \
    .withColumnRenamed("Furnished", "furnished") \
    .withColumnRenamed("Bedrooms", "bedrooms") \
    .withColumnRenamed("Building Size", "building_sizes") \
    .withColumnRenamed("Land Size", "land_sizes") \
    .withColumnRenamed("Subdivision Name", "subdivision_names") \
    .withColumnRenamed("SKU", "sku") \
    .withColumnRenamed("Geo Point", "geo_points") \
    .withColumnRenamed("New Development", "new_development") \
    .withColumnRenamed("Price", "price") \
    .withColumnRenamed("Price", "price")


df_deduped = df_cleaned.dropDuplicates(["SKU"])

df_final = df_deduped

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_silver')


