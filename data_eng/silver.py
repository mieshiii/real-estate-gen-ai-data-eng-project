from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, monotonically_increasing_id, current_date, current_timestamp, col, split, regexp_extract, when, lit
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
  "time_of_processing", lit(current_timestamp())
)

#df cleaning and dedup
# Split the 'Geo Point' column into latitude and longitude columns
df_cleaned = df.withColumn("Latitude", split(col("Geo Point"), ",")[0].cast("double"))
df_cleaned = df_cleaned.withColumn("Longitude", split(col("Geo Point"), ",")[1].cast("double"))
df_cleaned = df_cleaned.withColumn("Bedrooms", col("Bedrooms").cast("int"))
df_cleaned = df_cleaned.withColumn("Bathrooms", col("Bathrooms").cast("int"))
df_cleaned = df_cleaned.withColumn("Price", df["Price"].cast("double"))
df_cleaned = df_cleaned.withColumn("Building Size", df["Building Size"].cast("double"))
df_cleaned = df_cleaned.withColumn("Land Size", df["Land Size"].cast("double"))

df_cleaned = df_cleaned \
    .withColumnRenamed("Price", "price") \
    .withColumnRenamed("Category", "category") \
    .withColumnRenamed("Subcategory", "subcategory") \
    .withColumnRenamed("Year Built", "year_built") \
    .withColumnRenamed("Furnished", "furnished") \
    .withColumnRenamed("Bedrooms", "bedrooms") \
    .withColumnRenamed("Bathrooms", "bathrooms") \
    .withColumnRenamed("Building Size", "building_sizes") \
    .withColumnRenamed("Land Size", "land_sizes") \
    .withColumnRenamed("Subdivision Name", "subdivision_names") \
    .withColumnRenamed("SKU", "sku") \
    .withColumnRenamed("Latitude", "latitude") \
    .withColumnRenamed("Longitude", "longitude") \
    .withColumnRenamed("New Development", "new_development")

df_deduped = df_cleaned.dropDuplicates(["SKU"])

df_final = df_deduped

#test silver parquet file and url
df_final.write.parquet('abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees_silver')


