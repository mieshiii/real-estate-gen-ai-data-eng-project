from ..data_scraper import scrape_website

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit

spark = SparkSession.builder.appName("Bronze").getOrCreate()

#start scraping
url = 'https://www.lamudi.com.ph/house/buy/'

data = scrape_website(url)

df = spark.createDataFrame(data)
df.printSchema()
#sample abfss url
df = df.withColumn("date_ingested", lit(current_date()))
df.write.parquet("abfss://parquet@deltaformatdemostorage.dfs.core.windows.net/employees")
