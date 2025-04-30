# Databricks notebook source
# MAGIC %md
# MAGIC ## Big Countries

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create Country DataFrame") \
    .getOrCreate()

# Define the country data
countries_data = [
    Row(name="Afghanistan", continent="Asia", area=652230, population=25500100, gdp=20343000000),
    Row(name="Albania", continent="Europe", area=28748, population=2831741, gdp=12960000000),
    Row(name="Algeria", continent="Africa", area=2381741, population=37100000, gdp=188681000000),
    Row(name="Andorra", continent="Europe", area=468, population=78115, gdp=3712000000),
    Row(name="Angola", continent="Africa", area=1246700, population=20609294, gdp=100990000000)
]

# Create countries DataFrame
countries_df = spark.createDataFrame(countries_data)

# Show the countries DataFrame
print("Countries DataFrame:")
countries_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

countries_df.filter((countries_df.area>3000000) | (countries_df.population >25000000)).select("name","area","population").show()

# COMMAND ----------

