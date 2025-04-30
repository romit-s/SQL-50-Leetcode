# Databricks notebook source
# MAGIC %md
# MAGIC ## Rising Temperature

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create Weather DataFrame") \
    .getOrCreate()

# Data for Weather table
weather_data = [
    (1, "2015-01-01", 10),
    (2, "2015-01-02", 25),
    (3, "2015-01-03", 20),
    (4, "2015-01-04", 30)
]

# Define column names
weather_columns = ["id", "recordDate", "temperature"]

# Create DataFrame
weather_df = spark.createDataFrame(weather_data, weather_columns)
weather_df = weather_df.withColumn("recordDate", to_date(col("recordDate")))
# Show the DataFrame
print("Weather DataFrame:")
weather_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------


joined_df = weather_df.alias("w1").join(weather_df.alias("w2"), col("w2.recordDate") == col("w1.recordDate")+1, "inner").filter("w1.temperature<w2.temperature").select("w2.id").show()

# COMMAND ----------

