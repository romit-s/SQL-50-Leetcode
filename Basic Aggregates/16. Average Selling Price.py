# Databricks notebook source
# MAGIC %md
# MAGIC ## Average Selling Price

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Create DataFrames Example").getOrCreate()

# Define the schema for the DataFrame
schema = ["id", "movie", "description", "rating"]

# Create a list of tuples representing the data
data = [
    (1, "War", "great 3D", 8.9),
    (2, "Science", "fiction", 8.5),
    (3, "irish", "boring", 6.2),
    (4, "Ice song", "Fantacy", 8.6),
    (5, "House card", "Interesting", 9.1)
]

# Create the DataFrame using the data and schema
cinema_df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame
cinema_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import sum,count,when,col
cinema_df.filter((col("id")%2==1) & (col("description")!= "boring")).show()

# COMMAND ----------

