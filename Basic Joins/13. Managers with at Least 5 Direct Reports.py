# Databricks notebook source
# MAGIC %md
# MAGIC ## Managers with at Least 5 Direct Reports

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create DataFrame Example") \
    .getOrCreate()

# Sample data as a list of tuples
data = [
    (101, "John", "A", None),
    (102, "Dan", "A", 101),
    (103, "James", "A", 101),
    (104, "Amy", "A", 101),
    (105, "Anne", "A", 101),
    (106, "Ron", "B", 101)
]

# Define column names
columns = ["id", "name", "department", "managerId"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Show the DataFrame
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col
count_df = df.groupBy("managerId").count().filter("count>4")
count_df.join(df,col("count_df.managerId") == col("df.id"),"inner")

# COMMAND ----------

