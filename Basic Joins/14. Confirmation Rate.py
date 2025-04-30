# Databricks notebook source
# MAGIC %md
# MAGIC ## Confirmation Rate

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Create DataFrames Example").getOrCreate()

# Sample data for Signups table as a list of tuples
signups_data = [
    (3, "2020-03-21 10:16:13"),
    (7, "2020-01-04 13:57:59"),
    (2, "2020-07-29 23:09:44"),
    (6, "2020-12-09 10:39:37")
]

# Define column names for Signups
signups_columns = ["user_id", "time_stamp"]

# Create DataFrame for Signups
signups_df = spark.createDataFrame(signups_data, signups_columns)

# Sample data for Confirmations table as a list of tuples
confirmations_data = [
    (3, "2021-01-06 03:30:46", "timeout"),
    (3, "2021-07-14 14:00:00", "timeout"),
    (7, "2021-06-12 11:57:29", "confirmed"),
    (7, "2021-06-13 12:58:28", "confirmed"),
    (7, "2021-06-14 13:59:27", "confirmed"),
    (2, "2021-01-22 00:00:00", "confirmed"),
    (2, "2021-02-28 23:59:59", "timeout")
]

# Define column names for Confirmations
confirmations_columns = ["user_id", "time_stamp", "action"]

# Create DataFrame for Confirmations
confirmations_df = spark.createDataFrame(confirmations_data, confirmations_columns)

# Show the DataFrames
print("Signups DataFrame:")
signups_df.show()

print("Confirmations DataFrame:")
confirmations_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

joined_df = signups_df.join(confirmations_df,"user_id","left")

# COMMAND ----------

from pyspark.sql.functions import sum,count,when
count_df = joined_df.groupBy("user_id").agg([count("*").alias("total_count"),sum(when(col("action")=="confirmed",1).otherwise(0)).alias("confirmed_count")])

# COMMAND ----------

