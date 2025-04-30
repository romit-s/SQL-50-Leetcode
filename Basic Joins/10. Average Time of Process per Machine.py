# Databricks notebook source
# MAGIC %md
# MAGIC ## Average Time of Process per Machine

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Activity DataFrame").getOrCreate()

# Data for the Activity table
activity_data = [
    (0, 0, "start", 0.712),
    (0, 0, "end", 1.520),
    (0, 1, "start", 3.140),
    (0, 1, "end", 4.120),
    (1, 0, "start", 0.550),
    (1, 0, "end", 1.550),
    (1, 1, "start", 0.430),
    (1, 1, "end", 1.420),
    (2, 0, "start", 4.100),
    (2, 0, "end", 4.512),
    (2, 1, "start", 2.500),
    (2, 1, "end", 5.000)
]

# Define column names
activity_columns = ["machine_id", "process_id", "activity_type", "timestamp"]

# Create DataFrame
activity_df = spark.createDataFrame(activity_data, activity_columns)

# Show the DataFrame
activity_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col, avg,round
joined_df = activity_df.alias("a1").join(
    activity_df.alias("a2"), 
    (col("a1.machine_id") == col("a2.machine_id")) & 
    (col("a1.process_id") == col("a2.process_id")) & 
    (col("a1.timestamp") < col("a2.timestamp")), 
    "inner"
)


# COMMAND ----------

joined_df.groupBy("a1.machine_id").agg(round(avg("a2.timestamp")-avg("a1.timestamp"),3).alias("average")).show()

# COMMAND ----------

