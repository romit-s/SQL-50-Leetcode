# Databricks notebook source
# MAGIC %md
# MAGIC ## Customer Who Visited but Did Not Make Any Transactions

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder.appName("Create Visits and Transactions DataFrames") .getOrCreate()

# Data for Visits table
data_visits = [
    (1, 23),
    (2, 9),
    (4, 30),
    (5, 54),
    (6, 96),
    (7, 54),
    (8, 54)
]

# Data for Transactions table
data_transactions = [
    (2, 5, 310),
    (3, 5, 300),
    (9, 5, 200),
    (12, 1, 910),
    (13, 2, 970)
]

# Define column names
visits_columns = ["visit_id", "customer_id"]
transactions_columns = ["transaction_id", "visit_id", "amount"]

# Create DataFrames
visits_df = spark.createDataFrame(data_visits, visits_columns)
transactions_df = spark.createDataFrame(data_transactions, transactions_columns)

# Show the DataFrames
print("Visits DataFrame:")
visits_df.show()

print("Transactions DataFrame:")
transactions_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import count
visits_df.join(transactions_df,"visit_id","left_anti").groupBy("customer_id").agg(count("*").alias("count_no_trans")).show()

# COMMAND ----------

