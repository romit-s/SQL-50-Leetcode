# Databricks notebook source
# MAGIC %md
# MAGIC ## Employee Bonus

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Employee and Bonus DataFrames") \
    .getOrCreate()

# Data for the Employee table
employee_data = [
    (3, "Brad", None, 4000),
    (1, "John", 3, 1000),
    (2, "Dan", 3, 2000),
    (4, "Thomas", 3, 4000)
]

# Define column names for Employee table
employee_columns = ["empId", "name", "supervisor", "salary"]

# Create DataFrame for Employee table
employee_df = spark.createDataFrame(employee_data, employee_columns)

# Data for the Bonus table
bonus_data = [
    (2, 500),
    (4, 2000)
]

# Define column names for Bonus table
bonus_columns = ["empId", "bonus"]

# Create DataFrame for Bonus table
bonus_df = spark.createDataFrame(bonus_data, bonus_columns)

# Show the DataFrames
employee_df.show()
bonus_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

employee_df.join(bonus_df,"empId", "left").filter("bonus is null or  bonus <1000").select("name","bonus").show()

# COMMAND ----------

