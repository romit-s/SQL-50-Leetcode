# Databricks notebook source
# MAGIC %md
# MAGIC ## Replace Employee ID With The Unique Identifier

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create Employees and EmployeeUNI DataFrames") \
    .getOrCreate()

# Data for Employees table
employees_data = [
    (1, "Alice"),
    (7, "Bob"),
    (11, "Meir"),
    (90, "Winston"),
    (3, "Jonathan")
]

# Data for EmployeeUNI table
employeeuni_data = [
    (3, 1),
    (11, 2),
    (90, 3)
]

# Define column names
employees_columns = ["id", "name"]
employeeuni_columns = ["id", "unique_id"]

# Create DataFrames
employees_df = spark.createDataFrame(employees_data, employees_columns)
employeeuni_df = spark.createDataFrame(employeeuni_data, employeeuni_columns)

# Show the DataFrames
print("Employees DataFrame:")
employees_df.show()

print("EmployeeUNI DataFrame:")
employeeuni_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

employees_df.join(employeeuni_df,"id","left").select("unique_id","name").show()

# COMMAND ----------

