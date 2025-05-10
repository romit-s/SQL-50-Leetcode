# Databricks notebook source
# MAGIC %md
# MAGIC ## Project Employees I

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFramesExample").getOrCreate()

# Creating the Project DataFrame
project_data = [
    Row(project_id=1, employee_id=1),
    Row(project_id=1, employee_id=2),
    Row(project_id=1, employee_id=3),
    Row(project_id=2, employee_id=1),
    Row(project_id=2, employee_id=4)
]
project_df = spark.createDataFrame(project_data)

# Creating the Employee DataFrame
employee_data = [
    Row(employee_id=1, name='Khaled', experience_years=3),
    Row(employee_id=2, name='Ali', experience_years=2),
    Row(employee_id=3, name='John', experience_years=1),
    Row(employee_id=4, name='Doe', experience_years=2)
]
employee_df = spark.createDataFrame(employee_data)

# Displaying the DataFrames
print("Project DataFrame:")
project_df.show()

print("Employee DataFrame:")
employee_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import mean
joined_df = project_df.join(employee_df,"employee_id","inner")
joined_df.groupBy("project_id").agg(mean("experience_years").alias("average_years")).show()

# COMMAND ----------

