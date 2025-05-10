# Databricks notebook source
# MAGIC %md
# MAGIC ## Number of Unique Subjects Taught by Each Teacher

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("TeacherDataFrameExample").getOrCreate()

# Creating the Teacher DataFrame
data = [
    Row(teacher_id=1, subject_id=2, dept_id=3),
    Row(teacher_id=1, subject_id=2, dept_id=4),
    Row(teacher_id=1, subject_id=3, dept_id=3),
    Row(teacher_id=2, subject_id=1, dept_id=1),
    Row(teacher_id=2, subject_id=2, dept_id=1),
    Row(teacher_id=2, subject_id=3, dept_id=1),
    Row(teacher_id=2, subject_id=4, dept_id=1)
]

teacher_df = spark.createDataFrame(data)

# Show the DataFrame
teacher_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------



# COMMAND ----------

