# Databricks notebook source
# MAGIC %md
# MAGIC ## Classes More Than 5 Students

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("CoursesDataFrameExample").getOrCreate()

# Creating the Courses DataFrame
data = [
    Row(student="A", class_="Math"),
    Row(student="B", class_="English"),
    Row(student="C", class_="Math"),
    Row(student="D", class_="Biology"),
    Row(student="E", class_="Math"),
    Row(student="F", class_="Computer"),
    Row(student="G", class_="Math"),
    Row(student="H", class_="Math"),
    Row(student="I", class_="Math")
]
courses_df = spark.createDataFrame(data)

# Show the DataFrame
courses_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col
courses_df.groupBy("class_").count().filter(col("count")!=1).show()

# COMMAND ----------

