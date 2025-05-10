# Databricks notebook source
# MAGIC %md
# MAGIC ## Percentage of Users Attended a Contest

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("DataFramesExample").getOrCreate()

# Creating the Users DataFrame
users_data = [
    Row(user_id=6, user_name='Alice'),
    Row(user_id=2, user_name='Bob'),
    Row(user_id=7, user_name='Alex')
]
users_df = spark.createDataFrame(users_data)

# Creating the Register DataFrame
register_data = [
    Row(contest_id=215, user_id=6),
    Row(contest_id=209, user_id=2),
    Row(contest_id=208, user_id=2),
    Row(contest_id=210, user_id=6),
    Row(contest_id=208, user_id=6),
    Row(contest_id=209, user_id=7),
    Row(contest_id=209, user_id=6),
    Row(contest_id=215, user_id=7),
    Row(contest_id=208, user_id=7),
    Row(contest_id=210, user_id=2),
    Row(contest_id=207, user_id=2),
    Row(contest_id=210, user_id=7)
]
register_df = spark.createDataFrame(register_data)

# Show the DataFrames
users_df.show()
register_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import lit,count,col,round
count_df = register_df.groupBy("contest_id").agg(count("user_id").alias("cnt"))

# COMMAND ----------


count_df.withColumn("percentage",round(col("cnt")*100/lit(users_df.count()),2)).show()

# COMMAND ----------

