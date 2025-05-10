# Databricks notebook source
# MAGIC %md
# MAGIC ## Queries Quality and Percentage

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("QueriesDataFrameExample").getOrCreate()

# Creating the Queries DataFrame
queries_data = [
    Row(query_name='Dog', result='Golden Retriever', position=1, rating=5),
    Row(query_name='Dog', result='German Shepherd', position=2, rating=5),
    Row(query_name='Dog', result='Mule', position=200, rating=1),
    Row(query_name='Cat', result='Shirazi', position=5, rating=2),
    Row(query_name='Cat', result='Siamese', position=3, rating=3),
    Row(query_name='Cat', result='Sphynx', position=7, rating=4)
]
queries_df = spark.createDataFrame(queries_data)

# Show the DataFrame
queries_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col,mean,when,round
queries_df.withColumn("bad_rating",when(col("rating")<3,1).otherwise(0)).groupBy("query_name").agg(round(mean(col("rating")/col("position")),2).alias("quality"),round(mean(col("bad_rating"))*100,2).alias("poor_query_percentage")).show()

# COMMAND ----------

