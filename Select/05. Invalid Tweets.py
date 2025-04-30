# Databricks notebook source
# MAGIC %md
# MAGIC ## Invalid Tweets

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("Tweets DataFrame").getOrCreate()

# Define the schema for the DataFrame
schema = StructType([
    StructField("tweet_id", IntegerType(), True),
    StructField("content", StringType(), True)
])

# Define the data as a list of tuples
data = [
    (1, "Let us Code"),
    (2, "More than fifteen chars are here!")
]

# Create the DataFrame using the data and schema
tweets_df = spark.createDataFrame(data, schema)

# Show the DataFrame
tweets_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col,length
tweets_df.where(length(col("content"))> 15).select("tweet_id").show()

# COMMAND ----------

