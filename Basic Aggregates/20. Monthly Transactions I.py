# Databricks notebook source
# MAGIC %md
# MAGIC ## Monthly Transactions I

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("TransactionsDataFrameExample").getOrCreate()

# Creating the Transactions DataFrame
data = [
    Row(id=121, country='US', state='approved', amount=1000, trans_date='2018-12-18'),
    Row(id=122, country='US', state='declined', amount=2000, trans_date='2018-12-19'),
    Row(id=123, country='US', state='approved', amount=2000, trans_date='2019-01-01'),
    Row(id=124, country='DE', state='approved', amount=2000, trans_date='2019-01-07')
]
transactions_df = spark.createDataFrame(data)

# Show the DataFrame
transactions_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import col,sum,when,lit,month,count,year,concat
transactions_df.withColumn("month",concat(year(col("trans_date")),lit("-"),month(col("trans_date")))).groupBy("month","country").agg(
count("id").alias("trans_count"),
sum(when(col("state")=="approved",1).otherwise(0)).alias("approved_count"),
sum("amount").alias("trans_amount"),
sum(when(col("state")=="approved",col("amount")).otherwise(0)).alias("approved_amount")).show()

# COMMAND ----------

