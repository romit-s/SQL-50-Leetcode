# Databricks notebook source
# MAGIC %md
# MAGIC ## Product Sales Analyis III

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("SalesProductDataFrameExample").getOrCreate()

# Creating the Sales DataFrame
sales_data = [
    Row(sale_id=1, product_id=100, year=2008, quantity=10, price=5000),
    Row(sale_id=2, product_id=100, year=2009, quantity=12, price=5000),
    Row(sale_id=7, product_id=200, year=2011, quantity=15, price=9000)
]
sales_df = spark.createDataFrame(sales_data)

# Creating the Product DataFrame
product_data = [
    Row(product_id=100, product_name="Nokia"),
    Row(product_id=200, product_name="Apple"),
    Row(product_id=300, product_name="Samsung")
]
product_df = spark.createDataFrame(product_data)

# Show the DataFrames
sales_df.show()
product_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col,rank
ws = Window().partitionBy("product_id").orderBy("sale_id")
sales_df.withColumn("rank",rank().over(ws)).filter(col("rank")==1).show()

# COMMAND ----------

