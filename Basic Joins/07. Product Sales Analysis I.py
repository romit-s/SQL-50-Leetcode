# Databricks notebook source
# MAGIC %md
# MAGIC ## Product Sales Analysis I

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create Sales and Product DataFrames") \
    .getOrCreate()

# Data for Sales table
sales_data = [
    (1, 100, 2008, 10, 5000),
    (2, 100, 2009, 12, 5000),
    (7, 200, 2011, 15, 9000)
]

# Data for Product table
product_data = [
    (100, "Nokia"),
    (200, "Apple"),
    (300, "Samsung")
]

# Define column names
sales_columns = ["sale_id", "product_id", "year", "quantity", "price"]
product_columns = ["product_id", "product_name"]

# Create DataFrames
sales_df = spark.createDataFrame(sales_data, sales_columns)
product_df = spark.createDataFrame(product_data, product_columns)

# Show the DataFrames
print("Sales DataFrame:")
sales_df.show()

print("Product DataFrame:")
product_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

product_df.join(sales_df,"product_id","inner").select("product_name","year","price").show()

# COMMAND ----------

