# Databricks notebook source
# MAGIC %md
# MAGIC ## Recyclable and Low Fat Products

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import Row

# Define the Products data
products_data = [
    Row(product_id=0, low_fats="Y", recyclable="N"),
    Row(product_id=1, low_fats="Y", recyclable="Y"),
    Row(product_id=2, low_fats="N", recyclable="Y"),
    Row(product_id=3, low_fats="Y", recyclable="Y"),
    Row(product_id=4, low_fats="N", recyclable="N")
]

# Create Products DataFrame
products_df = spark.createDataFrame(products_data)

# Show the Products DataFrame
print("Products DataFrame:")
products_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

products_df.filter((products_df.low_fats == "Y") & (products_df.recyclable == "Y")).select("product_id").show()

# COMMAND ----------

