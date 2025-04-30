# Databricks notebook source
# MAGIC %md
# MAGIC ## Find Customer Referee

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import Row

# Define the Customer data
customers_data = [
    Row(id=1, name="Will", referee_id=None),
    Row(id=2, name="Jane", referee_id=None),
    Row(id=3, name="Alex", referee_id=2),
    Row(id=4, name="Bill", referee_id=None),
    Row(id=5, name="Zack", referee_id=1),
    Row(id=6, name="Mark", referee_id=2)
]

# Create Customer DataFrame
customers_df = spark.createDataFrame(customers_data)

# Show the Customer DataFrame
print("Customer DataFrame:")
customers_df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer below

# COMMAND ----------

customers_df.filter((customers_df.referee_id.isNull())|(customers_df.referee_id!= 2)).select("name").show()

# COMMAND ----------

