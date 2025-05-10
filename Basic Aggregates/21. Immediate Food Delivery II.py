# Databricks notebook source
# MAGIC %md
# MAGIC ## Immediate Food Delivery II

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import to_date

# Initialize a Spark session
spark = SparkSession.builder.appName("DeliveryDataFrameExample").getOrCreate()

# Creating the Delivery DataFrame
data = [
    Row(delivery_id=1, customer_id=1, order_date='2019-08-01', customer_pref_delivery_date='2019-08-02'),
    Row(delivery_id=2, customer_id=2, order_date='2019-08-02', customer_pref_delivery_date='2019-08-02'),
    Row(delivery_id=3, customer_id=1, order_date='2019-08-11', customer_pref_delivery_date='2019-08-12'),
    Row(delivery_id=4, customer_id=3, order_date='2019-08-24', customer_pref_delivery_date='2019-08-24'),
    Row(delivery_id=5, customer_id=3, order_date='2019-08-21', customer_pref_delivery_date='2019-08-22'),
    Row(delivery_id=6, customer_id=2, order_date='2019-08-11', customer_pref_delivery_date='2019-08-13'),
    Row(delivery_id=7, customer_id=4, order_date='2019-08-09', customer_pref_delivery_date='2019-08-09')
]
delivery_df = spark.createDataFrame(data)

# Convert date columns to date type
delivery_df = delivery_df.withColumn(
    "order_date", to_date(delivery_df.order_date)
).withColumn(
    "customer_pref_delivery_date", to_date(delivery_df.customer_pref_delivery_date)
)

# Show the updated DataFrame with date types
delivery_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import rank,datediff
from pyspark.sql import Window
window_spec = Window().partitionBy("customer_id").orderBy("order_date")
first_order_df = delivery_df.withColumn("rank",rank().over(window_spec)).filter("rank == 1")

# COMMAND ----------

first_order_df.withColumn("immediate",datediff(col("customer_pref_delivery_date"),col("order_date"))).groupBy("rank").agg((sum("immediate")*100/sum(lit(1))).alias("immediate_percentage")).select("immediate_percentage").show()

# COMMAND ----------

