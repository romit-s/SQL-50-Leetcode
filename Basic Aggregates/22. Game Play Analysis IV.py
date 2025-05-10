# Databricks notebook source
# MAGIC %md
# MAGIC ## Game Play Analysis IV

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import to_date

# Initialize a Spark session
spark = SparkSession.builder.appName("ActivityDataFrameExample").getOrCreate()

# Creating the Activity DataFrame
data = [
    Row(player_id=1, device_id=2, event_date='2016-03-01', games_played=5),
    Row(player_id=1, device_id=2, event_date='2016-03-02', games_played=6),
    Row(player_id=2, device_id=3, event_date='2017-06-25', games_played=1),
    Row(player_id=3, device_id=1, event_date='2016-03-02', games_played=0),
    Row(player_id=3, device_id=4, event_date='2018-07-03', games_played=5)
]
activity_df = spark.createDataFrame(data)
activity_df = activity_df.withColumn("event_date",to_date("event_date"))
# Show the DataFrame
activity_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import date_sub,count,col,sum,countDistinct,when,round

previous_df = activity_df.withColumn("previous_date",date_sub("event_date",1))
transformed_df = activity_df.alias("e").join(previous_df.alias("p"),(col("e.event_date") ==col("p.previous_date")) & (col("e.player_id") == col("p.player_id")),"left")

# COMMAND ----------

transformed_df.groupBy().agg(countDistinct("e.player_id").alias("total_count"),sum(when(col("p.player_id").isNotNull(),1)).alias("immediate_count")).withColumn("fraction",round(col("immediate_count")/col("total_count"),2)).select("fraction").show()

# COMMAND ----------

