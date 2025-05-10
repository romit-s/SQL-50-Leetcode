# Databricks notebook source
# MAGIC %md
# MAGIC ## User Activity for the Past 30 Days I

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder.appName("ActivityDataFrameExample").getOrCreate()

# Creating the Activity DataFrame
activity_data = [
    Row(user_id=1, session_id=1, activity_date="2019-07-20", activity_type="open_session"),
    Row(user_id=1, session_id=1, activity_date="2019-07-20", activity_type="scroll_down"),
    Row(user_id=1, session_id=1, activity_date="2019-07-20", activity_type="end_session"),
    Row(user_id=2, session_id=4, activity_date="2019-07-20", activity_type="open_session"),
    Row(user_id=2, session_id=4, activity_date="2019-07-21", activity_type="send_message"),
    Row(user_id=2, session_id=4, activity_date="2019-07-21", activity_type="end_session"),
    Row(user_id=3, session_id=2, activity_date="2019-07-21", activity_type="open_session"),
    Row(user_id=3, session_id=2, activity_date="2019-07-21", activity_type="send_message"),
    Row(user_id=3, session_id=2, activity_date="2019-07-21", activity_type="end_session"),
    Row(user_id=4, session_id=3, activity_date="2019-06-25", activity_type="open_session"),
    Row(user_id=4, session_id=3, activity_date="2019-06-25", activity_type="end_session")
]

activity_df = spark.createDataFrame(activity_data)

# Show the DataFrame
activity_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

from pyspark.sql.functions import countDistinct,col
activity_df.groupBy("activity_date").agg(countDistinct("user_id")).filter(col("activity_date").rlike("2019-07.*")).show()

# COMMAND ----------

