# Databricks notebook source
# MAGIC %md
# MAGIC ## Article Views I

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import Row

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Create Article Views DataFrame") \
    .getOrCreate()

# Define the article views data
article_views_data = [
    Row(article_id=1, author_id=3, viewer_id=5, view_date="2019-08-01"),
    Row(article_id=1, author_id=3, viewer_id=6, view_date="2019-08-02"),
    Row(article_id=2, author_id=7, viewer_id=7, view_date="2019-08-01"),
    Row(article_id=2, author_id=7, viewer_id=6, view_date="2019-08-02"),
    Row(article_id=4, author_id=7, viewer_id=1, view_date="2019-07-22"),
    Row(article_id=3, author_id=4, viewer_id=4, view_date="2019-07-21"),
    Row(article_id=3, author_id=4, viewer_id=4, view_date="2019-07-21")
]

# Create DataFrame for article views
article_views_df = spark.createDataFrame(article_views_data)

# Show the article views DataFrame
print("Article Views DataFrame:")
article_views_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

article_views_df.filter(article_views_df.author_id == article_views_df.viewer_id).select(article_views_df.author_id.alias("id")).distinct().show()

# COMMAND ----------

