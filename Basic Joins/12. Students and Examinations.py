# Databricks notebook source
# MAGIC %md
# MAGIC ## Students and Examinations

# COMMAND ----------

# MAGIC %md
# MAGIC Creating Input Dataframe

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
# Initialize a Spark session
spark = SparkSession.builder \
    .appName("Students, Subjects, and Examinations DataFrames") \
    .getOrCreate()

# Data for the Students table
data_students = [
    (1, "Alice"),
    (2, "Bob"),
    (13, "John"),
    (6, "Alex")
]

# Define column names for Students table
columns_students = ["student_id", "student_name"]

# Create DataFrame for Students table
students_df = spark.createDataFrame(data_students, columns_students)

# Data for the Subjects table
data_subjects = [
    ("Math"),
    ("Physics"),
    ("Programming")
]
# Wrap each subject in a tuple
data_subjects = [(subject,) for subject in data_subjects]

# Define column names for Subjects table
columns_subjects = ["subject_name"]

# Create DataFrame for Subjects table
subjects_df = spark.createDataFrame(data_subjects, columns_subjects)

# Data for the Examinations table
data_examinations = [
    (1, "Math"),
    (1, "Physics"),
    (1, "Programming"),
    (2, "Programming"),
    (1, "Physics"),
    (1, "Math"),
    (13, "Math"),
    (13, "Programming"),
    (13, "Physics"),
    (2, "Math"),
    (1, "Math")
]

# Define column names for Examinations table
columns_examinations = ["student_id", "subject_name"]

# Create DataFrame for Examinations table
examinations_df = spark.createDataFrame(data_examinations, columns_examinations)

# Show the DataFrames
students_df.show()
subjects_df.show()
examinations_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Write your answer Below:

# COMMAND ----------

total_exams = students_df.crossJoin(subjects_df)
count_df = examinations_df.groupBy(["student_id","subject_name"]).count()

# COMMAND ----------

total_exams.join(count_df,["student_id","subject_name"],"left").show()

# COMMAND ----------

