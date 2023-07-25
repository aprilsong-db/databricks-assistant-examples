# Databricks notebook source
# MAGIC %md
# MAGIC #Databricks Assistant: sample tasks
# MAGIC
# MAGIC Source: https://docs.databricks.com/notebooks/notebook-assistant-examples.html
# MAGIC
# MAGIC Databricks Assistant works as an AI-based companion pair-programmer to make you more efficient as you create notebooks, queries, and files. It can help you rapidly answer questions by generating, optimizing, completing, explaining, and fixing code and queries.
# MAGIC
# MAGIC For general information about Databricks Assistant, see Databricks Assistant FAQ.
# MAGIC
# MAGIC The prompt you provide can significantly change the output of the assistant. Try adding one of the following to your prompts:
# MAGIC - “No explanatory text” when generating code.
# MAGIC - “Explain the code to me step by step”.
# MAGIC - “Show me two/three options that I can try”.
# MAGIC - “Be concise”.
# MAGIC
# MAGIC You can also experiment with the following types of queries:
# MAGIC - Write a SQL UDF to reverse a string.
# MAGIC - Add a date filter to this query to restrict results to the last 30 days.
# MAGIC -Help me plot a graph from the results of a SQL query. The query results are in the format of a Pandas DataFrame. The x-axis should be labeled ‘Week’ and the y-axis should be labeled ‘Distinct weekly users’.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate code examples
# MAGIC
# MAGIC ### Analyze data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Starting code:**

# COMMAND ----------

import pandas as pd

# Read the sample NYC Taxi Trips dataset and load it into a DataFrame
df = spark.read.table('samples.nyctaxi.trips')

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC   
# MAGIC generate pandas code to convert the pyspark dataframe to a pandas dataframe and select the 10 most expensive trips from df based on the fare_amount column
# MAGIC

# COMMAND ----------

import pandas as pd

# Read the sample NYC Taxi Trips dataset and load it into a DataFrame
df = spark.read.table('samples.nyctaxi.trips')

# Convert PySpark DataFrame to Pandas DataFrame
pandas_df = df.toPandas()

# Select 10 most expensive trips based on fare_amount
expensive_trips = pandas_df.nlargest(10, 'fare_amount')
expensive_trips

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC   
# MAGIC generate some interested plots based on expensive_trips
# MAGIC

# COMMAND ----------

import matplotlib.pyplot as plt

# Histogram of fare_amount for the 10 most expensive trips
expensive_trips['fare_amount'].plot(kind='hist', bins=20)
plt.title('Histogram of Fare Amount for 10 Most Expensive Trips')
plt.xlabel('Fare Amount')
plt.ylabel('Count')
plt.show()

# Scatter plot of trip distance vs. fare amount for the 10 most expensive trips
expensive_trips.plot(kind='scatter', x='trip_distance', y='fare_amount')
plt.title('Scatter Plot of Trip Distance vs. Fare Amount for 10 Most Expensive Trips')
plt.xlabel('Trip Distance')
plt.ylabel('Fare Amount')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC show me some time series plots

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

# Read the bike sharing dataset and load it into Pandas DataFrame
df = pd.read_csv('/dbfs/databricks-datasets/bikeSharing/data-001/hour.csv')

# Convert the date column to a datetime series and set it as index of dataframe
df['dteday'] = pd.to_datetime(df['dteday'])
df = df.set_index('dteday')

# Group hourly data by day to get daily rental count
daily_counts = df[['cnt']].groupby(pd.Grouper(freq='D')).sum()

# Plot the time series of daily rental count
fig, ax = plt.subplots(figsize=(10, 6))
ax.plot(daily_counts.index, daily_counts['cnt'])
ax.set(xlabel='Date', ylabel='Daily Rental Count', title='Time Series Plot of Daily Rental Count')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a DataFrame reader
# MAGIC
# MAGIC **Starting code:**
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks-datasets/bikeSharing/data-001/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC
# MAGIC Generate code to read the day.csv file in the bikeSharing dataset
# MAGIC

# COMMAND ----------

import pandas as pd

# Read the day.csv file
df = pd.read_csv('/dbfs/databricks-datasets/bikeSharing/data-001/day.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC
# MAGIC generate spark code that reads the dataset

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("BikeSharing").getOrCreate()

# Read the day.csv file into a DataFrame
df = spark.read.format("csv") \
          .option("header", "true") \
          .option("inferSchema", "true") \
          .load("/databricks-datasets/bikeSharing/data-001/day.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform or optimize code examples
# MAGIC
# MAGIC ### Translate Pandas to PySpark
# MAGIC
# MAGIC **Starting code:**
# MAGIC

# COMMAND ----------

import pandas as pd

# Convert Spark DataFrame to Pandas DataFrame
pdf = df.toPandas()

# Select the 10 most expensive trips based on the fare_amount column
most_expensive_trips = pdf.nlargest(10, 'fare_amount')

# Show the result
most_expensive_trips

# COMMAND ----------

# MAGIC %md
# MAGIC **Assistant prompt:**
# MAGIC
# MAGIC convert this code to PySpark
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import desc

# Read the sample NYC Taxi Trips dataset and load it into a DataFrame
df = spark.read.table('samples.nyctaxi.trips')

# Select the 10 most expensive trips based on the fare_amount column
most_expensive_trips = df.orderBy(desc("fare_amount")).limit(10)

# Show the result
most_expensive_trips.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate more efficient code
# MAGIC
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC Show me a code example of inefficient python code, explain why it is inefficient, and then show me an improved version of that code that is more efficient. Explain why it is more efficient, then give me a list of strings to test this out with and the code to benchmark trying each one out.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC Write me a function to benchmark the execution of code in this cell, then give me another way to write this code that is more efficient and would perform better in the benchmark.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete code examples
# MAGIC You can use LakeSense to generate code from comments in a cell.
# MAGIC
# MAGIC On macOS, press `shift + option + space` or `control + option + space` directly in a cell.
# MAGIC
# MAGIC On Windows, press `ctrl + shift + space` directly in a cell.
# MAGIC
# MAGIC To accept the suggested code, press `tab`.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reverse a string
# MAGIC
# MAGIC **Starting code:**

# COMMAND ----------

# Write code to reverse a string.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform exploratory data analysis
# MAGIC **Starting code:**

# COMMAND ----------

# Load the wine dataset into a DataFrame from sklearn, bucket the data into 3 groups by quality, then visualize in a plotly barchart.


# COMMAND ----------

# MAGIC %md
# MAGIC ## Explain code examples
# MAGIC
# MAGIC ### Basic code explanation
# MAGIC **Starting code:**
# MAGIC
# MAGIC PySpark code that gets the total number of trips and sum of the fare amounts between the pickup and dropoff zip codes.
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F

fare_by_route = df.groupBy(
'pickup_zip', 'dropoff_zip'
).agg(
    F.sum('fare_amount').alias('total_fare'),
    F.count('fare_amount').alias('num_trips')
).sort(F.col('num_trips').desc())

display(fare_by_route)

# COMMAND ----------

# MAGIC %md
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC Explain what this code does
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fast documentation lookups
# MAGIC
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC When should I use repartition() vs. coalesce() in Apache Spark?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC What is the difference between the various pandas_udf functions (in PySpark and Pandas on Spark/Koalas) and when should I choose each?  Can you show me an example of each with the diamonds dataset?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fix code examples
# MAGIC ### Debugging
# MAGIC **Starting code:**
# MAGIC
# MAGIC This is the same code used in the basic code explanation example, but missing the import statement. It throws the error “This throws the error: NameError: name ‘F’ is not defined”.

# COMMAND ----------

fare_by_route = df.groupBy(
'pickup_zip', 'dropoff_zip'
).agg(
    F.sum('fare_amount').alias('total_fare'),
    F.count('fare_amount').alias('num_trips')
).sort(F.col('num_trips').desc())

display(fare_by_route)

# COMMAND ----------

# MAGIC %md
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC How do I fix this error? What is 'F'?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Help with errors
# MAGIC **Starting code:**
# MAGIC
# MAGIC This code throws the error “AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]”.
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# create a dataframe with two columns: a and b
df = spark.range(5).select(col('id').alias('a'), col('id').alias('b'))

# try to select a non-existing column c
df.select(col('c')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Assisted prompt:**
# MAGIC
# MAGIC Why am I getting this error and how do I fix it?
# MAGIC
