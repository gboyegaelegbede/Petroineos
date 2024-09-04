# Databricks notebook source
# MAGIC %pip install openpyxl
# MAGIC
# MAGIC import pandas as pd
# MAGIC
# MAGIC """
# MAGIC Check if this is not the initial run of this notebook i.e.; table named "result" exists
# MAGIC count the number of columns in the table named "result
# MAGIC If the nos of columns is less than nos of columns in existing file at the website
# MAGIC It means a new data (columns) has been added to the existing file at the website.
# MAGIC Append the new data to the existing file.
# MAGIC
# MAGIC """
# MAGIC
# MAGIC table_exists = spark.sql("SHOW TABLES LIKE 'result'").count() > 0
# MAGIC
# MAGIC if table_exists:
# MAGIC
# MAGIC
# MAGIC     # Read the new Excel file and load the "Quarter" sheet into a Pandas DataFrame
# MAGIC     df_new = pd.read_excel("https://assets.publishing.service.gov.uk/media/66a76ff1ab418ab055592e8a/ET_3.1_JUL_24.xlsx", sheet_name="Quarter", header=4)
# MAGIC
# MAGIC     # Convert the Pandas DataFrame to a Spark DataFrame
# MAGIC     df_new = spark.createDataFrame(df_new)
# MAGIC
# MAGIC     # Check for new data by comparing the nos of columns in new Spark DataFrame with the existing one
# MAGIC
# MAGIC     previous_columns = len(spark.table("Results").columns)
# MAGIC     new_columns = len(df_new.columns)
# MAGIC
# MAGIC     if (new_columns > previous_columns):
# MAGIC
# MAGIC         from pyspark.sql.functions import col, monotonically_increasing_id
# MAGIC
# MAGIC         # Load the two tables as DataFrames
# MAGIC         df_table2 = spark.table("Results")
# MAGIC         df_table1 = df_new
# MAGIC
# MAGIC         # Get the name of the last column in table1
# MAGIC         last_column_name = df_table1.columns[-1]
# MAGIC
# MAGIC         # Select the last column from table1
# MAGIC         last_column_df = df_table1.select(last_column_name)
# MAGIC
# MAGIC         # Add a monotonically increasing id to both DataFrames to ensure a one-to-one mapping
# MAGIC         df_table2_with_id = df_table2.withColumn("row_id", monotonically_increasing_id())
# MAGIC         last_column_df_with_id = last_column_df.withColumn("row_id", monotonically_increasing_id())
# MAGIC
# MAGIC         # Join the last column of table1 to table2 based on the row_id and then drop the row_id
# MAGIC
# MAGIC         df_table2_with_last_column = df_table2_with_id.join(last_column_df_with_id, df_table2_with_id["row_id"] == last_column_df_with_id["row_id"], "left").drop("row_id")
# MAGIC
# MAGIC          # Write the Spark DataFrame with new column appended to a csv table
# MAGIC          
# MAGIC         df_table2_with_last_column.write.format("csv").mode("overwrite").saveAsTable("Results")
# MAGIC
# MAGIC
# MAGIC else:
# MAGIC
# MAGIC     # It is an initial load, simply write the data to the csv table
# MAGIC     
# MAGIC     # Read the Excel file and load the "Quarter" sheet into a Pandas DataFrame
# MAGIC     df = pd.read_excel("https://assets.publishing.service.gov.uk/media/66a76ff1ab418ab055592e8a/ET_3.1_JUL_24.xlsx", sheet_name="Quarter", header=4)
# MAGIC
# MAGIC     # Convert the Pandas DataFrame to a Spark DataFrame
# MAGIC     spark_df = spark.createDataFrame(df)
# MAGIC
# MAGIC     # Write the Spark DataFrame to a csv table
# MAGIC     spark_df.write.format("csv").mode("overwrite").saveAsTable("Results")
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
