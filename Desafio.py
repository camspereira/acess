# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------


file_location = "/FileStore/tables/access_logs.txt"
file_type = "csv" 

infer_schema = "false" 
first_row_is_header = "false"
delimiter = " "  

df = spark.read.format(file_type) \
    .option("inferSchema", infer_schema) \
    .option("header", first_row_is_header) \
    .option("sep", delimiter) \
    .load(file_location)

df.show()


# COMMAND ----------

google_access_count = df.filter(df["_c3"] == "google.com").count()

print(f"Quantidade de acessos a google.com: {google_access_count}")


# COMMAND ----------

page_counts = df.groupBy("_c3").count()

top_5_pages = page_counts.orderBy("count", ascending=False).limit(5)

top_5_pages.show()


# COMMAND ----------

accesses_by_date = df.groupBy("_c1").count()

accesses_by_date.orderBy("_c1").show(100, False)

# COMMAND ----------

from pyspark.sql.functions import when, col

df_with_ip_type = df.withColumn(
    "ip_type", 
    when(col("_c0").like("10.%"), "internal")  
    .when(col("_c0").like("192.168.%"), "internal")  
    .when(col("_c0").like("172.16.%"), "internal")  
    .when(col("_c0").like("172.17.%"), "internal")  
    .when(col("_c0").like("172.18.%"), "internal")  
    .when(col("_c0").like("172.19.%"), "internal")  
    .when(col("_c0").like("172.20.%"), "internal")  
    .when(col("_c0").like("172.21.%"), "internal")  
    .when(col("_c0").like("172.22.%"), "internal")  
    .when(col("_c0").like("172.23.%"), "internal")  
    .when(col("_c0").like("172.24.%"), "internal")  
    .when(col("_c0").like("172.25.%"), "internal")  
    .when(col("_c0").like("172.26.%"), "internal")  
    .when(col("_c0").like("172.27.%"), "internal")  
    .when(col("_c0").like("172.28.%"), "internal")  
    .when(col("_c0").like("172.29.%"), "internal")  
    .when(col("_c0").like("172.30.%"), "internal")  
    .when(col("_c0").like("172.31.%"), "internal")  
    .otherwise("external") 
)

accesses_by_ip_type = df_with_ip_type.groupBy("ip_type").count()

accesses_by_ip_type.show()

# COMMAND ----------

# Create a view or table

temp_table_name = "access_logs"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `access_logs`

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "access_logs"

# df.write.format("parquet").saveAsTable(permanent_table_name)
