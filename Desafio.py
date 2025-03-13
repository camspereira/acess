# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Este notebook mostra como criar e consultar uma tabela ou DataFrame que foi carregado para o DBFS. O [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) é um sistema de arquivos do Databricks que permite armazenar dados para consulta dentro do Databricks. Este notebook assume que você já tem um arquivo dentro do DBFS que deseja ler.
# MAGIC
# MAGIC O notebook está escrito em **Python**, então o tipo de célula padrão é Python. No entanto, você pode usar diferentes linguagens utilizando a sintaxe `%LANGUAGE`. Python, Scala, SQL e R são todos suportados.

# COMMAND ----------

file_location = "/FileStore/tables/access_logs.txt"  
file_type = "csv"  # Tipo de arquivo

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
    when(col("_c0").like("10.%"), "internal")  # IPs internos começam com 10.
    .when(col("_c0").like("192.168.%"), "internal")  # IPs internos começam com 192.168.
    .when(col("_c0").like("172.16.%"), "internal")  # IPs internos começam com 172.16 até 172.31.
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
    .otherwise("external")  # Qualquer outro IP é classificado como 'externo'
)

accesses_by_ip_type = df_with_ip_type.groupBy("ip_type").count()

accesses_by_ip_type.show()
