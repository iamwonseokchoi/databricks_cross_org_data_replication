# Databricks notebook source
# MAGIC %fs
# MAGIC ls /mnt/input

# COMMAND ----------

target_data = dbutils.fs.ls("/mnt/input/")[0][0]

# COMMAND ----------

df1 = spark.read.option("header", True).option("inferSchema", True).csv(target_data)
df1.display()

# COMMAND ----------

# Same as for the mounted data in the config.py file, 
# Clean the "File No." column and add in a primary key for merges
from pyspark.sql.functions import monotonically_increasing_id

df_cleaned = df1.withColumnRenamed("File No.", "FileNo")
df_indexed = df_cleaned.withColumn("id", monotonically_increasing_id())

# COMMAND ----------

df_indexed.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Created a mock user on sqlserver database using sql editor
# MAGIC `CREATE USER admin_user WITH PASSWORD = 'randomPass1234!@#$';
# MAGIC ALTER ROLE db_owner ADD MEMBER admin_user;`

# COMMAND ----------

# sqlserver resources for reference
server_name = f"jdbc:sqlserver://databricksdatalineageproject.database.windows.net"
db_name = "databricks_replication_db"

# Azure SQL JDBC connection
url = "jdbc:sqlserver://databricksdatalineageproject.database.windows.net:1433;database=databricks_replication_db;user=admin_user@databricksdatalineageproject;password=randomPass1234!@#$;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

# write data to sqlserver
df_indexed.write.jdbc(url, table="insurance_claims", mode="append")
