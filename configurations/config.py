# Databricks notebook source
# Instantiate secret access to azure storage account
storageAccountName = dbutils.secrets.get(scope="azure_storage_account", key="storageAccountName")
storageAccountKey = dbutils.secrets.get(scope="azure_storage_account", key="storageAccountKey")

# COMMAND ----------

# This is the target output container simulating replication
storageContainerName = "lineage-project-output"

# Mount for sql_server
if not any(mount.mountPoint == "/mnt/replication/sql_server" for mount in dbutils.fs.mounts()):
    try:
        dbutils.fs.mount(
            source = f"wasbs://{storageContainerName}@{storageAccountName}.blob.core.windows.net",
            mount_point = "/mnt/replication/sql_server", 
            extra_configs = {f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net": storageAccountKey}
        )
    except Exception as _e:
        print(_e)
        print("Unmount then remount")

# Mount for delta
if not any(mount.mountPoint == "/mnt/replication/delta" for mount in dbutils.fs.mounts()):
    try:
        dbutils.fs.mount(
            source = f"wasbs://{storageContainerName}@{storageAccountName}.blob.core.windows.net",
            mount_point = "/mnt/replication/delta", 
            extra_configs = {f"fs.azure.account.key.{storageAccountName}.blob.core.windows.net": storageAccountKey}
        )
    except Exception as _e:
        print(_e)
        print("Unmount then remount")

# COMMAND ----------

dbutils.fs.ls("/mnt/input")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/input

# COMMAND ----------

# Read the CSV file with headers and inferred schema
df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv("dbfs:/mnt/input/Insurance_Company_Complaints__Resolutions__Status__and_Recoveries.csv")

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# There seems to be a column named "File No." which will not be accepted by delta so we rename it
df_cleaned = df.withColumnRenamed("File No.", "FileNo")

# Also assign a primary key index as there seems to be no PK defined
# Check: select count(*) from hive_metastore.default.project_table group by FileNo having count(FileNo) > 1; 
from pyspark.sql.functions import monotonically_increasing_id

df_indexed = df_cleaned.withColumn("id", monotonically_increasing_id())

# COMMAND ----------

# Write the DataFrame as a Delta table
df_indexed.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("hive_metastore.default.project_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick check
# MAGIC SELECT * FROM hive_metastore.default.project_table LIMIT 10; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL hive_metastore.default.project_table

# COMMAND ----------

# It seems the table is written in 2 files, but the table is only ~550KB. Not a big deal, 
# But coalesce to optimize as we did not bother to this before 
# with spark.conf.set("spark.databricks.optimizer.autoOptimize", "true")

df_optimized = df_cleaned.coalesce(1)
df_optimized.write.format("delta").mode("overwrite").saveAsTable("hive_metastore.default.project_table")

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE DETAIL hive_metastore.default.project_table
