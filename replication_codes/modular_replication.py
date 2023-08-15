# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *
import json

# COMMAND ----------

# Alotted file or table types in JSON keys:
# dbfs_delta, sql_server, csv
dbutils.widgets.text("user_input", 
    "[{'source_type': 'dbfs_delta', 'table_name': 'project_table'}, {'source_type': 'sql_server', 'table_name':'dbo.insurance_claims'}, {'source_type': 'csv', 'file_name': ''}]"
)
input = dbutils.widgets.get("user_input")

# COMMAND ----------

delta_files_list_dict = {}

for i in dbutils.fs.ls("dbfs:/user/hive/warehouse/"):
    if i.size == 0:
        delta_files_list_dict[i.name[:-1]] = i.path

# COMMAND ----------

csv_tables_list_dict={}

for i in dbutils.fs.ls('dbfs:/mnt/input/'):
    if i.size != 0:
        csv_tables_list_dict[i.name[:-4].capitalize()]=i.path

# COMMAND ----------

def delta_replication(table_name, delta_files_list_dict):
    """
    Replicates delta tables into the /replication/delta/* directory
    with either a full or merged load
    """
    delta_tables_list = {}
    try: 
        dbutils.fs.ls(f"/mnt/replication/delta/{table_name}")
        full_load = False 
    except: 
        full_load = True

    if table_name in delta_files_list_dict and not full_load:
        deltaTable = DeltaTable.forPath(sparkSession=spark, path=f"/mnt/replication/delta/{table_name}")
        df1 = spark.read.load(delta_files_list_dict[table_name])
        with open(f"/Workspace/Repos/user111@outlook.com/dbx_lineage_replication_project/source_definitions/{table_name}.json", "r") as f:
            data = json.load(f)
        condition = f"target.{data['primary_key']} = updates.{data['primary_key']}"
        deltaTable.alias("target").merge(df1.alias("updates"), condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    elif table_name in delta_files_list_dict and full_load:
        df1 = spark.read.load(delta_files_list_dict[table_name])
        df1.write.save(f"/mnt/replication/delta/{table_name}")
    
    else:
        print(f"No table '{table_name}' found in path")

# COMMAND ----------

def sqlserver_replication(table_name):
    """
    Replicates sqlServer tables into the /replication/sqlserver/* directory
    with either a full or merged load
    """
    server_name = f"jdbc:sqlserver://databricksdatalineageproject.database.windows.net"
    db_name = "databricks_replication_db"
    username = "<Created User on SQLServer Database>"
    password = "<Password for SQLServer User>"
    url = f"jdbc:sqlserver://databricksdatalineageproject.database.windows.net:1433;database=databricks_replication_db;user={username}@databricksdatalineageproject;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    connection_properties = {"user":username, "password":password, "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}

    try:
        df1 = spark.read.jdbc(url=url, table=table_name, properties=connection_properties)
        try:
            table_path = dbutils.fs.ls(f"/mnt/replication/sql_server/{table_name}")
            replication_folder = True 
        except: 
            replication_folder = False 
    except:
        print(f"Table '{table_name}' not present in SQLServer Database")

    if replication_folder:
        deltaTable = DeltaTable.forPath(sparkSession=spark, path=f"/mnt/replication/sql_server/{table_name}")
        with open (f"/Workspace/Repos/user111@outlook.com/dbx_lineage_replication_project/source_definitions/{table_name[4:]}.json", "r") as f:
            data = json.load(f)
        condition = f"target.{data['primary_key']} = updates.{data['primary_key']}"
        deltaTable.alias("target").merge(df1.alias("updates"), condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 
    
    else:
        df1.write.save(f"/mnt/replication/sql_server/{table_name}")

# COMMAND ----------

def csv_replication(file_name, csv_tables_list_dict):
    """
    Replicates csv files into the /replication/csv/* directory
    with either a full or merged load
    """
    try: 
        dbutils.fs.ls(f"/mnt/replication/csv_files/{file_name}")
        full_load = False 
    except: 
        full_load = True 
    
    if file_name in csv_tables_list_dict and not full_load:
        deltaTable = DeltaTable.forPath(sparkSession=spark, path=f"/mnt/replication/csv_files/{file_name}")
        df1 = spark.read.option("header", True).option("inferSchema", True).load(csv_tables_list_dict[file_name])
        with open (f"/Workspace/Repos/user111@outlook.com/dbx_lineage_replication_project/source_definitions/{file_name}.json", "r") as f:
            data = json.load(f)
        condition = f"target.{data['primary_key']} = updates.{data['primary_key']}"
        deltaTable.alias("target").merge(df1.alias("updates"), condition).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

    elif file_name in csv_tables_list_dict and full_load:
        df1 = spark.read.option("header", True).option("inferSchema", True).csv(csv_tables_list_dict[file_name])
        df1.write.save(f"/mnt/replication/csv_files/{file_name}")
    
    else: 
        print(f"No table/csv file '{file_name}' in path")

# COMMAND ----------

# Execution: 
for i in eval(input):
    print(i)
    if i["source_type"] == "dbfs_delta":
        table_name = i["table_name"]
        delta_replication(table_name, delta_files_list_dict)
    elif i["source_type"] == "sql_server":
        table_name = i["table_name"]
        sqlserver_replication(table_name)
    elif i["source_type"] == "csv":
        file_name = i["file_name"]
        csv_replication(file_name, csv_tables_list_dict)
