# Databricks notebook source
import json

# COMMAND ----------

class delta_test:
    def __init__(self, delta_table_name):
        self.delta_table_name = delta_table_name
        delta_files_list_dict = {}
        for i in dbutils.fs.ls("dbfs:/user/hive/warehouse/"):
            if i.size == 0:
                delta_files_list_dict[i.name[:-1]] = i.path
        self.delta_files_list_dict = delta_files_list_dict
    
    def test_count(self):
        df_source = spark.read.load(self.delta_files_list_dict[self.delta_table_name])
        df_target = spark.read.load(f"/mnt/replication/delta/{self.delta_table_name}")
        if df_source.count() == df_target.count():
            return True
        else:
            print(f"Test failed: Source count: {df_source.count()}, Target count: {df_target.count()}")
            return False
    
    def test_full_scan(self):
        df_source = spark.read.load(self.delta_files_list_dict[self.delta_table_name])
        df_target = spark.read.load(f"/mnt/replication/delta/{self.delta_table_name}")
        df_difference = df_source.exceptAll(df_target)
        if df_difference.count() == 0:
            return True
        else:
            print(f"Test failed: {df_difference.count()} rows are different between source and target")
            return False

# COMMAND ----------

class sql_server_test:
    def __init__(self, table_name):
        self.table_name = table_name 
        server_name = f"jdbc:sqlserver://databricksdatalineageproject.database.windows.net"
        db_name = "databricks_replication_db"
        username = "<Authorized User>"
        password = "<Password>"
        url = f"jdbc:sqlserver://databricksdatalineageproject.database.windows.net:1433;database=databricks_replication_db;user={username}@databricksdatalineageproject;password={password};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        connection_properties = {"user":username, "password":password, "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"}
        self.url = url
        self.database_name = db_name
        self.server_name = server_name
        self.connectionProperties = connection_properties
        self.username = username
        self.password = password

    def test_count(self):
        df_source=spark.read.jdbc(url=self.url,table=self.table_name,properties=self.connectionProperties)
        df_target=spark.read.load(f"/mnt/replication/sql_server/{self.table_name}")
        if df_source.count() == df_target.count():
            return True
        else:
            print(f"Test failed: Source count: {df_source.count()}, Target count: {df_target.count()}")
            return False

    def test_full_scan(self):
        df_source=spark.read.jdbc(url=self.url,table=self.table_name,properties=self.connectionProperties)
        df_target=spark.read.load(f"/mnt/replication/sql_server/{self.table_name}")
        df_difference = df_source.exceptAll(df_target)
        if df_difference.count() == 0:
            return True
        else:
            print(f"Test failed: {df_difference.count()} rows are different between source and target")
            return False

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def run_tests(test):
    count_result = test.test_count()
    scan_result = test.test_full_scan() 
    if not count_result:
        raise AssertionError(f"Count Test failed for {getattr(test, 'delta_table_name', test.table_name)}")
    if not scan_result:
        raise AssertionError(f"Full-scan Test failed for {getattr(test, 'delta_table_name', test.table_name)}")
    return True 

# Tables to test as a list for scalability
tables_to_test = [
    delta_test("project_table"),
    sql_server_test("dbo.insurance_claims")
]

# In case we have many tables and many cores to spare
success = False
with ThreadPoolExecutor(max_workers=spark.sparkContext.defaultParallelism) as executor: 
    results = list(executor.map(run_tests, tables_to_test))
    if all(results):
        print("All tests passed.")
        success = True
    else: 
        failed_count = len([r for r in results if not r])
        print(f"{failed_count} tests failed.")

success # This variable is meant to be called in the email_trigger notebook
