# Databricks notebook source
# %pip install databricks_api

# COMMAND ----------

import sys
import ast  
import requests 
from databricks_api import DatabricksAPI 

# COMMAND ----------

def post_request(API_arg="['project_table']"):
    token = "<Databricks User Access Token>"
    host = "https://<Databricks Workspace>.azuredatabricks.net/"
    db = DatabricksAPI(
        host=host,
        token=token
    )
    request_payload = {
        "run_name": "project_run",
        "existing_cluster_id": "<Cluster ID>",
        "notebook_task":
            {
                "notebook_path": "/Repos/user111@outlook.com/dbx_lineage_replication_project/replication_codes/modular_replication",
                "base_parameters": {"list1": API_arg}
            }
    }
    response = requests.post("https://<Databricks Workspace>.azuredatabricks.net/api/2.0/jobs/runs/submit",
        json=request_payload, headers={"Authorization": f"Bearer {token}"}
    )
    run_id = int(response.text[10:-1])
    final_result = db.jobs.get_run_output(run_id=run_id)
    print(final_result, request_payload)


if __name__ == "__main__":
    API_arg = ast.literal_eval(sys.argv[1])
    post_request(str(API_arg))
