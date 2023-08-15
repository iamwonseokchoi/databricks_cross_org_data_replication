# Databricks notebook source
# DBTITLE 1,Run tests notebook
# MAGIC %run ../tests/test_replications

# COMMAND ----------

import requests 

# Azure Logic Apps
# Workflow made on Logic Apps to first receive an HTTP request, and then send an email from Outlook
url = "<Logic Apps HTTP endpoint URL>"
email = "<Some Email>"

if success:
    subject = "Data Replication Success Notification"
    body = """
        This is to inform you that Data replication has been successful.  
    """
else: 
    subject = "Data Replication Error Notification"
    body = """
        :(
    """

request_payload = {
    "email": email, 
    "subject": subject,
    "body": body
}

email_response = requests.post(url, json=request_payload)
print(email_response.text)
