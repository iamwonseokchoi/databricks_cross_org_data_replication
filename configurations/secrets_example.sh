#! /bin/sh

# Create secret scope on Databricks CLI 
databricks secrets create-scope azure_storage_account

# Create secrets onto scope 
databricks secrets put-secret azure_storage_account storageAccountName --string-value "<myPassword>"
databricks secrets put-secret azure_storage_account storageAccountKey --string-value "<myPassword>"