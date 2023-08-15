#! /bin/bash/

# Build docker image
docker build -f docker/Dockerfile -t databricks_replication_project_latest .

# Run docker image
docker run databricks_replication_project_latest python /app/main.py "{'source_type': 'dbfs_delta', 'table_name': 'project_table'}"