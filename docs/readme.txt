1. Setting Up a Source
Setup a PostgreSQL on Neon.tech
Create Schema and pushed csv data using psychopg

2. Importing from neon.tech in Bronze Layer
Had to turn on logical replication in neon, set replica identity of all tables to full, create a publication to connect to cdc, didn't work. CDC option is in preview and not available in free tier of databricks.

Used Ingest-data in databricks with Query-Based Capture using a cursor column and primary-id column

3. DBT
create dbt project and connect to databricks compute
setup bronze as source for lineage graph
silver technical layer : added processed_at column and ran tests for ids and other columns
silver business layer : One Big Table(OBT)
gold layer: ephemeral sub-folder contains dimensions materialized as ephemeral which are then used by snapshots to create SCD type-2 dimension 		    tables, obt-b is used for creating the fact-table

4. Apache AIRFLOW
Needed for orchestrating and scheduling
Bind mounted Walmart Project inside dbt folder in the docker-compose.yaml
Separate bash tasks created for each layer of model and separate for their test to make diagnosis easier
Ingestion pipeline called manually using databricks-sdk package
Scheduled DAG to run 11AM daily


