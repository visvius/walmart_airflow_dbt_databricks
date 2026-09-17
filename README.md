# Walmart Retail Operations Pipeline
An end-to-end data engineering pipeline implementing a Medallion architecture to process enterprise retail operations data. This project extracts multi-domain data including data about customers, employees, products, orders, order items and stores — from a PostgreSQL database into Bronze layer, transforms it through clearly defined Silver and Gold layers using dbt, and orchestrates the entire workflow with Apache Airflow using Docker Containers.
<br><br>
## Architecture & Data Flow
![Architecture Diagram](docs/project_architecture.png)

Data flows through three distinct stages:
1. **Bronze (Ingestion Layer):** Raw data is extracted from PostgreSQL.
2. **Silver (Processing Layer):** Split into two distinct phases:
   * **Technical:** Cleanses, deduplicates, and standardizes initial data types across 6 core entities.
   * **Business:** Integrates the cleansed entities into a unified 'One-Big Table' view representing core business logic.
3. **Gold (Reporting Layer):** Generates final dimensional models using dbt ephemeral tables and snapshots for slowly changing dimensions, alongside standard fact tables.
<br><br>

## Tech Stack
* **Orchestration:** Apache Airflow
* **Transformation:** dbt (Data Build Tool)
* **Processing Engine:** Databricks
* **Database:** PostgreSQL (Using neon.tech)
* **Languages:** Python, SQL
<br><br>

## Repository Structure
* **`/data_ingestion`**<br>
  Contains the Python scripts using `psycopg2` to establish the initial PostgreSQL database source and load the raw retail dataset.
* **`/dbt`**<br>
  Houses the dbt project containing the Medallion architecture logic. Includes custom macros for data testing, logic for materializing different layers of tables, ephemeral table and dimensional snapshots (which creates SCD Type 2 tables).
  Final Output data is in form of Star Schema containing 4 dimension and 1 fact table.
* **`/airflow`**<br>
  Contains the DAGs that schedule and orchestrate the pipeline (currently scheduled daily at 11:00 AM IST). 
<br><br>

## Data Modeling Strategy & Lineage
![dbt Lineage Diagram](docs/lineage_diagram.png)

The transformation logic relies on dbt techniques to optimize performance and track historical changes:
* **Ephemeral Tables:** Used to optimize intermediate queries without materializing unnecessary tables in the warehouse.
* **Snapshots:** Implemented to build Type 2 Slowly Changing Dimensions (SCDs) in the Gold layer, accurately tracking historical states of dimensional records.
* **Data Quality:** Primary key integrity checks and custom threshold macros are enforced at the Silver layer.
<br><br>

## Orchestration
![Airflow DAG Structure](docs/airflow_dag.png)

Apache Airflow manages the task dependencies and orchestrates complete pipeline from ingesting data into databricks to complete gold layer result.<br>
It ensures data ingestion completes fully for all 6 tables before triggering the dbt transformation jobs in Databricks.
<br><br>


# Setup Guide

## 1. Virtual Environment Setup 
Each folder contains its own virtual environment setup using `uv` instead of `pip` but can be installed using `pip install uv`.<br>
The dbt virtual environment does not need to be set up since it is bind-mounted to the Airflow container and can be run from Airflow. (Required for development)<br>
The following command sets up the `.venv` in their respective directories:
```bash
uv sync
```

## 2. Environment variables & Secrets
This project requires secrets to be filled in 3 files for running.<br>
`/airflow` and `/ingestion` each contain `.env_clone` files. Rename them to `.env` and fill in the details.<br>
`/dbt` contains `profiles_clone.yml`. Rename it to `profiles.yml` and fill in the compute credentials to connect with the compute.
<br>

## 3. Setting up PostgreSql Database for Source
Navigate to `/data_ingestion`, create .venv, and execute the setup script to load the raw tables.<br>
WORKS WITH ANY POSTGRESQL DB. 
```bash
python ddl/initialize_db.py
```

## 4. Start Airflow: 
Navigate to `/airflow` and start the local environment.
```bash
docker-compose up -d
```

## 5. Trigger the Pipeline
Access the Airflow UI at `localhost:8080` and unpause the DAG to execute the full extraction and transformation process.
User and Password for login are set to defaults, which are : 'airflow'
