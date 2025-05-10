from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import sys
import os
sys.path.append("/Users/neelkalavadiya/Practicum_Project_Local/airflow/scripts")



# Import the ETL functions
from ingestion_DAG import process_json_file
from cleaning_DAG import clean_bronze_table
from enrichment_DAG import enrich_bronze_table
from aggregation_DAG import aggregate_gold_layer
from load_DAG import export_gold_tables_to_postgres


# Define the DAG
with DAG(
    dag_id="master_healthcare_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["ETL", "healthcare", "python_operator"]
) as dag:

    start = DummyOperator(task_id="start")

    ingestion_task = PythonOperator(
        task_id="json_ingestion",
        python_callable=process_json_file
    )

    cleaning_task = PythonOperator(
        task_id="clean_bronze_table",
        python_callable=clean_bronze_table
    )

    enrichment_task = PythonOperator(
        task_id="enrich_bronze_table",
        python_callable=enrich_bronze_table
    )

    aggregate_task = PythonOperator(
        task_id="aggregate_gold_layer",
        python_callable=aggregate_gold_layer
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=export_gold_tables_to_postgres
    )

    end = DummyOperator(task_id="end")

    # Task dependencies
    # start >> load_task >> end
    start >> ingestion_task >> cleaning_task >> enrichment_task >> aggregate_task >> load_task >> end
