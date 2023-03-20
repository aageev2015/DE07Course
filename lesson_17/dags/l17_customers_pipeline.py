from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.customers_csv import customers_csv

DEFAULT_ARGS = {
    'depends_on_past': True,
    'wait_for_downstream': True,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
    'retry_delay': 60,
}


dag = DAG(
    dag_id='l17_customers_pipeline',
    description="Lesson 17 customers pipeline",
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 6),
    schedule_interval="0 1 * * *",
    catchup=True,
    tags=['customers'],
    default_args=DEFAULT_ARGS,
)

task1_customers_data_lake_raw_to_bronze = BigQueryInsertJobOperator(
    task_id='task1_customers_data_lake_raw_to_bronze',
    dag=dag,
    location='us-east1',
    project_id='de-07-ageiev-oleksii-l17',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_data_lake_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "customers_csv": customers_csv,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-bucket-aoleksii-l17",
        'project_id': "de-07-ageiev-oleksii-l17"
    }
)

task2_customers_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='task2_customers_bronze_to_silver',
    dag=dag,
    location='us-east1',
    project_id='de-07-ageiev-oleksii-l17',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': "de-07-ageiev-oleksii-l17"
    }
)


task1_customers_data_lake_raw_to_bronze >> task2_customers_bronze_to_silver

