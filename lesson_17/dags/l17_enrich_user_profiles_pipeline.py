from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from table_defs.user_profiles_jsonl import user_profiles_jsonl


DEFAULT_ARGS = {
    'depends_on_past': True,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'max_active_runs': 1,
    'retry_delay': 60,
}


dag = DAG(
    dag_id='l17_enrich_user_profiles_pipeline',
    description="Lesson 17 enrich user profiles pipeline",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    tags=['user_profiles'],
    default_args=DEFAULT_ARGS,
)

task1_user_profiles_data_lake_raw_to_bronze = BigQueryInsertJobOperator(
    task_id='task1_user_profiles_data_lake_raw_to_bronze',
    dag=dag,
    location='us-east1',
    project_id='de-07-ageiev-oleksii-l17',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_user_profiles_data_lake_raw_to_bronze.sql' %}",
            "useLegacySql": False,
            "tableDefinitions": {
                "user_profiles_jsonl": user_profiles_jsonl,
            },
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-bucket-aoleksii-l17",
        'project_id': "de-07-ageiev-oleksii-l17"
    }
)

task2_user_profiles_bronze_to_silver = BigQueryInsertJobOperator(
    task_id='task2_user_profiles_bronze_to_silver',
    dag=dag,
    location='us-east1',
    project_id='de-07-ageiev-oleksii-l17',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_user_profiles_bronze_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-bucket-aoleksii-l17",
        'project_id': "de-07-ageiev-oleksii-l17"
    }
)

task3_user_profiles_enriched_silver_to_gold = BigQueryInsertJobOperator(
    task_id='task3_user_profiles_enriched_silver_to_gold',
    dag=dag,
    location='us-east1',
    project_id='de-07-ageiev-oleksii-l17',
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_user_profiles_enriched_silver_to_gold.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'data_lake_raw_bucket': "de-07-bucket-aoleksii-l17",
        'project_id': "de-07-ageiev-oleksii-l17"
    }
)



task1_user_profiles_data_lake_raw_to_bronze >> task2_user_profiles_bronze_to_silver
task2_user_profiles_bronze_to_silver >> task3_user_profiles_enriched_silver_to_gold

