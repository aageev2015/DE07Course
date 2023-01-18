import json
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator

DEFAULT_ARGS = {
    'depends_on_past': False,
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': 30,
}


dag = DAG(
    dag_id='process_sales',
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 12),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

task1_extract_data_from_api = SimpleHttpOperator(
    task_id='extract_data_from_api',
    dag=dag,
    http_conn_id='conn_extract_data_api',
    endpoint='/',
    method="post",
    headers={"Content-Type": "application/json"},
    data=json.dumps({
          "date": "{{ ds }}",
          "raw_dir": "/raw/sales/{{ ds }}"
        }),
    response_check=lambda response: response.status_code == 201,
)

task2_convert_to_avro = SimpleHttpOperator(
    task_id='convert_to_avro',
    dag=dag,
    http_conn_id='conn_convert_to_avro_api',
    endpoint='/',
    method="post",
    headers={"Content-Type": "application/json"},
    data=json.dumps({
            "stg_dir": "/stg/sales/{{ ds }}",
            "raw_dir": "/raw/sales/{{ ds }}"
        }),
    response_check=lambda response: response.status_code == 201,
)

task1_extract_data_from_api >> task2_convert_to_avro



