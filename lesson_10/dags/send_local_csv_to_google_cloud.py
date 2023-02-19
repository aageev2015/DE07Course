import json
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

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
    dag_id='send_local_csv_to_google_cloud',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 3),
    schedule_interval="0 1 * * *",
    catchup=True,
    default_args=DEFAULT_ARGS,
)

task1_send_local_csv_to_google_cloud = LocalFilesystemToGCSOperator(
    task_id='send_local_csv_to_google_cloud_task',
    dag=dag,
    src=os.path.join(Variable.get("lesson_10_local_absolute_src_path_keyfile_dict"), '{{ ds }}/*.csv'),

    dst='src1/sales/v1/{{ logical_date.strftime("year=%Y/month=%m/day=%d") }}/',
    bucket='de-07-bucket-ao-0253553471'
)

task1_send_local_csv_to_google_cloud



