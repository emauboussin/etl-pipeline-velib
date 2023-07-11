from airflow import DAG
from datetime import datetime, timedelta
import pendulum
from airflow.operators.python import PythonOperator
from tasks_template import (get_data_daily, upload_data_daily)
# @dag(
#     dag_id="load_velib",
#     schedule_interval="@hourly",
#     start_date=pendulum.datetime(2023,6,16),
#     catchup=False
# )

default_args = {
    'owner': 'esteban',
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}
with DAG(
    dag_id="load_velib_daily",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023,7,10),
    catchup=False
) as dag :
    get_daily = PythonOperator(
        task_id = "get_data_daily",
        python_callable = get_data_daily
    )

    upload_daily = PythonOperator(
        task_id = "upload_data_daily",
        python_callable = upload_data_daily
    )
    
    get_daily >> upload_daily

        