from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow import DAG
import pendulum

from tasks_template import (get_docks_info_data, upload_docks_info_data)


default_args = {
    'owner': 'esteban',
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}
with DAG(
    dag_id="docks_info",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2023,7,10),
    catchup=False
) as dag :
    get_velib_docks_info = PythonOperator(
        task_id = "get_docks_info",
        python_callable = get_docks_info_data
    )

    upload_velib_docks_info = PythonOperator(
        task_id = "upload_docks_info",
        python_callable = upload_docks_info_data
    )
    
    get_velib_docks_info >> upload_velib_docks_info

        