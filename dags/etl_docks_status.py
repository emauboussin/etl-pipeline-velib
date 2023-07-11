from airflow.operators.python import PythonOperator
from datetime import timedelta
from airflow import DAG
import pendulum

from tasks_template import (get_docks_status_data, upload_docks_status_data)


default_args = {
    'owner': 'esteban',
    'retries': 5,
    'retry_delay' : timedelta(minutes=5)
}
with DAG(
    dag_id="docks_status",
    schedule_interval=timedelta(seconds=60),
    start_date=pendulum.datetime(2023,7,10),
    catchup=False
) as dag :
    get_status = PythonOperator(
        task_id = "get_docks_status",
        python_callable = get_docks_status_data
    )

    upload_status = PythonOperator(
        task_id = "upload_docks_status",
        python_callable = upload_docks_status_data
    )
    
    get_status >> upload_status

        
