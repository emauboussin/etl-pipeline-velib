import os
from airflow.decorators import dag, task
import pendulum
import pandas as pd

@dag(
    dag_id="load_velib",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2023,6,16),
    catchup=False
)

def load_velib():
    @task()
    def get_data():
        url="https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/csv?lang=fr"
        if url.endswith('.csv.gz'):
            csv_name = '../data/output.csv.gz'
        else:
            csv_name = '../data/output.csv'
        os.system(f"wget {url} -O {csv_name}")
        return csv_name
    @task()
    def get_number_lines(csv_name):
        df = pd.read_csv(csv_name, sep=";")
    
        df.columns = ["id","name","working","capacity","park_available","bike_available","classic_bike_available","eletric_bike_available","payment_available","possible_park","actualisation_time","geographic_data","city_name","insee_code"]
        df["actualisation_time"] = pd.to_datetime(df["actualisation_time"])
        df[['lat', 'long']] = df['geographic_data'].str.split(',', 1, expand=True)

        return df.shape
    
    df_shape = get_number_lines(get_data())



load = load_velib()