import os
import pandas as pd
from sqlalchemy import create_engine
import requests
from io import StringIO

# def create_db():
#     create_db = PostgresOperator(
#     task_id="create_velib_db",
#     sql="""SELECT 'CREATE DATABASE velib'
#         WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'velib')\gexec
#         """
#     )
    
def get_data():
    url_core="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    response_core = requests.get(url_core)
    dict_core = response_core.json()

    url_info="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
    response_info = requests.get(url_info)
    dict_info = response_info.json()

    return dict_core, dict_info 

def transform_dataframe(dict_core, dict_info):
    stations_json = dict_core['data']['stations']
    dataframe_with_station_data = pd.json_normalize(stations_json)

    stations_informations_json = dict_info['data']['stations']
    dataframe_with_station_informations = pd.json_normalize(stations_informations_json)

    dataframe = pd.merge(dataframe_with_station_data, dataframe_with_station_informations, on=["station_id","stationCode"])

    dataframe.drop(columns=["num_bikes_available","num_bikes_available_types","num_docks_available","rental_methods"],inplace=True)

    return dataframe

def upload_data():

    #login informations
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station"

    dict_c, dict_i = get_data()

    dataframe = transform_dataframe(dict_c, dict_i)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #df = transform_dataframe(dataframe)
    
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    dataframe.to_sql(name=table_name, con=engine, if_exists='append')
    return True    