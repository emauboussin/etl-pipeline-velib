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
    
def get_data_hourly():
    url_core="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    response_core = requests.get(url_core)
    dict_core = response_core.json()

    return dict_core

def transform_dataframe_hourly(dict_core):
    stations_json = dict_core['data']['stations']
    dataframe_with_station_data = pd.json_normalize(stations_json)

    #dataframe = pd.merge(dataframe_with_station_data, dataframe_with_station_informations, on=["station_id","stationCode"])

    dataframe_with_station_data.drop(columns=["num_bikes_available","num_bikes_available_types","num_docks_available"],inplace=True)

    return dataframe_with_station_data

def upload_data_hourly():

    #login informations
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station_status"

    dict_data = get_data_hourly()

    dataframe = transform_dataframe_hourly(dict_data)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    dataframe.to_sql(name=table_name, con=engine, if_exists='append')
    
    return True    

def get_data_daily():
    url_info="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
    response_info = requests.get(url_info)
    dict_info = response_info.json()
    return dict_info

def transform_dataframe_daily(dict_info):
    stations_informations_json = dict_info['data']['stations']
    dataframe_with_station_informations = pd.json_normalize(stations_informations_json)
    dataframe_with_station_informations.drop(columns=["rental_methods"],inplace=True)

    return dataframe_with_station_informations

def upload_data_daily():

    #login informations
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station_infos"

    dict_info = get_data_daily()

    dataframe = transform_dataframe_daily(dict_info)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    dataframe.to_sql(name=table_name, con=engine, if_exists='append')
    
    return True    