import os
import pandas as pd
from sqlalchemy import create_engine
import requests
from io import StringIO


    
def get_docks_status_data():
    url_core="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json"
    response_core = requests.get(url_core)
    dict_core = response_core.json()

    return dict_core

def transform_dataframe_status_data(dict_core):
    stations_json = dict_core['data']['stations']
    dataframe_with_station_data = pd.json_normalize(stations_json)

    #dataframe = pd.merge(dataframe_with_station_data, dataframe_with_station_informations, on=["station_id","stationCode"])

    dataframe_with_station_data.drop(columns=["station_id","num_bikes_available","num_bikes_available_types","num_docks_available"],inplace=True)
    dataframe_with_station_data.columns = ["stationcode","numbikesavailable","numdocksavailable","is_installed","is_returning","is_renting","last_reported"]
    return dataframe_with_station_data

def upload_docks_status_data():

    #login informations
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station_status"

    dict_data = get_docks_status_data()

    dataframe = transform_dataframe_status_data(dict_data)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    dataframe.to_sql(name=table_name, con=engine, if_exists='append')
    
    return True    

def get_docks_info_data():
    url_info="https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json"
    response_info = requests.get(url_info)
    dict_info = response_info.json()
    return dict_info

def transform_dataframe_info_data(dict_info):
    stations_informations_json = dict_info['data']['stations']
    dataframe_with_station_informations = pd.json_normalize(stations_informations_json)
    dataframe_with_station_informations.drop(columns=["rental_methods","station_id"],inplace=True)
    dataframe_with_station_informations.columns = ["name","lat","lon","capacity","stationcode"]
    return dataframe_with_station_informations

def upload_docks_info_data():

    #login informations
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station_infos"

    dict_info = get_docks_info_data()

    dataframe = transform_dataframe_info_data(dict_info)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    dataframe.to_sql(name=table_name, con=engine, if_exists='append')
    
    return True    