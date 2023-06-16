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
    url="https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/csv?lang=fr"
    
    response = requests.get(url)
    data = response.text

    return data
    
def upload_data():
    data = get_data()
    df = pd.read_csv(StringIO(data), sep=";")
    user = "airflow"
    password = "airflow"
    host = "postgres" 
    port = "5432" 
    db = "velib"
    table_name = "velib_station"
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')
    return True    