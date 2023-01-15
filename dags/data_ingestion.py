from airflow import DAG
from airflow.decorators import task
from google.cloud import storage, bigquery
from datetime import datetime
import pandas as pd
import requests
import json

with DAG(dag_id='data_ingestion', start_date=datetime.now(), schedule='@once'):

    @task
    def call_dataset():
        data = requests.get('https://datausa.io/api/data?drilldowns=Nation&measures=Population').content
        data = json.loads(data)
        with open('outputs/data.json', 'w') as file:
            json.dump(data, file)

    @task
    def save_as_csv():
        with open('outputs/data.json', 'r') as file:
            data = json.load(file)
        df = pd.json_normalize(data, record_path=['data'])
        df.to_csv('outputs/data.csv', index=False)

    @task
    def format_to_parquet():
        df = pd.read_csv('outputs/data.csv')
        df.to_parquet('outputs/data.parquet')
    
    call_dataset() >> save_as_csv() >> format_to_parquet() 