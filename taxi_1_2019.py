import os
import time
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from psycopg2 import OperationalError
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import dash
import dash_core_components as dcc
import dash_html_components as html
from sqlalchemy import create_engine
from preprocessing_functions import handle_outliers_iqr
import psycopg2
import plotly.express as px

from airflow.configuration import conf

from functions import *


dags_folder = conf.get('core', 'DAGS_FOLDER')
print(dags_folder)



raw_data_path ='/opt/airflow/data/green_tripdata_2019-01.csv'

cleaned_data_path = "/opt/airflow/data/cleaned_green_tripdata_2019-01.csv"




print("Current working directory:", os.getcwd())
print("Absolute path to raw data file:", os.path.abspath(raw_data_path))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    'start_date': days_ago(2),
    "retries": 1,
}

dag = DAG(
    'green_trip_etl_pipeline',
    default_args=default_args,
    description='Green Trip ETL Pipeline',
    schedule_interval=None,
)

with dag:
    extract_clean_task = PythonOperator(
        task_id='extract_clean',
        python_callable=load_and_clean,
    )

    create_dashboard_task = PythonOperator(
        task_id='create_dashboard',
        python_callable=create_dashboard,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

extract_clean_task  >> load_to_postgres_task >> create_dashboard_task
