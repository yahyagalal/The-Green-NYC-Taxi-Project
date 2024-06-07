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





dataset = 'green_tripdata_2019-01.csv'

raw_data_path ='/opt/airflow/data/green_tripdata_2019-01.csv'

cleaned_data_path = "/opt/airflow/data/cleaned_green_tripdata_2019-01.csv"






def wait_for_postgres():
    while True:
        try:
            # Try to connect to PostgreSQL
            connection = psycopg2.connect(
                host='postgres',
                user='root',
                password='root',
                dbname='taxii'
            )
            connection.close()
            print("PostgreSQL is ready.")
            break
        except OperationalError as e:
            print(f"PostgreSQL is not ready yet. Error: {e}")
            time.sleep(2)  # Wait for 2 seconds before the next attempt


def load_and_clean():

    # Define the percentage of the dataset to process (20%)
    dataset_percentage = 20

    # Calculate the number of rows to skip
    total_rows = pd.read_csv(raw_data_path, nrows=1).shape[0]
    skip_rows = int((dataset_percentage / 100) * total_rows)

    # Load the raw dataset into a DataFrame, skipping rows
    data = pd.read_csv(raw_data_path, skiprows=range(1, skip_rows + 1))
  

    # Renaming columns to remove spaces
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.lower()

    # Checking for and removing duplicate rows, considering all columns
    duplicates = data[data.duplicated(keep=False)]
    data_no_duplicates = data.drop_duplicates()

    # Handling missing values and categorical data
    data['passenger_count'].fillna(data['passenger_count'].median(), inplace=True)
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data = data[data['lpep_pickup_datetime'].dt.year.isin([2019])]
    data = data[data['vendor'] != 'Unknown']
    most_frequent_category = data['store_and_fwd_flag'].mode()[0]
    data['store_and_fwd_flag'].fillna(most_frequent_category, inplace=True)
    most_frequent_rate = data['rate_type'].mode()[0]
    data['rate_type'] = data['rate_type'].replace('Unknown', most_frequent_rate)
    different_start_end = data[data['pu_location'] != data['do_location']]
    data = different_start_end.copy()

    # Handling zero values in 'trip_distance'
    median_trip_distance = data[data['trip_distance'] != 0]['trip_distance'].median()
    data.loc[data['trip_distance'] == 0, 'trip_distance'] = median_trip_distance

    # Handling negative values in 'fare_amount'
    fare_amount_median = data['fare_amount'].median()
    data.loc[data['fare_amount'] < 0, 'fare_amount'] = fare_amount_median

    # Handling missing and zero fare amounts
    fare_mean = data[(data['fare_amount'].notnull()) & (data['fare_amount'] != 0)]['fare_amount'].mean()
    data['fare_amount'].fillna(fare_mean, inplace=True)

    # Handling 'extra' column
    median_extra = data[data['extra'] >= 0]['extra'].median()
    data['extra'] = data['extra'].apply(lambda x: median_extra if x < 0 else x)
    data['extra'].fillna(0, inplace=True)

    # Handling 'mta_tax' column
    median_mta_tax = data[data['mta_tax'] >= 0]['mta_tax'].median()
    data['mta_tax'] = data['mta_tax'].apply(lambda x: median_mta_tax if x < 0 else x)

    # Handling 'tip_amount' column
    mean_tip_amount = data[data['tip_amount'] >= 0]['tip_amount'].mean()
    data['tip_amount'] = data['tip_amount'].apply(lambda x: mean_tip_amount if x < 0 else x)

    # Dropping 'ehail_fee' column
    data.drop('ehail_fee', axis=1, inplace=True)

    # Handling 'improvement_surcharge' column
    mode_improvement_surcharge = data['improvement_surcharge'].mode()[0]
    data.loc[data['improvement_surcharge'] < 0, 'improvement_surcharge'] = mode_improvement_surcharge

    # Handling 'total_amount' column
    median_total_amount = data['total_amount'].median()
    data.loc[data['total_amount'] < 0, 'total_amount'] = median_total_amount

    # Handling 'payment_type' column
    mode_payment_type = data['payment_type'].mode()[0]
    data['payment_type'].replace(['Unknown', 'Uknown'], mode_payment_type, inplace=True)

    # Handling 'trip_type' column
    mode_trip_type = data['trip_type'].mode()[0]
    data['trip_type'].replace('Unknown', mode_trip_type, inplace=True)

    # Dropping 'congestion_surcharge' column
    data.drop('congestion_surcharge', axis=1, inplace=True)

    # Handling 'passenger_count' outliers
    mode_passenger_count = data['passenger_count'].mode()[0]
    data.loc[data['passenger_count'] == 444, 'passenger_count'] = mode_passenger_count

    # Handling outliers using IQR for specific columns
    data['trip_distance'] = handle_outliers_iqr(data['trip_distance'])
    data['fare_amount'] = handle_outliers_iqr(data['fare_amount'])
    data['tip_amount'] = handle_outliers_iqr(data['tip_amount'])
    data['tolls_amount'] = handle_outliers_iqr(data['tolls_amount'])
    data['total_amount'] = handle_outliers_iqr(data['total_amount'])

    # Feature engineering
    data['lpep_pickup_datetime'] = pd.to_datetime(data['lpep_pickup_datetime'])
    data['week_number'] = data['lpep_pickup_datetime'].dt.isocalendar().week
    data['week_range_beginning'] = data['lpep_pickup_datetime'] - pd.to_timedelta(data['lpep_pickup_datetime'].dt.dayofweek, unit='D')

    

    
    # data = pd.read_csv(cleaned_data_path)

    data = pd.get_dummies(data, columns=['vendor'])
    label_encoder = LabelEncoder()
    data['extra_label_encoded'] = label_encoder.fit_transform(data['extra'])

    # Save the cleaned dataset to a new CSV file

    # transformed_data_path = "/opt/airflow/data/transformed_green_tripdata_2019-01.csv"

    

    # Save the cleaned dataset to a new CSV file
    data.to_csv(cleaned_data_path, index=False)
    print("CSV file saved")


    return data



def create_dashboard():
    # Load your cleaned dataset
    df_cleaned = pd.read_csv(cleaned_data_path)

    # Create a Dash web application
    app = dash.Dash()

    # Define layout of the dashboard
    app.layout = html.Div(children=[
        html.H1(children='Yahia Galal, 49-8349, and MET Dashboard'),

        # Graph 1: Example scatter plot
        dcc.Graph(
            id='scatter-plot',
            figure=px.scatter(df_cleaned, x='trip_distance', y='fare_amount', title='Scatter Plot'),
        ),

        # Graph 2: Example bar chart
        dcc.Graph(
            id='bar-chart',
            figure=px.bar(df_cleaned, x='passenger_count', y='total_amount', title='Bar Chart'),
        ),

        # Graph 3: Example pie chart
        dcc.Graph(
            id='pie-chart',
            figure=px.pie(df_cleaned, names='tip_amount', title='Pie Chart'),
        ),
    ])

    app.run_server(host='localhost')

if __name__ == '__main__':
    create_dashboard()




def load_to_postgres():
    df = pd.read_csv(cleaned_data_path)
    engine = create_engine('postgresql://root:root@pgdatabase:5432/taxii')
    if engine.connect():
        print('Connected successfully')
    else:
        print('Failed to connect')
    df.to_sql(name='taxi_1_2019', con=engine, if_exists='replace')
