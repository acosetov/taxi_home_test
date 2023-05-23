import pandas as pd
import os
import zipfile
import csv
from datetime import datetime
import uuid


from airflow.models import DAG, Variable
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from google.cloud import bigquery



def remove_header_and_columns_with_words(csv_path):
    """
    This function removed header from csv file and columns
    and columns that don't match specification
    """

    special_words = ["pick_noid", "pickup_noid"]
    keyword = "VendorID"
    
    # Detect delimiter
    delimiter1 = '\t'
    delimiter2 = '|'

    df = pd.read_csv(csv_path, delimiter=delimiter1)
    if len(df.columns) == 1:
        df = pd.read_csv(csv_path, delimiter=delimiter2)

    
    # Remove columns with special words
    columns_to_drop = [column for column in df.columns if any(word in str(column) for word in special_words)]
    df = df.drop(columns=columns_to_drop)
    
    # Remove header
    header_row = None
    if keyword in df.iloc[0].values:
        header_row = df[df.apply(lambda row: keyword in row.values, axis=1)].iloc[0]
        df = df.iloc[df.index.get_loc(header_row.name) + 1:].reset_index(drop=True)
    else:
        df.columns = range(len(df.columns))
    
    return df



def load_data_to_bigquery():
   
    hook = GoogleCloudStorageHook()
    files = hook.list(Variable.get("bucket_name"), prefix=None, delimiter='.csv.zip')

    client = bigquery.Client.from_service_account_json(Variable.get('gb_access_key_path'))
    
    for file in files:
        path_file = f'gs://{Variable.get("bucket_name")}/{file}'
        print(path_file)
        df = remove_header_and_columns_with_words(path_file)
        
        column_names = Variable.get('column_names').split(',')
        df.columns = column_names
        df['key'] = [uuid.uuid4() for _ in range(len(df))]

        coodrinate_fields = ['pickup_longitude', 'pickup_latitude', 'dropoff_latitude', 'dropoff_longitude']
        for cooerinate_value in coodrinate_fields:
            df = df.drop(df[(df[cooerinate_value] < -90) | (df[cooerinate_value] > 90)].index)
        
        source_data = '/home/acosetov/airflow_project/dags/csv_files/data.csv' 
        df.to_csv(source_data, header=False, index=False, sep='|')
    
        table_ref = Variable.get("table_ref")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter='|'
        )
        with open(source_data, 'rb') as source_file:
            job = client.load_table_from_file(
                source_file,
                table_ref,
                job_config=job_config,
            )

        job.result()
        os.remove(source_data)



default_args = {
    'start_date': datetime(2023, 5, 21)
}


with DAG('taxi_home_test', default_args=default_args, schedule_interval=None) as dag:
    load_data_to_bigquery_task = PythonOperator(
        task_id='load_data_to_bigquery_task',
        python_callable=load_data_to_bigquery
    )

    create_data_table_task = BigQueryExecuteQueryOperator(
        task_id='create_data_table',
        gcp_conn_id='my_bigquery_connection',
        sql='''
            CREATE TABLE `taxi_dataset.data` AS
            SELECT
              vendor_id,
              tpep_pickup_datetime,
              tpep_dropoff_datetime,
              passenger_count,
              trip_distance,
              rate_code_id,
              store_and_fwd_flag,
              payment_type,
              fare_amount,
              extra,
              mta_tax,
              tip_amount,
              tolls_amount,
              improvement_surcharge,
              total_amount,
              key
            FROM
              `taxi_dataset.yellow_taxi`
        ''',
        use_legacy_sql=False
    )


    create_geometries_table_task = BigQueryExecuteQueryOperator(
        task_id='create_geometries_table',
        gcp_conn_id='my_bigquery_connection',
        sql='''
            CREATE TABLE `taxi_dataset.geometries` AS
            SELECT
                key,
                ST_GeogPoint(pickup_longitude, pickup_latitude) AS pickup_geometry,
                ST_GeogPoint(dropoff_longitude, dropoff_latitude) AS dropoff_geometry
            FROM
                `taxi_dataset.yellow_taxi`
        ''',
        use_legacy_sql=False
    )

    load_data_to_bigquery_task >> create_data_table_task >> create_geometries_table_task
