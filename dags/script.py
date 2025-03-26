from airflow import DAG 
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from ingest_data import ingest_data_to_postgres

from transform_dim_vendor import transform_dim_vendor
from transform_dim_location import transform_dim_location
from transform_dim_date import transform_dim_date
from transform_dim_ratecode import transform_dim_ratecode
from transform_dim_store_and_fwd_flag import transform_dim_store_and_fwd_flag
from transform_dim_payment_type import transform_dim_payment_type
from transform_fact_yellow_taxi import transform_fact_yellow_taxi

# 'conn_id' connect to postgreSQL
conn_id: str = 'postgres_local'

url_prefix: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_"
url_yellow_taxi: str = url_prefix + "{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_taxi_file: str = "/opt/airflow/" + "yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
yellow_taxi_table_name: str = "yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}"

url_location: str = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
location_file: str = "/opt/airflow/taxi_zone_lookup.csv"
location_table_name: str = "taxi_zone_lookup"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'end_date': datetime(2024, 2, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False   # set to False to prevent backfilling
}

with DAG(
    dag_id="data_pipeline",
    default_args=default_args,
    schedule_interval="0 0 3 * *",
) as dag:

    # Task download data
    with TaskGroup("download_data") as download_data_group:
        download_yellow_trip_data_task = BashOperator(
            task_id="download_yellow_taxi_data",
            bash_command=f"curl {url_yellow_taxi} > {yellow_taxi_file}"
        )

        download_location_data_task = BashOperator(
            task_id="download_location_data",
            bash_command=f"curl {url_location} > {location_file}",    
        )

    # Task ingest data into postgres
    with TaskGroup("ingest_data_into_postgres") as ingest_data_group:
        ingest_location_data_task = PythonOperator(
            task_id="ingest_location_data_to_postgres",
            python_callable=ingest_data_to_postgres,
            op_kwargs=dict(
                parquet_file=location_file,
                conn_id=conn_id,
                table_name=location_table_name
            )
        )

        ingest_yellow_taxi_data_task = PythonOperator(
            task_id="ingest_yellow_taxi_data_to_postgres",
            python_callable=ingest_data_to_postgres,
            op_kwargs=dict(
                parquet_file=yellow_taxi_file,
                conn_id=conn_id,
                table_name=yellow_taxi_table_name
            )
        )
    
    # Task transform and load dimension
    with TaskGroup("Transform_and_Load_Dimension") as transform_and_load_dimension:
        transform_dim_vendor = PythonOperator(
            task_id="transform_dim_vendor",
            python_callable=transform_dim_vendor,
            op_args=[conn_id]
        )
        transform_dim_date = PythonOperator(
            task_id="transform_dim_date",
            python_callable=transform_dim_date,
            op_args=[yellow_taxi_table_name, conn_id]
        )
        transform_dim_location = PythonOperator(
            task_id="transform_dim_location",
            python_callable=transform_dim_location,
            op_args=[location_table_name, conn_id]
        )
        transform_dim_ratecode = PythonOperator(
            task_id="transform_dim_ratecode",
            python_callable=transform_dim_ratecode,
            op_args=[conn_id]
        )
        transform_dim_store_and_fwd_flag = PythonOperator(
            task_id="transform_dim_store_and_fwd_flag",
            python_callable=transform_dim_store_and_fwd_flag,
            op_args=[conn_id]
        )
        transform_dim_payment_type = PythonOperator(
            task_id="transform_dim_payment_type",
            python_callable=transform_dim_payment_type,
            op_args=[conn_id]
        )

    # Task transform and load fact
    with TaskGroup("Transform_and_Load_Fact") as transform_and_load_fact:
        transform_fact_table = PythonOperator(
            task_id="transform_fact_table",
            python_callable=transform_fact_yellow_taxi,
            op_args=[yellow_taxi_table_name,conn_id]
        )

    download_data_group >> ingest_data_group >> transform_and_load_dimension >> transform_and_load_fact