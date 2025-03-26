from postgres_operator import PostgresOperators
import pandas as pd
from time import time

def transform_dim_date(table_name: str, conn_id: str) -> None:
    staging_operator = PostgresOperators(conn_id=conn_id)
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Get data 
    print("Getting data from database...")
    t_start = time()
    query: str = f"""
    SELECT tpep_pickup_datetime, tpep_dropoff_datetime FROM staging."{table_name}"
    """
    df = staging_operator.get_data_to_df(query)
    t_end = time()
    print("Time: ", t_end - t_start)

    # Concat to 1 columns
    df = df.melt(value_name='datetime', value_vars=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])[['datetime']]

    # Transform
    df['DateID'] = df['datetime'].dt.strftime('%Y%m%d%H%M%S')   # format YYYYMMDDHHMMSS
    df['Date'] = df['datetime'].dt.date                         # full date
    df['Year'] = df['datetime'].dt.year.astype(int)             # year
    df['Quarter'] = df['datetime'].dt.quarter.astype(int)       # quarter
    df['Month'] = df['datetime'].dt.month.astype(int)           # month 
    df['Day'] = df['datetime'].dt.day.astype(int)               # day
    df['Hour'] = df['datetime'].dt.hour.astype(int)             # hour
    df['Minute'] = df['datetime'].dt.minute.astype(int)         # minute
    df['Second'] = df['datetime'].dt.second.astype(int)         # second
    df['Weekday'] = df['datetime'].dt.dayofweek + 2             # weekday (2=2, 1=8)
    df['Weekday'] = df['Weekday'].where(df['Weekday'] <= 7, 1).astype(int)  

    # Drop datetime
    df.drop(columns=['datetime'], inplace=True)

    # Drop duplicates
    df = df.drop_duplicates()

    # Check the existence of table
    check_query: str = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'warehouse' 
        AND table_name = 'Dim_Date'
    )
    """
    table_exists = warehouse_operator.get_data_to_df(check_query).iloc[0, 0]

    # exist -> append -> drop_duplicates, no exist -> replace
    if table_exists:
        print("The 'Dim_Date' table already exists. Proceeding with data update...")
        warehouse_operator.save_data_to_postgres(df, 'Dim_Date', schema='warehouse', if_exists='append')
    else:
        print("The 'Dim_Date' table does not exist. Creating a new one...")
        warehouse_operator.save_data_to_postgres(df, 'Dim_Date', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_Date table successfully!")