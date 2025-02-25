from postgres_operator import PostgresOperators
import pandas as pd

def transform_fact_yellow_taxi(table_name, conn_id):
    staging_operator = PostgresOperators(conn_id=conn_id)
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Get data from database
    query = f"""
    SELECT * FROM staging."{table_name}"
    """
    df = staging_operator.get_data_to_df(query)

    # Check the existence of table
    check_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'warehouse' 
        AND table_name = 'Fact_Yellow_taxi'
    )
    """
    table_exists = warehouse_operator.get_data_to_df(check_query).iloc[0, 0]

    # Get lastest TripID
    if table_exists:
        query = """
        SELECT MAX("TripID") FROM warehouse."Fact_Yellow_taxi"
        """
        last_trip_id = warehouse_operator.get_data_to_df(query).iloc[0, 0]

        # Insert column TripID
        df.insert(0, 'TripID', range(last_trip_id, len(df) + last_trip_id))
    else:
        df.insert(0, 'TripID', range(1, len(df) + 1))

    # Transform
    # df['VendorID'] = df['VendorID'].astype(int)
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y%m%d%H%M%S')
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y%m%d%H%M%S')
    # df['passenger_count'] = df['passenger_count'].astype(int) # error -> int type cannot have NaN
    # df['trip_distance'] = df['trip_distance'].astype(float)
    # df['PULocationID'] = df['PULocationID'].astype(int)
    # df['DOLocationID'] = df['DOLocationID'].astype(int)
    # df['RatecodeID'] = df['RatecodeID'].astype(int)
    # df['payment_type'] = df['payment_type'].astype(int)
    # df['fare_amount'] = df['fare_amount'].astype(float)
    # df['extra'] = df['extra'].astype(float)
    # df['mta_tax'] = df['mta_tax'].astype(float)
    # df['tip_amount'] = df['tip_amount'].astype(float)
    # df['tolls_amount'] = df['tolls_amount'].astype(float)
    # df['improvement_surcharge'] = df['improvement_surcharge'].astype(float)
    # df['total_amount'] = df['total_amount'].astype(float)
    # df['congestion_surcharge'] = df['congestion_surcharge'].astype(float)
    # df['Airport_fee'] = df['Airport_fee'].astype(float)

    # Rename columns
    rename = {
        'tpep_pickup_datetime': 'PickupDateID',
        'tpep_dropoff_datetime': 'DropoffDateID',
        'passenger_count': 'Passenger_Count',
        'trip_distance': 'Trip_Distance',
        'RatecodeID': 'RateCodeID',
        'store_and_fwd_flag': 'Store_and_fwd_flagID',
        'payment_type': 'PaymentTypeID',
        'fare_amount': 'Fare_Amount',
        'extra': 'Extra',
        'mta_tax': 'MTA_Tax',
        'tip_amount': 'Tip_Amount',
        'tolls_amount': 'Tolls_Amount',
        'improvement_surcharge': 'Improvement_Surcharge',
        'total_amount': 'Total_Amount',
        'congestion_surcharge': 'Congestion_Surcharge',
        'Airport_fee': 'Airport_Fee'
    }

    column_names = df.columns.tolist()
    print("Before rename:", column_names)
    df = df.rename(columns=rename)
    # Check columns name
    column_names = df.columns.tolist()
    print("After rename:", column_names)


    # Rearrange columns
    df = df[['TripID', 'VendorID', 'PickupDateID', 'DropoffDateID', 'PULocationID', 'DOLocationID', 'RateCodeID', \
        'Store_and_fwd_flagID', 'PaymentTypeID', 'Passenger_Count', 'Trip_Distance', 'Fare_Amount', 'Extra', \
        'MTA_Tax', 'Improvement_Surcharge', 'Tip_Amount', 'Tolls_Amount', 'Total_Amount', 'Congestion_Surcharge', 'Airport_Fee']]

    # Drop duplicates
    df = df.drop_duplicates()

    # exist -> append -> drop_duplicates, no exist -> replace
    if table_exists:
        print("The 'Fact_Yellow_taxi' table already exists. Proceeding with data update...")
        warehouse_operator.save_data_to_postgres(df, 'Fact_Yellow_taxi', schema='warehouse', if_exists='append')
    else:
        print("The 'Fact_Yellow_taxi' table does not exist. Creating a new one...")
        warehouse_operator.save_data_to_postgres(df, 'Fact_Yellow_taxi', schema='warehouse', if_exists='replace')