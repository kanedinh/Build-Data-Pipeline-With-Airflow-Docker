from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_store_and_fwd_flag(conn_id):
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Create data
    data = {
        'Store_and_fwd_flagID': ['Y', 'N'],
        'Description': ["store and forward trip", "not a store and foward trip"]
    }

    df = pd.DataFrame(data)

    # Save data to warehouse
    warehouse_operator.save_data_to_postgres(df, 'Dim_Store_and_fwd_flag', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_Store_and_fwd_flag table successfully!")