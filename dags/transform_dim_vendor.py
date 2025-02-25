from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_vendor(conn_id):
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Create data
    data = {
        'VendorID': [1, 2],
        'VendorDescription': ["Createive Mobile Technologies, LLC", "VeriFone Inc"]
    }

    df = pd.DataFrame(data)

    df['VendorID'] = df['VendorID'].astype(int)

    # Save data to warehouse
    warehouse_operator.save_data_to_postgres(df, 'Dim_Vendor', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_Vendor table successfully!")