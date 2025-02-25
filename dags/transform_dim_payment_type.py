from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_payment_type(conn_id):
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Create data
    data = {
        'PaymentTypeID': [1, 2, 3, 4, 5, 6],
        'PaymentDescription': ["Credit card", "Cash", "No charge", "Dispute", "Unknown", "Voided trip"]
    }

    df = pd.DataFrame(data)

    df['PaymentTypeID'] = df['PaymentTypeID'].astype(int)

    # Save data to warehouse
    warehouse_operator.save_data_to_postgres(df, 'Dim_PaymentType', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_PaymentType table successfully!")