from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_ratecode(conn_id: str) -> None:
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Create data
    data = {
        'RateCodeID': [1, 2, 3, 4, 5, 6],
        'RateDescription': ["Standart rate", "JFK", "Newark", "Nassau or Westchester", "Negotiated fare", "Group ride"]
    }

    df = pd.DataFrame(data)

    df['RateCodeID'] = df['RateCodeID'].astype(int)

    # Save data to warehouse
    warehouse_operator.save_data_to_postgres(df, 'Dim_RateCode', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_RateCode table successfully!")