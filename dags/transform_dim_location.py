from postgres_operator import PostgresOperators
import pandas as pd

def transform_dim_location(table_name: str, conn_id: str) -> None:
    staging_operator = PostgresOperators(conn_id=conn_id)
    warehouse_operator = PostgresOperators(conn_id=conn_id)

    # Read data from staging to df
    query: str = f"""
    SELECT * FROM staging.{table_name}
    """
    df = staging_operator.get_data_to_df(query)

    # Transnform data
    df['LocationID'] = df['LocationID'].astype(int)
    # df['Borough'] = df['Borough'].str.title()
    # df['Zone'] = df['Zone'].str.title()
    # df['service_zone'] = df['service_zone'].str.title()

    # Save data to warehouse
    warehouse_operator.save_data_to_postgres(df, 'Dim_Location', schema='warehouse', if_exists='replace')

    print("Transform and load Dim_Location table successfully!")