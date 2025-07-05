import os
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from airflow.providers.postgres.hooks.postgres import PostgresHook
from urllib.parse import quote_plus
from sqlalchemy import create_engine

def get_sqlalchemy_connection():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_dwh')
    conn = postgres_hook.get_connection(postgres_hook.postgres_conn_id)
    
    user = conn.login
    password = quote_plus(conn.password)  # cần encode password
    host = conn.host
    port = conn.port
    db = conn.schema

    uri = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(uri)
    return engine

def get_last_loaded_timestamp():
    """
    Retrieve the last loaded timestamp from the database.
    If no data exists, return datetime.min.
    """
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_dwh')
    sql = "SELECT MAX(updated_at) FROM orders;"
    result = postgres_hook.get_first(sql)
    last_loaded = result[0] if result and result[0] else None
    return last_loaded if last_loaded else datetime.min


def load_data_to_staging(path_dirr):
    """
    Load delta data newer than the last loaded timestamp into staging tables.
    """
    last_loaded = get_last_loaded_timestamp()
    engine = get_sqlalchemy_connection()

    with engine.begin() as conn:
        for root, _, files in os.walk(path_dirr):
            for file in files:
                if file.endswith('.csv'):
                    table = file.split('.')[0]
                    file_path = os.path.join(root, file)

                    folder_date = root.split('/')[-2]  # Example: 20250_
