import psycopg2
from psycopg2 import DatabaseError, OperationalError
from config import load_config

def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect(**config) as conn:
            print('Connected to the PostgreSQL Server!.')
            return conn
    except OperationalError as e:
        print(f"[ERROR] Could not connect to PostgreSQL: {e}")
    except Exception as e:
        print(f"[ERROR] Unexpected error while connecting to DB: {e}")
if __name__ == '__main__':
    config = load_config()
    connect(config)