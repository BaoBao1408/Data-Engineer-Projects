import psycopg2
from config import load_config

# PG_CONFIG = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "Tiki_Data",
#     "user": "postgres",
#     "password": "123456",  
# }

DDL = """
CREATE TABLE IF NOT EXISTS tiki_products (
    id           BIGINT PRIMARY KEY,
    name         TEXT,
    url_key      TEXT,
    price        BIGINT,
    description  TEXT,
    images       JSONB
);
"""

def main():
    config = load_config()  

    try:
        with psycopg2.connect(**config) as conn:
            with conn.cursor() as cur:
                cur.execute(DDL)
                conn.commit()
                print("Table tiki_products Created/Exists already!.")
    except OperationalError as e:
        print(f"[ERROR] Database connection failed: {e}")
    except DatabaseError as e:
        print(f"[ERROR] Error while executing DDL: {e}")
    except Exception as e:
        print("Error creating table:", e)

if __name__ == "__main__":
    main()
