from sqlalchemy import text
from db.connection import engine

DDL_FILE = "db/ddl.sql"

def run_ddl():
    with open(DDL_FILE, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    with engine.connect() as conn:
        conn.execute(text(ddl_sql))
        conn.commit()

    print("DDL executed successfully")

if __name__ == "__main__":
    run_ddl()
