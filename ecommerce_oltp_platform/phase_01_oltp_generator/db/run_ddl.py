from pathlib import Path
from sqlalchemy import text
from utils.logger import log
from db.connection import engine


def run_ddl():
    ddl_path = Path(__file__).parent / "ddl.sql"

    log(f"Running DDL from {ddl_path}")

    ddl_sql = ddl_path.read_text(encoding="utf-8")

    with engine.begin() as conn:
        conn.execute(text(ddl_sql))

    log("DDL executed successfully")


if __name__ == "__main__":
    run_ddl()
