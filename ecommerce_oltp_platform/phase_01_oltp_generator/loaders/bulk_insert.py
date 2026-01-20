from sqlalchemy import text
from db.connection import engine


def bulk_insert(table_name: str, columns: list[str], rows: list[tuple]):
    if not rows:
        return

    placeholders = ", ".join([f":{c}" for c in columns])
    sql = text(
        f"""
        INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        """
    )

    data = [dict(zip(columns, row)) for row in rows]

    with engine.begin() as conn:
        conn.execute(sql, data)
    