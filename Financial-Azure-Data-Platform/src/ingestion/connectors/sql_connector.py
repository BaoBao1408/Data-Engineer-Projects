"""
SQL Connector – supports PostgreSQL (local dev) and Azure SQL Server (production).
Uses SQLAlchemy 2.0 with connection pooling + retry logic.
"""
from contextlib import contextmanager
from typing import Any, Generator, Optional

import pyodbc
import sqlalchemy as sa
from loguru import logger
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import get_settings

settings = get_settings()


class WarehouseConnector:
    """
    PostgreSQL connector (local dev / staging).
    Maps to Azure SQL Database in production via WarehouseConnectorAzureSQL.
    """

    def __init__(self, database_url: Optional[str] = None):
        self._url = database_url or settings.database.url
        self._engine: Optional[Engine] = None
        self._session_factory: Optional[sessionmaker] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(
                self._url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,         # Verify connection before use
                pool_recycle=3600,          # Recycle connections every 1h
                echo=settings.is_development,
            )
            logger.info(f"SQLAlchemy engine created: {self._url.split('@')[-1]}")
        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        if self._session_factory is None:
            self._session_factory = sessionmaker(
                bind=self.engine,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False,
            )
        return self._session_factory

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Context manager that auto-commits or rolls back."""
        db = self.session_factory()
        try:
            yield db
            db.commit()
        except Exception as exc:
            db.rollback()
            logger.error(f"DB session error, rolled back: {exc}")
            raise
        finally:
            db.close()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        reraise=True,
    )
    def execute(self, sql: str, params: Optional[dict] = None) -> list[dict]:
        """Execute raw SQL and return list of dicts."""
        with self.session() as db:
            result = db.execute(text(sql), params or {})
            if result.returns_rows:
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]
            return []

    def execute_many(self, sql: str, params_list: list[dict]) -> int:
        """Bulk execute SQL. Returns rows affected."""
        with self.session() as db:
            result = db.execute(text(sql), params_list)
            return result.rowcount

    def bulk_insert_df(
        self,
        df,
        table_name: str,
        schema: str = "public",
        if_exists: str = "append",
        chunksize: int = 1000,
    ) -> int:
        """
        Bulk insert a Pandas DataFrame into a table.
        if_exists: 'append' | 'replace' | 'fail'
        """
        rows = df.to_sql(
            name=table_name,
            con=self.engine,
            schema=schema,
            if_exists=if_exists,
            index=False,
            chunksize=chunksize,
            method="multi",
        )
        logger.info(f"Inserted {rows} rows into {schema}.{table_name}")
        return rows or 0

    def health_check(self) -> bool:
        try:
            result = self.execute("SELECT 1 AS ok")
            return result[0]["ok"] == 1
        except Exception as exc:
            logger.error(f"Warehouse health check failed: {exc}")
            return False


class AzureSQLConnector:
    """
    MS SQL Server connector via pyodbc / ODBC Driver 18.
    Used in production targeting Azure SQL Database.
    """

    def __init__(self):
        self._conn_str = settings.azure_sql.connection_string
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            connection_url = sa.engine.URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": self._conn_str},
            )
            self._engine = create_engine(
                connection_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=1800,
                fast_executemany=True,      # Bulk insert optimization
            )
            logger.info("Azure SQL Engine created")
        return self._engine

    @contextmanager
    def raw_connection(self) -> Generator[pyodbc.Connection, None, None]:
        """Raw pyodbc connection for stored procedures / bulk copy."""
        conn = pyodbc.connect(self._conn_str, timeout=30)
        conn.autocommit = False
        try:
            yield conn
            conn.commit()
        except Exception as exc:
            conn.rollback()
            logger.error(f"Azure SQL error, rolled back: {exc}")
            raise
        finally:
            conn.close()

    def execute_stored_procedure(
        self, proc_name: str, params: Optional[dict] = None
    ) -> list[dict]:
        """Call a stored procedure and return results."""
        param_str = ", ".join(
            [f"@{k} = ?" for k in (params or {}).keys()]
        )
        sql = f"EXEC {proc_name} {param_str}"
        values = list((params or {}).values())

        with self.raw_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, values)
            if cursor.description:
                columns = [col[0] for col in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
            return []

    def bulk_copy(self, df, table_name: str, schema: str = "dbo") -> int:
        """High-performance bulk insert using fast_executemany."""
        cols = list(df.columns)
        placeholders = ", ".join(["?" for _ in cols])
        col_names = ", ".join([f"[{c}]" for c in cols])
        sql = f"INSERT INTO [{schema}].[{table_name}] ({col_names}) VALUES ({placeholders})"

        with self.raw_connection() as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(sql, df.values.tolist())
            rows = cursor.rowcount
            logger.info(f"Bulk copied {rows} rows → [{schema}].[{table_name}]")
            return rows


def get_warehouse() -> WarehouseConnector:
    """FastAPI / Airflow dependency injection factory."""
    if settings.is_production and settings.azure_sql.server:
        # In production, build SQLAlchemy engine over Azure SQL
        conn = AzureSQLConnector()
        sa_url = sa.engine.URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": settings.azure_sql.connection_string},
        )
        return WarehouseConnector(database_url=str(sa_url))
    return WarehouseConnector()
