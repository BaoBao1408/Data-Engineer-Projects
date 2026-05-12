"""
tests/conftest.py — Shared pytest fixtures.
"""
import os
import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock

# Force local mode cho tất cả tests
os.environ["PIPELINE_ENV"] = "local"

from tests.fixtures.sample_data import (
    operator_a_df, operator_b_df, operator_c_df,
    tracking_codes_df, clicks_df, dim_campaigns_df, page_events_df,
)


@pytest.fixture(scope="session")
def run_date() -> date:
    return date(2026, 1, 15)


@pytest.fixture
def df_operator_a(run_date):
    df = operator_a_df(run_date)
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
    df["event_code"] = df["event_code"].astype("Int64")
    df["amount"]     = pd.to_numeric(df["amount"], errors="coerce")
    return df


@pytest.fixture
def df_operator_b(run_date):
    df = operator_b_df(run_date)
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["amount"]     = pd.to_numeric(df["amount"], errors="coerce")
    return df


@pytest.fixture
def df_operator_c(run_date):
    df = operator_c_df(run_date)
    df["received_time"] = pd.to_datetime(df["received_time"], utc=True)
    return df


@pytest.fixture
def df_tracking_codes():
    df = tracking_codes_df()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["expired_at"] = pd.to_datetime(df["expired_at"], utc=True)
    return df


@pytest.fixture
def df_clicks():
    df = clicks_df()
    df["clicked_at"] = pd.to_datetime(df["clicked_at"], utc=True)
    return df


@pytest.fixture
def df_dim():
    return dim_campaigns_df()


@pytest.fixture
def df_page_events():
    df = page_events_df()
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df


@pytest.fixture
def mock_warehouse(run_date, df_operator_a, df_operator_b, df_operator_c,
                   df_tracking_codes, df_clicks, df_dim, df_page_events):
    """Mock AWSWarehouse với tất cả raw tables pre-loaded."""
    wh = MagicMock()

    table_map = {
        "raw_operator_a":    df_operator_a,
        "raw_operator_b":    df_operator_b,
        "raw_operator_c":    df_operator_c,
        "raw_tracking_codes": df_tracking_codes,
        "raw_clicks":        df_clicks,
        "dim_campaigns":     df_dim,
        "raw_page_events":   df_page_events,
        "raw_campaigns":     df_dim.rename(columns={"campaign_id": "id"}),
    }

    def mock_query(sql: str, layer: str = "") -> pd.DataFrame:
        # Simple table name extraction from SQL
        sql_lower = sql.lower().strip()
        for table_name, df in table_map.items():
            if table_name in sql_lower:
                # Apply simple WHERE _loaded_date filter if present
                if f"_loaded_date='{run_date}'" in sql or f"_loaded_date = '{run_date}'" in sql:
                    if "_loaded_date" in df.columns:
                        return df[df["_loaded_date"] == str(run_date)].copy()
                return df.copy()
        # Default empty DataFrame
        return pd.DataFrame()

    wh.query.side_effect = mock_query
    wh.write_table.return_value = 0
    wh.execute.return_value = None
    wh.count.return_value = 100

    return wh
