"""
tests/fixtures/sample_data.py — Shared test fixtures.

Import trong conftest.py hoặc trực tiếp trong test files.
Tất cả fixtures là pandas DataFrames với schema đúng của production.
"""
from __future__ import annotations

from datetime import date
import pandas as pd


RUN_DATE = date(2026, 1, 15)


# ── Raw operator DataFrames ───────────────────────────────────────

def operator_a_df(run_date: date = RUN_DATE) -> pd.DataFrame:
    return pd.DataFrame({
        "transaction_id": ["tx_A_001", "tx_A_002", "tx_A_003", "tx_A_004"],
        "rotate_id":      ["rot_001",  "rot_002",  "rot_001",  "rot_003"],
        "msisdn":         ["447700900001", "447700900002", "447700900001", "447700900003"],
        "event_code":     [1, 1, 2, 1],          # 1=SUB, 2=BILL
        "status":         ["SUCCESS", "SUCCESS", "SUCCESS", "FAILED"],
        "amount":         [None, None, 4.99, None],
        "currency":       ["GBP", "GBP", "GBP", "GBP"],
        "event_time":     [
            "2026-01-15 09:30:00+00",
            "2026-01-15 10:00:00+00",
            "2026-01-15 11:00:00+00",
            "2026-01-15 12:00:00+00",
        ],
        "_loaded_date": [str(run_date)] * 4,
    })


def operator_b_df(run_date: date = RUN_DATE) -> pd.DataFrame:
    return pd.DataFrame({
        "transaction_id":   ["tx_B_001", "tx_B_002", "tx_B_003"],
        "rotate_id":        ["rot_001", None, "rot_004"],
        "msisdn":           ["447700900001", "447700900004", "447700900005"],
        "transaction_type": ["SUB", "REN", "SUB"],
        "amount":           [None, 3.99, None],
        "currency":         ["GBP", "GBP", "GBP"],
        "created_at":       [
            "2026-01-15 09:00:00+00",
            "2026-01-15 10:30:00+00",
            "2026-01-15 11:30:00+00",
        ],
        "_loaded_date": [str(run_date)] * 3,
    })


def operator_c_df(run_date: date = RUN_DATE) -> pd.DataFrame:
    return pd.DataFrame({
        "message_id":      ["msg_001", "msg_002", "msg_003", "msg_004"],
        "tracking_code":   ["AB1", "CD2", "ABCD",  "EF3"],   # ABCD = too long
        "msisdn":          ["447700900006", "447700900007", "447700900008", "447700900009"],
        "delivery_status": ["DELIVERED", "DELIVERED", "DELIVERED", "FAILED"],
        "service_id":      ["svc_1", "svc_1", "svc_1", "svc_1"],
        "received_time":   [
            "2026-01-15 09:05:00+00",
            "2026-01-15 09:10:00+00",
            "2026-01-15 09:15:00+00",
            "2026-01-15 09:20:00+00",
        ],
        "_loaded_date": [str(run_date)] * 4,
    })


def tracking_codes_df() -> pd.DataFrame:
    """tracking_codes với 30-min windows."""
    return pd.DataFrame({
        "rotate_id":  ["rot_005", "rot_006", "rot_007"],
        "code":       ["AB1", "CD2", "EF3"],
        "service_id": ["svc_1", "svc_1", "svc_1"],
        "created_at": [
            "2026-01-15 09:00:00+00",
            "2026-01-15 09:00:00+00",
            "2026-01-15 09:00:00+00",
        ],
        "expired_at": [
            "2026-01-15 09:30:00+00",
            "2026-01-15 09:30:00+00",
            "2026-01-15 09:30:00+00",
        ],
    })


def clicks_df() -> pd.DataFrame:
    return pd.DataFrame({
        "rotate_id":   ["rot_001", "rot_002", "rot_003", "rot_004",
                        "rot_005", "rot_006", "rot_007"],
        "campaign_id": ["camp_A", "camp_A", "camp_B", "camp_B",
                        "camp_A", "camp_B", "camp_A"],
        "pub_id":      ["pub_1", "pub_1", "pub_2", "pub_2",
                        "pub_1", "pub_2", "pub_1"],
        "clicked_at":  ["2026-01-15 09:00:00+00"] * 7,
    })


def dim_campaigns_df() -> pd.DataFrame:
    return pd.DataFrame({
        "campaign_id":  ["camp_A", "camp_B"],
        "operator":     ["operator_A", "operator_B"],
        "service_name": ["Premium Service A", "Standard Service B"],
        "partner_id":   ["partner_001", "partner_002"],
        "country":      ["GB", "GB"],
        "status":       ["active", "active"],
    })


def page_events_df() -> pd.DataFrame:
    return pd.DataFrame({
        "event_id":   ["pe_001", "pe_002", "pe_003", "pe_004"],
        "rotate_id":  ["rot_001", "rot_001", "rot_002", "rot_003"],
        "campaign_id":["camp_A", "camp_A", "camp_A", "camp_B"],
        "msisdn":     ["447700900001", "447700900001", "447700900002", "447700900003"],
        "event_type": ["PAGE_VIEW", "CTA_CLICK", "PAGE_VIEW", "ENTRY"],
        "device_type":["mobile", "mobile", "mobile", "desktop"],
        "created_at": ["2026-01-15 09:01:00+00"] * 4,
    })
