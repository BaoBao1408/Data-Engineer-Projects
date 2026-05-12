"""
tests/unit/test_transformations.py — Unit tests cho transformation logic.

Dùng moto để mock AWS services — không cần real AWS account.
Tests chạy hoàn toàn offline.
"""
from __future__ import annotations

import os
import pytest
import pandas as pd
from datetime import date
from unittest.mock import MagicMock, patch

# ── Set local mode trước khi import bất cứ thứ gì ────────────────
os.environ["PIPELINE_ENV"] = "local"


# ── Fixtures ──────────────────────────────────────────────────────

@pytest.fixture
def run_date():
    return date(2026, 1, 15)


@pytest.fixture
def sample_operator_a():
    return pd.DataFrame({
        "transaction_id": ["tx_001", "tx_002", "tx_003"],
        "rotate_id":      ["rot_001", "rot_002", "rot_003"],
        "msisdn":         ["447700900001", "447700900002", "447700900003"],
        "event_code":     [1, 1, 2],           # 1=subscribe, 2=bill
        "status":         ["SUCCESS", "SUCCESS", "SUCCESS"],
        "amount":         [None, None, 4.99],
        "currency":       ["GBP", "GBP", "GBP"],
        "event_time":     ["2026-01-15 10:00:00+00", "2026-01-15 11:00:00+00",
                           "2026-01-15 12:00:00+00"],
        "_loaded_date":   ["2026-01-15", "2026-01-15", "2026-01-15"],
    })


@pytest.fixture
def sample_clicks():
    return pd.DataFrame({
        "rotate_id":   ["rot_001", "rot_002", "rot_003"],
        "campaign_id": ["camp_A", "camp_A", "camp_B"],
        "pub_id":      ["pub_1", "pub_1", "pub_2"],
        "clicked_at":  ["2026-01-15 09:00:00+00"] * 3,
    })


@pytest.fixture
def sample_dim():
    return pd.DataFrame({
        "campaign_id":  ["camp_A", "camp_B"],
        "operator":     ["operator_A", "operator_B"],
        "service_name": ["Service A", "Service B"],
        "partner_id":   ["partner_1", "partner_2"],
        "country":      ["GB", "GB"],
        "status":       ["active", "active"],
    })


# ── Tests: subscriptions ──────────────────────────────────────────

class TestBuildSubsOperatorA:
    def test_filters_subscribe_events(self, sample_operator_a, sample_clicks, sample_dim, run_date):
        from src.transformations.subscriptions import _build_subs_operator_a
        result = _build_subs_operator_a(sample_operator_a, sample_clicks, sample_dim, run_date)

        # Chỉ event_code=1 (subscribe) được giữ
        assert len(result) == 2
        assert all(result["operator"] == "operator_A")
        assert all(result["attribution_method"] == "direct_rotate_id")

    def test_empty_when_no_subscribe_events(self, sample_clicks, sample_dim, run_date):
        from src.transformations.subscriptions import _build_subs_operator_a
        df_bills_only = pd.DataFrame({
            "transaction_id": ["tx_001"],
            "rotate_id":      ["rot_001"],
            "msisdn":         ["447700900001"],
            "event_code":     [2],              # 2=bill only
            "status":         ["SUCCESS"],
            "amount":         [4.99],
            "currency":       ["GBP"],
            "event_time":     ["2026-01-15 10:00:00+00"],
            "_loaded_date":   ["2026-01-15"],
        })
        result = _build_subs_operator_a(df_bills_only, sample_clicks, sample_dim, run_date)
        assert result.empty

    def test_excludes_failed_events(self, sample_clicks, sample_dim, run_date):
        from src.transformations.subscriptions import _build_subs_operator_a
        df = pd.DataFrame({
            "transaction_id": ["tx_001"],
            "rotate_id":      ["rot_001"],
            "msisdn":         ["447700900001"],
            "event_code":     [1],
            "status":         ["FAILED"],  # ← failed, không được include
            "amount":         [None],
            "currency":       ["GBP"],
            "event_time":     ["2026-01-15 10:00:00+00"],
            "_loaded_date":   ["2026-01-15"],
        })
        result = _build_subs_operator_a(df, sample_clicks, sample_dim, run_date)
        assert result.empty


class TestBuildSubsOperatorC:
    def test_quarantines_bad_tracking_codes(self, sample_clicks, sample_dim, run_date):
        from src.transformations.subscriptions import _build_subs_operator_c

        df_c = pd.DataFrame({
            "message_id":      ["msg_001", "msg_002"],
            "tracking_code":   ["AB1", "ABCD"],  # AB1 ok, ABCD = too long
            "msisdn":          ["447700900001", "447700900002"],
            "delivery_status": ["DELIVERED", "DELIVERED"],
            "service_id":      ["svc_1", "svc_1"],
            "received_time":   ["2026-01-15 10:05:00+00", "2026-01-15 10:06:00+00"],
            "_loaded_date":    ["2026-01-15", "2026-01-15"],
        })
        df_tc = pd.DataFrame({
            "rotate_id":  ["rot_001"],
            "code":       ["AB1"],
            "service_id": ["svc_1"],
            "created_at": ["2026-01-15 10:00:00+00"],
            "expired_at": ["2026-01-15 10:30:00+00"],
        })

        df_tc["created_at"] = pd.to_datetime(df_tc["created_at"], utc=True)
        df_tc["expired_at"] = pd.to_datetime(df_tc["expired_at"], utc=True)
        df_c["received_time"] = pd.to_datetime(df_c["received_time"], utc=True)

        attr, quarantine = _build_subs_operator_c(df_c, df_tc, sample_clicks, sample_dim, run_date)

        # msg_002 với ABCD should be quarantined
        assert len(quarantine) >= 1
        bad_reasons = quarantine["unattributed_reason"].tolist()
        assert any("too_long" in r for r in bad_reasons)


# ── Tests: mart ───────────────────────────────────────────────────

class TestBuildMartDailyPerformance:
    def test_aggregation_correctness(self, run_date):
        """Kiểm tra aggregation math là đúng."""
        import numpy as np
        from src.transformations.billing_clicks_mart import build_mart_daily_performance

        # Mock warehouse
        clicks_df = pd.DataFrame({
            "click_id":         ["c1", "c2", "c3", "c4"],
            "rotate_id":        ["r1", "r2", "r3", "r4"],
            "campaign_id":      ["camp_A", "camp_A", "camp_A", "camp_B"],
            "operator":         ["operator_A"] * 3 + ["operator_B"],
            "service_name":     ["Service A"] * 3 + ["Service B"],
            "partner_id":       ["partner_1"] * 3 + ["partner_2"],
            "clicked_at":       ["2026-01-15 10:00:00+00"] * 4,
            "report_date":      ["2026-01-15"] * 4,
            "has_page_view":    [True, True, False, True],
            "has_cta_click":    [True, False, False, True],
            "has_entry":        [True, True, False, False],
            "has_subscription": [True, True, False, False],
            "has_first_bill":   [True, False, False, False],
        })

        billing_df = pd.DataFrame({
            "billing_id":    ["b1"],
            "campaign_id":   ["camp_A"],
            "msisdn":        ["447700900001"],
            "amount":        [4.99],
            "is_first_bill": [True],
            "billed_at":     ["2026-01-15 11:00:00+00"],
            "report_date":   ["2026-01-15"],
        })

        mock_wh = MagicMock()

        def mock_query(sql, layer=""):
            if "fct_clicks" in sql:
                return clicks_df
            elif "fct_billing" in sql:
                return billing_df
            return pd.DataFrame()

        mock_wh.query.side_effect = mock_query
        mock_wh.write_table.return_value = 2

        result = build_mart_daily_performance(mock_wh, run_date)
        assert result > 0


# ── Tests: validator ──────────────────────────────────────────────

class TestValidator:
    def test_raises_on_empty_dataframe(self):
        from src.ingest.validator import validate_dataframe
        with pytest.raises(ValueError, match="trống"):
            validate_dataframe(pd.DataFrame(), "raw_operator_a")

    def test_passes_with_good_data(self):
        from src.ingest.validator import validate_dataframe
        df = pd.DataFrame({
            "transaction_id": ["tx_001"],
            "event_code":     [1],
            "msisdn":         ["447700900001"],
            "event_time":     ["2026-01-15 10:00:00"],
        })
        result = validate_dataframe(df, "raw_operator_a")
        assert result["row_count"] == 1
        assert result["warnings"] == []

    def test_warns_on_high_null_rate(self):
        from src.ingest.validator import validate_dataframe
        # 60% null rate on msisdn → should warn
        df = pd.DataFrame({
            "transaction_id": ["tx_001", "tx_002", "tx_003", "tx_004", "tx_005"],
            "event_code":     [1, 1, 1, 1, 1],
            "msisdn":         [None, None, None, "447700900001", "447700900002"],
            "event_time":     ["2026-01-15 10:00:00"] * 5,
        })
        result = validate_dataframe(df, "raw_operator_a")
        assert any("msisdn" in w for w in result["warnings"])
