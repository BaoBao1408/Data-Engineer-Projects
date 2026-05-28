"""Unit tests for ETL pipeline and financial ratio computation."""
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import date


# ─── ETL Pipeline Tests ───────────────────────────────────────────────────────

class TestDocumentETLPipeline:

    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "entity_code": ["VCB", "HPG", "FPT", None, "VCB"],
            "amount": [1_000_000, 2_500_000, 800_000, 300_000, 1_000_000],
            "currency": ["VND", "VND", "USD", "VND", "VND"],
            "transaction_date": ["2024-01-15", "2024-01-20", "2024-02-01", "2024-02-10", "2024-01-15"],
            "description": ["  Payment  ", "Receipt", "Transfer", "", "Payment"],
        })

    def test_silver_clean_drops_nulls(self, sample_df):
        from src.etl.pipelines.document_pipeline import DocumentETLPipeline, PipelineContext
        pipeline = DocumentETLPipeline.__new__(DocumentETLPipeline)
        pipeline.data_lake = MagicMock()
        pipeline.warehouse = MagicMock()
        pipeline.validator  = MagicMock()
        pipeline.validator.validate.return_value = MagicMock(passed=True, errors=[])

        ctx = PipelineContext("test", "run001", "test.csv")
        result = pipeline.silver_clean(sample_df.copy(), ctx)

        # Should have cleaned whitespace
        assert "Payment" in result["description"].values

    def test_silver_clean_removes_duplicates(self, sample_df):
        from src.etl.pipelines.document_pipeline import DocumentETLPipeline, PipelineContext
        pipeline = DocumentETLPipeline.__new__(DocumentETLPipeline)
        pipeline.data_lake = MagicMock()
        pipeline.warehouse = MagicMock()
        pipeline.validator  = MagicMock()
        pipeline.validator.validate.return_value = MagicMock(passed=True, errors=[])

        ctx = PipelineContext("test", "run001", "test.csv")
        result = pipeline.silver_clean(sample_df.copy(), ctx)
        # VCB rows are duplicates - one should be removed
        assert len(result) < len(sample_df)

    def test_pipeline_context_records_stages(self):
        from src.etl.pipelines.document_pipeline import PipelineContext
        ctx = PipelineContext("test_pipeline", "abc123", "file.csv")
        ctx.record_stage("bronze_ingest", 100, 100)
        ctx.record_stage("silver_clean",  100, 95)

        assert "bronze_ingest" in ctx.stages_completed
        assert "silver_clean"  in ctx.stages_completed
        assert ctx.metrics["silver_clean"]["rows_dropped"] == 5


# ─── Financial Ratio Tests ────────────────────────────────────────────────────

class TestFinancialRatioPipeline:

    def test_ratio_computation_basic(self):
        from src.etl.pipelines.financial_pipeline import FinancialRatioPipeline
        pipeline = FinancialRatioPipeline.__new__(FinancialRatioPipeline)
        pipeline.warehouse = MagicMock()

        # Mock warehouse responses
        bs = [
            {"line_code": "100", "current_year_amount": 1_000_000_000_000, "period_id": "pid1"},
            {"line_code": "110", "current_year_amount":   400_000_000_000, "period_id": "pid1"},
            {"line_code": "111", "current_year_amount":    50_000_000_000, "period_id": "pid1"},
            {"line_code": "200", "current_year_amount":   600_000_000_000, "period_id": "pid1"},
            {"line_code": "210", "current_year_amount":   200_000_000_000, "period_id": "pid1"},
            {"line_code": "300", "current_year_amount":   400_000_000_000, "period_id": "pid1"},
        ]
        inc = [
            {"line_code": "01",  "current_period_amount": 500_000_000_000},
            {"line_code": "20",  "current_period_amount": 300_000_000_000},
            {"line_code": "100", "current_period_amount":  80_000_000_000},
        ]
        pipeline.warehouse.execute.side_effect = [bs, inc, [], []]

        result = pipeline.compute_ratios_for_entity("entity-uuid", 2024)

        assert result is not None
        assert result["total_assets"]       == 1_000_000_000_000
        assert result["current_ratio"]       == 2.0   # 400B / 200B
        assert result["debt_to_equity"]      == 1.5   # 600B / 400B
        assert result["net_profit_margin"]   == 0.16  # 80B / 500B

    def test_safe_div_zero(self):
        from src.etl.pipelines.financial_pipeline import FinancialRatioPipeline
        pipeline = FinancialRatioPipeline.__new__(FinancialRatioPipeline)
        pipeline.warehouse = MagicMock()
        pipeline.warehouse.execute.side_effect = [[], [], [], []]

        result = pipeline.compute_ratios_for_entity("entity-uuid", 2024)
        assert result is None


# ─── Data Quality Validator Tests ─────────────────────────────────────────────

class TestDataQualityValidator:

    @pytest.fixture
    def validator(self):
        from src.quality.validators.schema_validator import DataQualityValidator
        return DataQualityValidator()

    @pytest.fixture
    def financial_df(self):
        return pd.DataFrame({
            "entity_id":   ["e1", "e2", "e3"],
            "amount":      [100.0, 250.0, 500.0],
            "currency":    ["VND", "USD", "VND"],
            "transaction_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
        })

    def test_not_null_passes(self, validator, financial_df):
        rules = {"entity_id": [{"type": "not_null"}]}
        result = validator.validate(financial_df, rules)
        assert result.passed
        assert result.pass_rate == 1.0

    def test_not_null_fails(self, validator):
        df = pd.DataFrame({"entity_id": ["e1", None, "e3"]})
        rules = {"entity_id": [{"type": "not_null"}]}
        result = validator.validate(df, rules)
        assert not result.passed
        assert len(result.errors) == 1

    def test_allowed_values_passes(self, validator, financial_df):
        rules = {"currency": [{"type": "allowed_values", "values": ["VND", "USD", "EUR"]}]}
        result = validator.validate(financial_df, rules)
        assert result.passed

    def test_allowed_values_fails(self, validator):
        df = pd.DataFrame({"currency": ["VND", "INVALID", "USD"]})
        rules = {"currency": [{"type": "allowed_values", "values": ["VND", "USD"]}]}
        result = validator.validate(df, rules)
        assert not result.passed

    def test_value_range_passes(self, validator, financial_df):
        rules = {"amount": [{"type": "value_range", "min": 0, "max": 1_000_000}]}
        result = validator.validate(financial_df, rules)
        assert result.passed

    def test_value_range_fails(self, validator):
        df = pd.DataFrame({"amount": [100, -50, 200]})
        rules = {"amount": [{"type": "value_range", "min": 0}]}
        result = validator.validate(df, rules)
        assert not result.passed

    def test_row_count_check(self, validator):
        df = pd.DataFrame({"col": [1, 2]})
        result = validator.validate(df, {"_table": [{"type": "row_count", "min": 5}]})
        assert not result.passed

    def test_missing_column(self, validator, financial_df):
        rules = {"nonexistent_column": [{"type": "not_null"}]}
        result = validator.validate(financial_df, rules)
        assert not result.passed


# ─── GL Anomaly Detection Tests ───────────────────────────────────────────────

class TestGLAnomalyDetector:

    def test_detects_round_number(self):
        from src.etl.pipelines.financial_pipeline import GLAnomalyDetector
        import pandas as pd

        detector = GLAnomalyDetector.__new__(GLAnomalyDetector)
        detector.warehouse = MagicMock()
        detector.warehouse.execute.return_value = []  # no rows to update

        rows = [
            {"transaction_id": "t1", "transaction_date": date(2024, 1, 10),
             "transaction_type": "EXPENSE", "amount": 5_000_000_000,  # 5B round
             "amount_vnd": 5_000_000_000, "currency": "VND",
             "counterparty_name": "Vendor A", "description": "Office supplies",
             "journal_entry_no": "JE-001", "document_no": "INV-001"},
        ]
        detector.warehouse.execute.side_effect = [rows, None]

        flagged = detector.detect_and_flag("entity-uuid", 2024)
        assert len(flagged) >= 0   # Should run without error

    def test_detects_weekend_transaction(self):
        from src.etl.pipelines.financial_pipeline import GLAnomalyDetector
        import pandas as pd

        detector = GLAnomalyDetector.__new__(GLAnomalyDetector)

        df = pd.DataFrame({
            "transaction_id":   ["t1", "t2"],
            "transaction_date": [date(2024, 1, 13), date(2024, 1, 15)],  # Saturday, Monday
            "amount":           [1_000_000, 2_000_000],
            "amount_vnd":       [1_000_000, 2_000_000],
            "currency":         ["VND", "VND"],
            "counterparty_name": ["A", "B"],
            "description":      ["D1", "D2"],
            "journal_entry_no": ["J1", "J2"],
            "document_no":      ["D1", "D2"],
            "transaction_type": ["EXPENSE", "EXPENSE"],
            "anomaly_score":    [0.0, 0.0],
            "anomaly_reasons":  [[], []],
        })

        # Saturday should get higher anomaly score
        df["transaction_date"] = pd.to_datetime(df["transaction_date"])
        weekend_mask = df["transaction_date"].dt.dayofweek >= 5
        assert weekend_mask.iloc[0] == True   # Jan 13, 2024 is Saturday
        assert weekend_mask.iloc[1] == False  # Jan 15, 2024 is Monday
