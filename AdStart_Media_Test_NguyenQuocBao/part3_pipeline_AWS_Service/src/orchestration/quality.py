"""
src/orchestration/quality.py — Post-build quality assertions.

LOCAL  : query DuckDB → check assertions trực tiếp trên DataFrame
AWS    : query Athena → check assertions, gửi SNS alert nếu fail

Checks được run sau mỗi layer build:
  Layer 0 (Ingest)   : row counts, null rates (trong validator.py)
  Layer 1 (Facts)    : duplicate primary keys, operator_C attribution rate
  Layer 2 (Mart)     : mart has rows, no negative revenue, conversion rate sanity
  Cross-layer        : subscriptions ≤ clicks (basic funnel sanity)

AWS SNS notification:
    Khi check fail → publish message tới SNS topic → trigger Lambda/email/PagerDuty
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import pandas as pd

from config.base import settings
from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


@dataclass
class QualityResult:
    check_name:   str
    passed:       bool
    failing_rows: int = 0
    details:      str = ""
    layer:        str = ""


@dataclass
class QualitySuite:
    run_date:  date
    results:   list[QualityResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failures(self) -> list[QualityResult]:
        return [r for r in self.results if not r.passed]

    def summary(self) -> str:
        total  = len(self.results)
        failed = len(self.failures)
        lines  = [f"Quality Suite {self.run_date}: {total-failed}/{total} checks passed"]
        for r in self.failures:
            lines.append(f"  ✗ [{r.layer}] {r.check_name}: {r.failing_rows} failing rows — {r.details}")
        return "\n".join(lines)


# ── Individual checks ─────────────────────────────────────────────

def _check_row_count(
    wh: AWSWarehouse, table: str, layer: str, run_date: date, min_rows: int = 1
) -> QualityResult:
    """Table phải có ít nhất min_rows rows cho ngày run_date."""
    try:
        df = wh.query(f"SELECT COUNT(*) AS n FROM {table} WHERE report_date='{run_date}'", layer=layer)
        n  = int(df["n"].iloc[0])
        passed = n >= min_rows
        return QualityResult(
            check_name=f"{table}_has_rows",
            passed=passed,
            failing_rows=0 if passed else 1,
            details=f"Found {n} rows (min={min_rows})",
            layer=layer,
        )
    except Exception as e:
        return QualityResult(check_name=f"{table}_has_rows", passed=False,
                             failing_rows=-1, details=str(e), layer=layer)


def _check_no_duplicates(
    wh: AWSWarehouse, table: str, layer: str, pk_col: str, run_date: date
) -> QualityResult:
    """Primary key phải unique trong partition run_date."""
    try:
        df = wh.query(f"""
            SELECT {pk_col}, COUNT(*) AS cnt
            FROM {table}
            WHERE report_date = '{run_date}'
            GROUP BY {pk_col}
            HAVING COUNT(*) > 1
        """, layer=layer)
        n = len(df)
        return QualityResult(
            check_name=f"{table}_no_duplicate_{pk_col}",
            passed=n == 0,
            failing_rows=n,
            details=f"{n} duplicate {pk_col} values" if n > 0 else "OK",
            layer=layer,
        )
    except Exception as e:
        return QualityResult(check_name=f"{table}_no_duplicate_{pk_col}",
                             passed=False, failing_rows=-1, details=str(e), layer=layer)


def _check_no_negative_revenue(wh: AWSWarehouse, run_date: date) -> QualityResult:
    try:
        df = wh.query(f"""
            SELECT COUNT(*) AS n FROM mart_daily_performance
            WHERE report_date='{run_date}' AND total_revenue < 0
        """, layer="mart")
        n = int(df["n"].iloc[0])
        return QualityResult(
            check_name="mart_no_negative_revenue",
            passed=n == 0,
            failing_rows=n,
            details=f"{n} rows với negative revenue" if n > 0 else "OK",
            layer="mart",
        )
    except Exception as e:
        return QualityResult(check_name="mart_no_negative_revenue",
                             passed=False, failing_rows=-1, details=str(e), layer="mart")


def _check_conversion_rates(wh: AWSWarehouse, run_date: date) -> QualityResult:
    try:
        df = wh.query(f"""
            SELECT COUNT(*) AS n FROM mart_daily_performance
            WHERE report_date='{run_date}'
              AND (sub_conversion_rate > 1 OR bill_conversion_rate > 1)
        """, layer="mart")
        n = int(df["n"].iloc[0])
        return QualityResult(
            check_name="mart_conversion_rate_valid",
            passed=n == 0,
            failing_rows=n,
            details=f"{n} rows với conversion rate > 100%" if n > 0 else "OK",
            layer="mart",
        )
    except Exception as e:
        return QualityResult(check_name="mart_conversion_rate_valid",
                             passed=False, failing_rows=-1, details=str(e), layer="mart")


def _check_subs_not_exceed_clicks(wh: AWSWarehouse, run_date: date) -> QualityResult:
    """total_subscriptions phải ≤ total_clicks (basic funnel sanity)."""
    try:
        df = wh.query(f"""
            SELECT COUNT(*) AS n FROM mart_daily_performance
            WHERE report_date='{run_date}'
              AND total_subscriptions > total_clicks
        """, layer="mart")
        n = int(df["n"].iloc[0])
        return QualityResult(
            check_name="mart_subs_not_exceed_clicks",
            passed=n == 0,
            failing_rows=n,
            details=f"{n} campaigns có subs > clicks" if n > 0 else "OK",
            layer="mart",
        )
    except Exception as e:
        return QualityResult(check_name="mart_subs_not_exceed_clicks",
                             passed=False, failing_rows=-1, details=str(e), layer="mart")


def _check_attribution_rate(wh: AWSWarehouse, run_date: date, threshold: float = 0.80) -> QualityResult:
    """
    operator_C attribution rate phải >= threshold (default 80%).
    Baseline: ~87% attributed. Alert nếu drop dưới 80% → có thể có SMS parser regression.
    """
    try:
        df_total = wh.query(f"""
            SELECT COUNT(*) AS n FROM raw_operator_c
            WHERE delivery_status='DELIVERED' AND _loaded_date='{run_date}'
        """, layer="raw")
        total = int(df_total["n"].iloc[0])

        if total == 0:
            return QualityResult(check_name="operator_c_attribution_rate",
                                 passed=True, details="No data for date", layer="facts")

        df_unattr = wh.query(f"""
            SELECT COUNT(*) AS n FROM fct_unattributed_events
            WHERE operator='operator_C' AND report_date='{run_date}'
        """, layer="facts")
        unattr = int(df_unattr["n"].iloc[0]) if not df_unattr.empty else 0

        rate   = (total - unattr) / total
        passed = rate >= threshold
        return QualityResult(
            check_name="operator_c_attribution_rate",
            passed=passed,
            failing_rows=0 if passed else 1,
            details=f"Attribution rate: {rate:.1%} (threshold {threshold:.0%})",
            layer="facts",
        )
    except Exception as e:
        return QualityResult(check_name="operator_c_attribution_rate",
                             passed=False, failing_rows=-1, details=str(e), layer="facts")


# ── SNS notification ──────────────────────────────────────────────

def _send_sns_alert(suite: QualitySuite) -> None:
    """Gửi SNS notification khi quality check fail (chỉ AWS mode)."""
    if not settings.is_aws or not settings.sns_alert_topic_arn:
        return
    try:
        import boto3
        sns = boto3.client("sns", region_name=settings.aws_region)
        message = {
            "pipeline":   "adstart-data-pipeline",
            "run_date":   str(suite.run_date),
            "status":     "QUALITY_FAILURE",
            "failures":   [
                {"check": r.check_name, "failing_rows": r.failing_rows, "details": r.details}
                for r in suite.failures
            ],
        }
        sns.publish(
            TopicArn=settings.sns_alert_topic_arn,
            Subject=f"[Pipeline Alert] Quality checks failed for {suite.run_date}",
            Message=json.dumps(message, indent=2),
        )
        logger.info(f"SNS alert sent to {settings.sns_alert_topic_arn}")
    except Exception as e:
        logger.error(f"Không gửi được SNS alert: {e}")


# ── Main quality runner ───────────────────────────────────────────

def run_quality_checks(wh: AWSWarehouse, run_date: date) -> QualitySuite:
    """
    Chạy toàn bộ quality checks và trả về QualitySuite.
    Pipeline sẽ raise ValueError nếu có critical check fail.

    Checks:
      Facts layer  : row counts, duplicates, attribution rate
      Mart layer   : row counts, revenue sanity, conversion rates, funnel sanity
    """
    suite = QualitySuite(run_date=run_date)

    logger.info(f"[quality] Running checks for {run_date} ...")

    # ── Facts layer ───────────────────────────────────────────────
    suite.results.append(_check_row_count(wh, "fct_subscriptions", "facts", run_date))
    suite.results.append(_check_no_duplicates(wh, "fct_subscriptions", "facts", "source_transaction_id", run_date))
    suite.results.append(_check_no_duplicates(wh, "fct_billing", "facts", "source_transaction_id", run_date))
    suite.results.append(_check_attribution_rate(wh, run_date))

    # ── Mart layer ────────────────────────────────────────────────
    suite.results.append(_check_row_count(wh, "mart_daily_performance", "mart", run_date))
    suite.results.append(_check_no_negative_revenue(wh, run_date))
    suite.results.append(_check_conversion_rates(wh, run_date))
    suite.results.append(_check_subs_not_exceed_clicks(wh, run_date))

    # ── Log results ───────────────────────────────────────────────
    summary = suite.summary()
    if suite.passed:
        logger.info(summary)
    else:
        logger.error(summary)
        _send_sns_alert(suite)

    return suite
