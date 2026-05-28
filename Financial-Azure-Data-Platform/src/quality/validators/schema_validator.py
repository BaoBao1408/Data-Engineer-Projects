"""
Data Quality Validator – rule-based validation engine.
Wraps Pandera for schema checks and Great Expectations for suite-level rules.
"""
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import pandas as pd
import pandera as pa
from loguru import logger


@dataclass
class ValidationResult:
    passed: bool
    total_checks: int
    passed_checks: int
    failed_checks: int
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    row_level_failures: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def pass_rate(self) -> float:
        if self.total_checks == 0:
            return 1.0
        return self.passed_checks / self.total_checks


class DataQualityValidator:
    """
    Validate DataFrames against configurable rule sets.

    Rule types:
        - not_null        : column must have no nulls
        - unique          : column must have unique values
        - value_range     : numeric column within [min, max]
        - regex           : string column matches pattern
        - allowed_values  : categorical column in allowed set
        - row_count       : DataFrame has at least N rows
        - custom          : arbitrary callable (df) → bool
    """

    def validate(
        self,
        df: pd.DataFrame,
        rules: dict[str, list[dict]],
        fail_fast: bool = False,
    ) -> ValidationResult:
        """
        Validate df against rules dict.

        rules format:
        {
            "column_name": [
                {"type": "not_null"},
                {"type": "value_range", "min": 0, "max": 1000},
                {"type": "regex", "pattern": r"^[A-Z]{3}$"},
            ],
            "_table": [
                {"type": "row_count", "min": 1},
            ]
        }
        """
        errors: list[str] = []
        warnings: list[str] = []
        total_checks = 0
        passed_checks = 0

        for target, rule_list in rules.items():
            for rule in rule_list:
                total_checks += 1
                rule_type = rule.get("type")

                try:
                    passed, msg = self._apply_rule(df, target, rule)
                    if passed:
                        passed_checks += 1
                    else:
                        errors.append(f"[{target}] {rule_type}: {msg}")
                        if fail_fast:
                            break
                except Exception as exc:
                    errors.append(f"[{target}] {rule_type} ERROR: {exc}")

        result = ValidationResult(
            passed=len(errors) == 0,
            total_checks=total_checks,
            passed_checks=passed_checks,
            failed_checks=total_checks - passed_checks,
            errors=errors,
            warnings=warnings,
        )

        if result.passed:
            logger.info(f"Data quality: ALL {total_checks} checks passed ✓")
        else:
            logger.warning(
                f"Data quality: {result.failed_checks}/{total_checks} checks FAILED"
            )
            for err in errors:
                logger.warning(f"  ✗ {err}")

        return result

    def _apply_rule(
        self, df: pd.DataFrame, target: str, rule: dict
    ) -> tuple[bool, str]:
        rule_type = rule["type"]

        # ── Table-level rules ──────────────────────────────────
        if target == "_table":
            if rule_type == "row_count":
                min_rows = rule.get("min", 1)
                actual = len(df)
                if actual < min_rows:
                    return False, f"Expected ≥{min_rows} rows, got {actual}"
                return True, ""

            if rule_type == "custom":
                fn: Callable = rule["fn"]
                passed = fn(df)
                return bool(passed), "" if passed else "Custom check failed"

        # ── Column-level rules ─────────────────────────────────
        if target not in df.columns:
            return False, f"Column '{target}' not found in DataFrame"

        col = df[target]

        if rule_type == "not_null":
            null_count = col.isna().sum()
            if null_count > 0:
                return False, f"{null_count} null values found"
            return True, ""

        if rule_type == "unique":
            dupe_count = col.duplicated().sum()
            if dupe_count > 0:
                return False, f"{dupe_count} duplicate values found"
            return True, ""

        if rule_type == "value_range":
            min_val = rule.get("min")
            max_val = rule.get("max")
            numeric_col = pd.to_numeric(col, errors="coerce")
            if min_val is not None:
                violations = (numeric_col < min_val).sum()
                if violations > 0:
                    return False, f"{violations} values below min={min_val}"
            if max_val is not None:
                violations = (numeric_col > max_val).sum()
                if violations > 0:
                    return False, f"{violations} values above max={max_val}"
            return True, ""

        if rule_type == "regex":
            pattern = rule["pattern"]
            str_col = col.dropna().astype(str)
            violations = (~str_col.str.match(pattern)).sum()
            if violations > 0:
                return False, f"{violations} values don't match pattern '{pattern}'"
            return True, ""

        if rule_type == "allowed_values":
            allowed = set(rule["values"])
            invalid = set(col.dropna().unique()) - allowed
            if invalid:
                return False, f"Invalid values found: {invalid}"
            return True, ""

        if rule_type == "not_empty_string":
            empty = (col.astype(str).str.strip() == "").sum()
            if empty > 0:
                return False, f"{empty} empty string values"
            return True, ""

        if rule_type == "dtype":
            expected = rule["dtype"]
            if str(col.dtype) != expected:
                return False, f"Expected dtype={expected}, got {col.dtype}"
            return True, ""

        return False, f"Unknown rule type: {rule_type}"


# ─── Pre-built Rule Suites ────────────────────────────────────────────────────

FINANCIAL_DATA_RULES = {
    "_table": [{"type": "row_count", "min": 1}],
    "amount": [
        {"type": "not_null"},
        {"type": "value_range", "min": 0},
    ],
    "currency": [
        {"type": "not_null"},
        {"type": "allowed_values", "values": ["USD", "VND", "EUR", "SGD", "GBP"]},
        {"type": "regex", "pattern": r"^[A-Z]{3}$"},
    ],
    "transaction_date": [
        {"type": "not_null"},
    ],
    "entity_id": [
        {"type": "not_null"},
        {"type": "unique"},
    ],
}

DOCUMENT_METADATA_RULES = {
    "_table": [{"type": "row_count", "min": 1}],
    "file_name": [{"type": "not_null"}, {"type": "unique"}],
    "source_type": [
        {"type": "not_null"},
        {"type": "allowed_values", "values": ["pdf", "xlsx", "docx", "csv", "api"]},
    ],
    "checksum_md5": [{"type": "not_null"}, {"type": "unique"}],
}
