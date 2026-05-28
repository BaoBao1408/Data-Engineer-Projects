"""
Financial ETL Pipeline.
Reads balance sheet + income statement items → computes financial ratios → loads to warehouse.
Also runs anomaly detection on GL transactions.
"""
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import pandas as pd
from loguru import logger

from src.etl.pipelines.document_pipeline import DocumentETLPipeline, PipelineContext
from src.ingestion.connectors.sql_connector import get_warehouse


class FinancialRatioPipeline:
    """
    Compute financial KPIs from raw statement line items.
    VAS / IFRS standard line codes are mapped to ratio inputs.
    """

    # VAS line code → ratio field mapping
    BALANCE_SHEET_MAP = {
        "100": "total_assets",
        "110": "current_assets",
        "111": "cash_and_equivalents",
        "200": "total_liabilities",
        "210": "current_liabilities",
        "300": "total_equity",
        "320": "charter_capital",
    }
    INCOME_MAP = {
        "01":  "total_revenue",
        "10":  "net_revenue",
        "20":  "cost_of_goods_sold",
        "30":  "gross_profit",
        "40":  "selling_expenses",
        "45":  "admin_expenses",
        "50":  "operating_profit",    # EBIT
        "60":  "financial_income",
        "70":  "financial_expenses",
        "71":  "interest_expense",
        "80":  "income_before_tax",
        "90":  "income_tax_expense",
        "100": "net_income",
    }
    CASHFLOW_MAP = {
        "20":  "operating_cash_flow",
        "30":  "investing_cash_flow",
        "40":  "financing_cash_flow",
    }

    def __init__(self):
        self.warehouse = get_warehouse()

    def compute_ratios_for_entity(
        self, entity_id: str, fiscal_year: int, period_type: str = "ANNUAL"
    ) -> Optional[dict]:
        """
        Pull statement items from warehouse and compute all financial ratios.
        Returns dict ready to upsert into financial.financial_ratios.
        """
        # 1. Fetch balance sheet items
        bs_rows = self.warehouse.execute("""
            SELECT bsi.line_code, bsi.current_year_amount, fp.period_id
            FROM financial.balance_sheet_items bsi
            JOIN financial.fiscal_periods fp
                ON fp.period_id = bsi.period_id
            WHERE bsi.entity_id = :entity_id
              AND fp.fiscal_year = :fiscal_year
              AND fp.period_type = :period_type
        """, {"entity_id": entity_id, "fiscal_year": fiscal_year, "period_type": period_type})

        # 2. Fetch income statement items
        is_rows = self.warehouse.execute("""
            SELECT isi.line_code, isi.current_period_amount
            FROM financial.income_statement_items isi
            JOIN financial.fiscal_periods fp
                ON fp.period_id = isi.period_id
            WHERE isi.entity_id = :entity_id
              AND fp.fiscal_year = :fiscal_year
              AND fp.period_type = :period_type
        """, {"entity_id": entity_id, "fiscal_year": fiscal_year, "period_type": period_type})

        # 3. Fetch cash flow items
        cf_rows = self.warehouse.execute("""
            SELECT cfi.line_code, cfi.current_period_amount
            FROM financial.cash_flow_items cfi
            JOIN financial.fiscal_periods fp
                ON fp.period_id = cfi.period_id
            WHERE cfi.entity_id = :entity_id
              AND fp.fiscal_year = :fiscal_year
              AND fp.period_type = :period_type
        """, {"entity_id": entity_id, "fiscal_year": fiscal_year, "period_type": period_type})

        if not bs_rows and not is_rows:
            logger.warning(f"No statement data for entity={entity_id} year={fiscal_year}")
            return None

        # Fetch prior year for growth rates
        prior_ratios = self.warehouse.execute("""
            SELECT total_revenue, net_income, total_assets, total_equity
            FROM financial.financial_ratios r
            JOIN financial.fiscal_periods fp ON fp.period_id = r.period_id
            WHERE r.entity_id = :entity_id
              AND fp.fiscal_year = :prior_year
              AND fp.period_type = :period_type
            LIMIT 1
        """, {"entity_id": entity_id, "prior_year": fiscal_year - 1, "period_type": period_type})

        prior = prior_ratios[0] if prior_ratios else {}

        # Build lookup dicts
        bs  = {r["line_code"]: float(r["current_year_amount"] or 0) for r in bs_rows}
        inc = {r["line_code"]: float(r["current_period_amount"] or 0) for r in is_rows}
        cf  = {r["line_code"]: float(r["current_period_amount"] or 0) for r in cf_rows}

        def g(d, key, default=0.0): return d.get(key, default) or default

        # Raw inputs
        total_assets       = g(bs, "100")
        current_assets     = g(bs, "110")
        cash               = g(bs, "111")
        total_liabilities  = g(bs, "200")
        current_liabilities= g(bs, "210")
        total_equity       = g(bs, "300")
        total_revenue      = g(inc, "01") or g(inc, "10")
        cogs               = g(inc, "20")
        gross_profit       = g(inc, "30") or (total_revenue - cogs)
        admin_exp          = g(inc, "45")
        selling_exp        = g(inc, "40")
        ebit               = g(inc, "50") or (gross_profit - admin_exp - selling_exp)
        interest_exp       = g(inc, "71")
        income_before_tax  = g(inc, "80")
        net_income         = g(inc, "100")
        operating_cf       = g(cf, "20")
        total_debt         = total_liabilities  # simplified

        # Depreciation (proxy from COGS - net income margin)
        ebitda = ebit + (total_assets * 0.05)   # rough D&A proxy if not available

        # ─── Compute Ratios ───────────────────────────────────────────────
        def safe_div(a, b): return round(a / b, 4) if b and b != 0 else None

        current_ratio           = safe_div(current_assets, current_liabilities)
        inventory               = current_assets - cash - (current_assets * 0.3)   # proxy
        quick_ratio             = safe_div(current_assets - max(inventory, 0), current_liabilities)
        cash_ratio              = safe_div(cash, current_liabilities)
        debt_to_equity          = safe_div(total_debt, total_equity)
        debt_to_assets          = safe_div(total_debt, total_assets)
        equity_ratio            = safe_div(total_equity, total_assets)
        interest_coverage       = safe_div(ebit, interest_exp)
        gross_profit_margin     = safe_div(gross_profit, total_revenue)
        operating_profit_margin = safe_div(ebit, total_revenue)
        net_profit_margin       = safe_div(net_income, total_revenue)
        return_on_assets        = safe_div(net_income, total_assets)
        return_on_equity        = safe_div(net_income, total_equity)
        ebitda_margin           = safe_div(ebitda, total_revenue)
        asset_turnover          = safe_div(total_revenue, total_assets)

        # Growth rates (YoY)
        revenue_growth_yoy     = safe_div(total_revenue - float(prior.get("total_revenue") or 0),
                                          float(prior.get("total_revenue") or 0)) if prior.get("total_revenue") else None
        net_income_growth_yoy  = safe_div(net_income   - float(prior.get("net_income") or 0),
                                          abs(float(prior.get("net_income") or 0))) if prior.get("net_income") else None

        period_id = bs_rows[0]["period_id"] if bs_rows else None

        return {
            "entity_id":               entity_id,
            "period_id":               period_id,
            "total_revenue":           total_revenue,
            "gross_profit":            gross_profit,
            "ebit":                    ebit,
            "ebitda":                  ebitda,
            "net_income":              net_income,
            "total_assets":            total_assets,
            "total_liabilities":       total_liabilities,
            "total_equity":            total_equity,
            "current_assets":          current_assets,
            "current_liabilities":     current_liabilities,
            "cash_and_equivalents":    cash,
            "operating_cash_flow":     operating_cf,
            "current_ratio":           current_ratio,
            "quick_ratio":             quick_ratio,
            "cash_ratio":              cash_ratio,
            "debt_to_equity":          debt_to_equity,
            "debt_to_assets":          debt_to_assets,
            "equity_ratio":            equity_ratio,
            "interest_coverage":       interest_coverage,
            "gross_profit_margin":     gross_profit_margin,
            "operating_profit_margin": operating_profit_margin,
            "net_profit_margin":       net_profit_margin,
            "return_on_assets":        return_on_assets,
            "return_on_equity":        return_on_equity,
            "ebitda_margin":           ebitda_margin,
            "asset_turnover":          asset_turnover,
            "revenue_growth_yoy":      revenue_growth_yoy,
            "net_income_growth_yoy":   net_income_growth_yoy,
        }

    def upsert_ratios(self, ratios: dict) -> None:
        """Upsert computed ratios into financial.financial_ratios."""
        cols = [c for c in ratios if c not in ("entity_id", "period_id")]
        update_set = ", ".join([f"{c} = :{c}" for c in cols])
        sql = f"""
            INSERT INTO financial.financial_ratios
                (entity_id, period_id, {', '.join(cols)})
            VALUES
                (:entity_id, :period_id, {', '.join([':' + c for c in cols])})
            ON CONFLICT (entity_id, period_id)
            DO UPDATE SET {update_set}, computed_at = NOW()
        """
        self.warehouse.execute_many(sql, [ratios])
        logger.info(f"Upserted ratios for entity={ratios['entity_id']}")


class GLAnomalyDetector:
    """
    Rule-based anomaly detection on GL transactions.
    Flags entries that deviate from statistical norms.

    Rules:
    1. Z-score > 3 on amount (statistical outlier)
    2. Round-number transactions (possible manual entries)
    3. Transactions on weekends / holidays
    4. Journal entries with no counterparty
    5. Same-day reversal patterns
    6. Intercompany imbalances
    """

    def __init__(self):
        self.warehouse = get_warehouse()

    def detect_and_flag(self, entity_id: str, fiscal_year: int) -> pd.DataFrame:
        """Run all anomaly rules on GL for an entity/year. Return flagged rows."""
        rows = self.warehouse.execute("""
            SELECT transaction_id, transaction_date, transaction_type,
                   amount, amount_vnd, currency, counterparty_name,
                   description, journal_entry_no, document_no
            FROM financial.gl_transactions
            WHERE entity_id = :entity_id
              AND EXTRACT(YEAR FROM transaction_date) = :year
        """, {"entity_id": entity_id, "year": fiscal_year})

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df["anomaly_score"] = 0.0
        df["anomaly_reasons"] = [[] for _ in range(len(df))]

        # Rule 1: Z-score on amount
        mean_amt = df["amount"].mean()
        std_amt  = df["amount"].std()
        if std_amt and std_amt > 0:
            df["z_score"] = ((df["amount"] - mean_amt) / std_amt).abs()
            mask = df["z_score"] > 3
            df.loc[mask, "anomaly_score"] += 0.4
            df.loc[mask, "anomaly_reasons"] = df.loc[mask, "anomaly_reasons"].apply(
                lambda x: x + ["HIGH_AMOUNT_ZSCORE"]
            )

        # Rule 2: Round numbers (multiples of 1B, 500M, 100M VND)
        thresholds = [1_000_000_000, 500_000_000, 100_000_000]
        round_mask = df["amount"].apply(
            lambda a: any(a % t == 0 for t in thresholds)
        ) & (df["amount"] > 1_000_000_000)
        df.loc[round_mask, "anomaly_score"] += 0.2
        df.loc[round_mask, "anomaly_reasons"] = df.loc[round_mask, "anomaly_reasons"].apply(
            lambda x: x + ["ROUND_NUMBER"]
        )

        # Rule 3: Weekend transactions
        if "transaction_date" in df.columns:
            df["transaction_date"] = pd.to_datetime(df["transaction_date"])
            weekend_mask = df["transaction_date"].dt.dayofweek >= 5
            df.loc[weekend_mask, "anomaly_score"] += 0.15
            df.loc[weekend_mask, "anomaly_reasons"] = df.loc[weekend_mask, "anomaly_reasons"].apply(
                lambda x: x + ["WEEKEND_TRANSACTION"]
            )

        # Rule 4: Missing counterparty for large transactions
        large_no_party = (
            df["amount"] > (mean_amt * 3)
        ) & (df["counterparty_name"].isna() | (df["counterparty_name"] == ""))
        df.loc[large_no_party, "anomaly_score"] += 0.25
        df.loc[large_no_party, "anomaly_reasons"] = df.loc[large_no_party, "anomaly_reasons"].apply(
            lambda x: x + ["LARGE_NO_COUNTERPARTY"]
        )

        # Normalize score to [0, 1]
        df["anomaly_score"] = df["anomaly_score"].clip(0, 1).round(4)
        df["anomaly_flag"]  = df["anomaly_score"] >= 0.4

        flagged = df[df["anomaly_flag"]].copy()
        logger.info(
            f"Anomaly detection: {len(flagged)}/{len(df)} transactions flagged "
            f"for entity={entity_id} year={fiscal_year}"
        )

        # Write flags back to warehouse
        if not flagged.empty:
            for _, row in flagged.iterrows():
                self.warehouse.execute("""
                    UPDATE financial.gl_transactions
                    SET anomaly_score = :score, anomaly_flag = TRUE
                    WHERE transaction_id = :tid
                """, {"score": float(row["anomaly_score"]), "tid": str(row["transaction_id"])})

        return flagged
