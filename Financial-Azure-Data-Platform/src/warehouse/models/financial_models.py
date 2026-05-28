"""
SQLAlchemy ORM Models – Financial Domain.
Mirrors the DDL in 001_financial_schema.sql.
"""
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    Boolean, CheckConstraint, Column, Date, DateTime,
    ForeignKey, Integer, Numeric, SmallInteger, String,
    Text, UniqueConstraint, func,
)
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def _uuid():
    return str(uuid.uuid4())


# ─────────────────────────────────────────────────────────────────────────────
# REFERENCE TABLES
# ─────────────────────────────────────────────────────────────────────────────

class Currency(Base):
    __tablename__ = "currencies"
    __table_args__ = {"schema": "financial"}

    code            = Column(String(3),   primary_key=True)
    name            = Column(String(100), nullable=False)
    symbol          = Column(String(10))
    decimal_places  = Column(SmallInteger, default=2)
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())


class IndustryCode(Base):
    __tablename__ = "industry_codes"
    __table_args__ = {"schema": "financial"}

    code        = Column(String(20),  primary_key=True)
    name_en     = Column(String(200), nullable=False)
    name_vi     = Column(String(200))
    category    = Column(String(100))
    created_at  = Column(DateTime(timezone=True), server_default=func.now())


# ─────────────────────────────────────────────────────────────────────────────
# ENTITIES
# ─────────────────────────────────────────────────────────────────────────────

class Entity(Base):
    __tablename__ = "entities"
    __table_args__ = {"schema": "financial"}

    entity_id           = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_code         = Column(String(50),  unique=True, nullable=False)
    legal_name          = Column(String(500), nullable=False)
    short_name          = Column(String(200))
    tax_id              = Column(String(20),  unique=True)
    registration_no     = Column(String(50))
    entity_type         = Column(String(50),  nullable=False)
    industry_code       = Column(String(20),  ForeignKey("financial.industry_codes.code"))
    functional_currency = Column(String(3),   ForeignKey("financial.currencies.code"), default="VND")
    reporting_standard  = Column(String(20),  default="VAS")
    country             = Column(String(2),   default="VN")
    province            = Column(String(100))
    address             = Column(Text)
    stock_exchange      = Column(String(20))
    ticker_symbol       = Column(String(20))
    listing_date        = Column(Date)
    parent_entity_id    = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"))
    is_active           = Column(Boolean, default=True)
    incorporation_date  = Column(Date)
    notes               = Column(Text)
    created_at          = Column(DateTime(timezone=True), server_default=func.now())
    updated_at          = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    fiscal_periods      = relationship("FiscalPeriod",        back_populates="entity")
    financial_statements = relationship("FinancialStatement", back_populates="entity")
    gl_transactions     = relationship("GLTransaction",       back_populates="entity")
    engagements         = relationship("Engagement",          back_populates="entity")
    documents           = relationship("Document",            back_populates="entity")
    financial_ratios    = relationship("FinancialRatio",      back_populates="entity")


# ─────────────────────────────────────────────────────────────────────────────
# ACCOUNTS
# ─────────────────────────────────────────────────────────────────────────────

class Account(Base):
    __tablename__ = "accounts"
    __table_args__ = {"schema": "financial"}

    account_id      = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    account_code    = Column(String(20),  nullable=False, unique=True)
    account_name    = Column(String(300), nullable=False)
    account_name_vi = Column(String(300))
    account_class   = Column(String(20),  nullable=False)
    account_type    = Column(String(30))
    normal_balance  = Column(String(6))
    parent_account_id = Column(UUID(as_uuid=True), ForeignKey("financial.accounts.account_id"))
    level           = Column(SmallInteger, default=1)
    is_control      = Column(Boolean, default=False)
    is_active       = Column(Boolean, default=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())


# ─────────────────────────────────────────────────────────────────────────────
# FISCAL PERIODS
# ─────────────────────────────────────────────────────────────────────────────

class FiscalPeriod(Base):
    __tablename__ = "fiscal_periods"
    __table_args__ = (
        UniqueConstraint("entity_id", "fiscal_year", "period_type", "period_number"),
        {"schema": "financial"},
    )

    period_id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id       = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    fiscal_year     = Column(SmallInteger, nullable=False)
    period_type     = Column(String(10),   nullable=False)
    period_number   = Column(SmallInteger)
    start_date      = Column(Date, nullable=False)
    end_date        = Column(Date, nullable=False)
    status          = Column(String(20), default="OPEN")
    close_date      = Column(Date)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    entity          = relationship("Entity", back_populates="fiscal_periods")
    financial_statements = relationship("FinancialStatement", back_populates="period")
    financial_ratios = relationship("FinancialRatio", back_populates="period")


# ─────────────────────────────────────────────────────────────────────────────
# FINANCIAL STATEMENTS
# ─────────────────────────────────────────────────────────────────────────────

class FinancialStatement(Base):
    __tablename__ = "financial_statements"
    __table_args__ = {"schema": "financial"}

    statement_id        = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id           = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    period_id           = Column(UUID(as_uuid=True), ForeignKey("financial.fiscal_periods.period_id"), nullable=False)
    statement_type      = Column(String(30), nullable=False)
    reporting_currency  = Column(String(3),  default="VND")
    reporting_unit      = Column(String(20), default="VND")
    reporting_standard  = Column(String(20), default="VAS")
    consolidation_type  = Column(String(20), default="STANDALONE")
    audit_status        = Column(String(30), default="UNAUDITED")
    submission_date     = Column(Date)
    source_file_name    = Column(String(500))
    source_blob_uri     = Column(Text)
    checksum_md5        = Column(String(32))
    created_at          = Column(DateTime(timezone=True), server_default=func.now())
    updated_at          = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    entity              = relationship("Entity",      back_populates="financial_statements")
    period              = relationship("FiscalPeriod", back_populates="financial_statements")
    balance_sheet_items = relationship("BalanceSheetItem",      back_populates="statement", cascade="all, delete-orphan")
    income_items        = relationship("IncomeStatementItem",   back_populates="statement", cascade="all, delete-orphan")
    cash_flow_items     = relationship("CashFlowItem",          back_populates="statement", cascade="all, delete-orphan")


class BalanceSheetItem(Base):
    __tablename__ = "balance_sheet_items"
    __table_args__ = {"schema": "financial"}

    item_id                 = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    statement_id            = Column(UUID(as_uuid=True), ForeignKey("financial.financial_statements.statement_id", ondelete="CASCADE"), nullable=False)
    entity_id               = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    period_id               = Column(UUID(as_uuid=True), ForeignKey("financial.fiscal_periods.period_id"), nullable=False)
    line_code               = Column(String(20),  nullable=False)
    line_name               = Column(String(300), nullable=False)
    line_name_vi            = Column(String(300))
    account_class           = Column(String(20))
    is_subtotal             = Column(Boolean, default=False)
    sort_order              = Column(Integer)
    level                   = Column(SmallInteger, default=1)
    parent_line_code        = Column(String(20))
    current_year_amount     = Column(Numeric(22, 2), default=0)
    prior_year_amount       = Column(Numeric(22, 2))
    beginning_year_amount   = Column(Numeric(22, 2))
    currency                = Column(String(3), default="VND")
    reporting_unit          = Column(String(20), default="VND")
    created_at              = Column(DateTime(timezone=True), server_default=func.now())

    statement = relationship("FinancialStatement", back_populates="balance_sheet_items")


class IncomeStatementItem(Base):
    __tablename__ = "income_statement_items"
    __table_args__ = {"schema": "financial"}

    item_id                 = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    statement_id            = Column(UUID(as_uuid=True), ForeignKey("financial.financial_statements.statement_id", ondelete="CASCADE"), nullable=False)
    entity_id               = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    period_id               = Column(UUID(as_uuid=True), ForeignKey("financial.fiscal_periods.period_id"), nullable=False)
    line_code               = Column(String(20),  nullable=False)
    line_name               = Column(String(300), nullable=False)
    line_name_vi            = Column(String(300))
    is_subtotal             = Column(Boolean, default=False)
    sort_order              = Column(Integer)
    level                   = Column(SmallInteger, default=1)
    current_period_amount   = Column(Numeric(22, 2), default=0)
    prior_period_amount     = Column(Numeric(22, 2))
    ytd_amount              = Column(Numeric(22, 2))
    currency                = Column(String(3), default="VND")
    created_at              = Column(DateTime(timezone=True), server_default=func.now())

    statement = relationship("FinancialStatement", back_populates="income_items")


class CashFlowItem(Base):
    __tablename__ = "cash_flow_items"
    __table_args__ = {"schema": "financial"}

    item_id                 = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    statement_id            = Column(UUID(as_uuid=True), ForeignKey("financial.financial_statements.statement_id", ondelete="CASCADE"), nullable=False)
    entity_id               = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    period_id               = Column(UUID(as_uuid=True), ForeignKey("financial.fiscal_periods.period_id"), nullable=False)
    line_code               = Column(String(20),  nullable=False)
    line_name               = Column(String(300), nullable=False)
    line_name_vi            = Column(String(300))
    activity_type           = Column(String(20),  nullable=False)
    method                  = Column(String(10),  default="INDIRECT")
    sort_order              = Column(Integer)
    is_subtotal             = Column(Boolean, default=False)
    current_period_amount   = Column(Numeric(22, 2), default=0)
    prior_period_amount     = Column(Numeric(22, 2))
    currency                = Column(String(3), default="VND")
    created_at              = Column(DateTime(timezone=True), server_default=func.now())

    statement = relationship("FinancialStatement", back_populates="cash_flow_items")


# ─────────────────────────────────────────────────────────────────────────────
# FINANCIAL RATIOS
# ─────────────────────────────────────────────────────────────────────────────

class FinancialRatio(Base):
    __tablename__ = "financial_ratios"
    __table_args__ = (
        UniqueConstraint("entity_id", "period_id"),
        {"schema": "financial"},
    )

    ratio_id                = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id               = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    period_id               = Column(UUID(as_uuid=True), ForeignKey("financial.fiscal_periods.period_id"), nullable=False)
    computed_at             = Column(DateTime(timezone=True), server_default=func.now())

    current_ratio           = Column(Numeric(10, 4))
    quick_ratio             = Column(Numeric(10, 4))
    cash_ratio              = Column(Numeric(10, 4))
    debt_to_equity          = Column(Numeric(10, 4))
    debt_to_assets          = Column(Numeric(10, 4))
    equity_ratio            = Column(Numeric(10, 4))
    interest_coverage       = Column(Numeric(10, 4))
    gross_profit_margin     = Column(Numeric(10, 4))
    operating_profit_margin = Column(Numeric(10, 4))
    net_profit_margin       = Column(Numeric(10, 4))
    return_on_assets        = Column(Numeric(10, 4))
    return_on_equity        = Column(Numeric(10, 4))
    ebitda_margin           = Column(Numeric(10, 4))
    asset_turnover          = Column(Numeric(10, 4))
    revenue_growth_yoy      = Column(Numeric(10, 4))
    net_income_growth_yoy   = Column(Numeric(10, 4))
    eps                     = Column(Numeric(18, 4))

    total_revenue           = Column(Numeric(22, 2))
    gross_profit            = Column(Numeric(22, 2))
    ebit                    = Column(Numeric(22, 2))
    ebitda                  = Column(Numeric(22, 2))
    net_income              = Column(Numeric(22, 2))
    total_assets            = Column(Numeric(22, 2))
    total_liabilities       = Column(Numeric(22, 2))
    total_equity            = Column(Numeric(22, 2))
    current_assets          = Column(Numeric(22, 2))
    current_liabilities     = Column(Numeric(22, 2))
    cash_and_equivalents    = Column(Numeric(22, 2))
    operating_cash_flow     = Column(Numeric(22, 2))

    entity  = relationship("Entity",      back_populates="financial_ratios")
    period  = relationship("FiscalPeriod", back_populates="financial_ratios")


# ─────────────────────────────────────────────────────────────────────────────
# GL TRANSACTIONS
# ─────────────────────────────────────────────────────────────────────────────

class GLTransaction(Base):
    __tablename__ = "gl_transactions"
    __table_args__ = {"schema": "financial"}

    transaction_id      = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id           = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    journal_entry_no    = Column(String(50),  nullable=False)
    transaction_date    = Column(Date,        nullable=False)
    posting_date        = Column(Date)
    transaction_type    = Column(String(50),  nullable=False)
    document_type       = Column(String(30))
    document_no         = Column(String(100))
    debit_account_id    = Column(UUID(as_uuid=True), ForeignKey("financial.accounts.account_id"))
    credit_account_id   = Column(UUID(as_uuid=True), ForeignKey("financial.accounts.account_id"))
    amount              = Column(Numeric(22, 2), nullable=False)
    currency            = Column(String(3), default="VND")
    exchange_rate       = Column(Numeric(18, 6), default=1)
    amount_vnd          = Column(Numeric(22, 2))
    counterparty_id     = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"))
    counterparty_name   = Column(String(500))
    description         = Column(Text)
    is_reconciled       = Column(Boolean, default=False)
    anomaly_score       = Column(Numeric(5, 4))
    anomaly_flag        = Column(Boolean, default=False)
    source_system       = Column(String(100))
    source_file         = Column(String(500))
    run_id              = Column(String(50))
    created_at          = Column(DateTime(timezone=True), server_default=func.now())

    entity = relationship("Entity", foreign_keys=[entity_id], back_populates="gl_transactions")


# ─────────────────────────────────────────────────────────────────────────────
# AUDIT
# ─────────────────────────────────────────────────────────────────────────────

class Engagement(Base):
    __tablename__ = "engagements"
    __table_args__ = {"schema": "audit"}

    engagement_id       = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    engagement_code     = Column(String(50),  unique=True, nullable=False)
    entity_id           = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    engagement_type     = Column(String(50),  nullable=False)
    fiscal_year         = Column(SmallInteger, nullable=False)
    period_start        = Column(Date, nullable=False)
    period_end          = Column(Date, nullable=False)
    partner_in_charge   = Column(String(200))
    manager             = Column(String(200))
    status              = Column(String(30), default="PLANNING")
    audit_opinion       = Column(String(30))
    opinion_date        = Column(Date)
    report_date         = Column(Date)
    contracted_fee      = Column(Numeric(18, 2))
    notes               = Column(Text)
    created_at          = Column(DateTime(timezone=True), server_default=func.now())
    updated_at          = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    entity   = relationship("Entity", back_populates="engagements")
    findings = relationship("Finding", back_populates="engagement", cascade="all, delete-orphan")


class Finding(Base):
    __tablename__ = "findings"
    __table_args__ = {"schema": "audit"}

    finding_id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    engagement_id       = Column(UUID(as_uuid=True), ForeignKey("audit.engagements.engagement_id"), nullable=False)
    entity_id           = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"), nullable=False)
    finding_code        = Column(String(50), unique=True, nullable=False)
    category            = Column(String(50), nullable=False)
    severity            = Column(String(20), nullable=False)
    title               = Column(String(500), nullable=False)
    description         = Column(Text, nullable=False)
    financial_impact    = Column(Numeric(22, 2))
    root_cause          = Column(Text)
    recommendation      = Column(Text)
    status              = Column(String(20), default="OPEN")
    resolved_date       = Column(Date)
    discovered_by       = Column(String(200))
    discovery_date      = Column(Date)
    created_at          = Column(DateTime(timezone=True), server_default=func.now())
    updated_at          = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    engagement = relationship("Engagement", back_populates="findings")


# ─────────────────────────────────────────────────────────────────────────────
# DOCUMENTS
# ─────────────────────────────────────────────────────────────────────────────

class Document(Base):
    __tablename__ = "documents"
    __table_args__ = {"schema": "financial"}

    document_id         = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id           = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"))
    engagement_id       = Column(UUID(as_uuid=True), ForeignKey("audit.engagements.engagement_id"))
    file_name           = Column(String(500), nullable=False)
    file_type           = Column(String(20),  nullable=False)
    document_category   = Column(String(50))
    fiscal_year         = Column(SmallInteger)
    blob_uri            = Column(Text)
    blob_zone           = Column(String(20), default="raw")
    page_count          = Column(Integer)
    word_count          = Column(Integer)
    language            = Column(String(2), default="vi")
    checksum_md5        = Column(String(32), unique=True)
    file_size_bytes     = Column(Integer)
    extraction_status   = Column(String(20), default="PENDING")
    rag_indexed         = Column(Boolean, default=False)
    kg_indexed          = Column(Boolean, default=False)
    run_id              = Column(String(50))
    ingested_at         = Column(DateTime(timezone=True), server_default=func.now())
    processed_at        = Column(DateTime(timezone=True))
    created_at          = Column(DateTime(timezone=True), server_default=func.now())

    entity = relationship("Entity", back_populates="documents")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE RUNS
# ─────────────────────────────────────────────────────────────────────────────

class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    __table_args__ = {"schema": "pipeline"}

    run_id          = Column(String(50),  primary_key=True)
    pipeline_name   = Column(String(200), nullable=False)
    source_file     = Column(String(500))
    entity_id       = Column(UUID(as_uuid=True), ForeignKey("financial.entities.entity_id"))
    target_table    = Column(String(200))
    status          = Column(String(20), default="RUNNING")
    started_at      = Column(DateTime(timezone=True), server_default=func.now())
    completed_at    = Column(DateTime(timezone=True))
    duration_seconds = Column(Numeric(10, 2))
    rows_ingested   = Column(Integer, default=0)
    rows_processed  = Column(Integer, default=0)
    rows_loaded     = Column(Integer, default=0)
    rows_failed     = Column(Integer, default=0)
    error_message   = Column(Text)
    metrics         = Column(JSONB)
    triggered_by    = Column(String(100), default="airflow")
    created_at      = Column(DateTime(timezone=True), server_default=func.now())
