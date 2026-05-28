-- ══════════════════════════════════════════════════════════════════════════════
-- Enterprise Data Platform – Financial Domain Schema
-- Modelled after KPMG Audit & Advisory operational data
-- Standards: IFRS, VAS (Vietnamese Accounting Standards), Basel III (banking)
-- ══════════════════════════════════════════════════════════════════════════════

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";      -- Full-text search on names

-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMA NAMESPACES
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS financial;       -- Core financial entities
CREATE SCHEMA IF NOT EXISTS audit;           -- Audit engagements & findings
CREATE SCHEMA IF NOT EXISTS risk;            -- Risk assessments
CREATE SCHEMA IF NOT EXISTS pipeline;        -- ETL lineage & monitoring

SET search_path TO financial, audit, risk, pipeline, public;

-- ─────────────────────────────────────────────────────────────────────────────
-- LOOKUP / REFERENCE TABLES
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.currencies (
    code            CHAR(3)     PRIMARY KEY,           -- ISO 4217: VND, USD, EUR
    name            VARCHAR(100) NOT NULL,
    symbol          VARCHAR(10),
    decimal_places  SMALLINT    DEFAULT 2,
    is_active       BOOLEAN     DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO financial.currencies (code, name, symbol, decimal_places) VALUES
    ('VND', 'Vietnamese Dong',  '₫', 0),
    ('USD', 'US Dollar',        '$', 2),
    ('EUR', 'Euro',             '€', 2),
    ('SGD', 'Singapore Dollar', 'S$', 2),
    ('HKD', 'Hong Kong Dollar', 'HK$', 2),
    ('GBP', 'British Pound',    '£', 2),
    ('JPY', 'Japanese Yen',     '¥', 0);

CREATE TABLE financial.industry_codes (
    code        VARCHAR(20)  PRIMARY KEY,   -- VSIC / ISIC code
    name_en     VARCHAR(200) NOT NULL,
    name_vi     VARCHAR(200),
    category    VARCHAR(100),               -- Banking, Manufacturing, Real Estate …
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO financial.industry_codes (code, name_en, name_vi, category) VALUES
    ('6412', 'Commercial Banking',                    'Ngân hàng thương mại',            'Financial Services'),
    ('6499', 'Other Financial Services',              'Dịch vụ tài chính khác',           'Financial Services'),
    ('6512', 'Non-Life Insurance',                    'Bảo hiểm phi nhân thọ',            'Insurance'),
    ('6511', 'Life Insurance',                        'Bảo hiểm nhân thọ',               'Insurance'),
    ('6810', 'Real Estate Development',               'Phát triển bất động sản',          'Real Estate'),
    ('4610', 'Wholesale Trade',                       'Thương mại bán buôn',             'Trade'),
    ('6201', 'Computer Programming',                  'Lập trình máy tính',              'Technology'),
    ('3510', 'Electric Power Generation',             'Sản xuất điện',                   'Energy'),
    ('1011', 'Processing of Meat',                    'Chế biến thịt',                   'Manufacturing'),
    ('2410', 'Iron and Steel Industry',               'Sản xuất thép',                   'Manufacturing'),
    ('5510', 'Hotels',                                'Khách sạn',                       'Hospitality'),
    ('4911', 'Rail Transport',                        'Vận tải đường sắt',              'Transport');

-- ─────────────────────────────────────────────────────────────────────────────
-- ENTITIES / CLIENTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.entities (
    entity_id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_code         VARCHAR(50) UNIQUE NOT NULL,    -- Internal reference code
    legal_name          VARCHAR(500) NOT NULL,
    short_name          VARCHAR(200),
    tax_id              VARCHAR(20) UNIQUE,             -- Mã số thuế (MST)
    registration_no     VARCHAR(50),                    -- Business registration number
    entity_type         VARCHAR(50) NOT NULL
        CHECK (entity_type IN (
            'PUBLIC_COMPANY',       -- Niêm yết
            'PRIVATE_COMPANY',      -- Công ty TNHH / Cổ phần tư nhân
            'STATE_OWNED',          -- Doanh nghiệp nhà nước
            'FOREIGN_INVESTED',     -- FDI
            'BANK',
            'INSURANCE',
            'FUND',
            'NGO',
            'GOVERNMENT'
        )),
    industry_code       VARCHAR(20) REFERENCES financial.industry_codes(code),
    functional_currency CHAR(3)     NOT NULL DEFAULT 'VND'
                        REFERENCES financial.currencies(code),
    reporting_standard  VARCHAR(20) DEFAULT 'VAS'
        CHECK (reporting_standard IN ('VAS', 'IFRS', 'US_GAAP')),

    -- Address
    country             CHAR(2)     DEFAULT 'VN',       -- ISO 3166
    province            VARCHAR(100),
    address             TEXT,

    -- Listing info (if public company)
    stock_exchange      VARCHAR(20),                    -- HOSE, HNX, UPCOM
    ticker_symbol       VARCHAR(20),
    listing_date        DATE,

    -- Parent / subsidiary
    parent_entity_id    UUID REFERENCES financial.entities(entity_id),

    -- Status
    is_active           BOOLEAN     DEFAULT TRUE,
    incorporation_date  DATE,
    dissolution_date    DATE,

    -- Metadata
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    created_by          VARCHAR(100),
    notes               TEXT
);

CREATE INDEX idx_entities_name    ON financial.entities USING gin(legal_name gin_trgm_ops);
CREATE INDEX idx_entities_tax_id  ON financial.entities(tax_id);
CREATE INDEX idx_entities_type    ON financial.entities(entity_type);
CREATE INDEX idx_entities_parent  ON financial.entities(parent_entity_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- CHART OF ACCOUNTS (Hệ thống tài khoản kế toán)
-- Based on Vietnamese Decision 48 / Circular 200
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.account_categories (
    category_code   VARCHAR(10)  PRIMARY KEY,
    name_en         VARCHAR(200) NOT NULL,
    name_vi         VARCHAR(200),
    account_class   VARCHAR(20)  NOT NULL
        CHECK (account_class IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE', 'OFF_BALANCE'))
);

INSERT INTO financial.account_categories VALUES
    ('1',  'Current Assets',                'Tài sản ngắn hạn',             'ASSET'),
    ('11', 'Cash and Cash Equivalents',     'Tiền và tương đương tiền',      'ASSET'),
    ('12', 'Short-term Investments',        'Đầu tư ngắn hạn',              'ASSET'),
    ('13', 'Receivables',                   'Phải thu ngắn hạn',            'ASSET'),
    ('15', 'Inventories',                   'Hàng tồn kho',                 'ASSET'),
    ('2',  'Non-Current Assets',            'Tài sản dài hạn',              'ASSET'),
    ('21', 'Long-term Receivables',         'Phải thu dài hạn',             'ASSET'),
    ('22', 'Long-term Investments',         'Đầu tư dài hạn',               'ASSET'),
    ('21', 'Fixed Assets',                  'Tài sản cố định',              'ASSET'),
    ('3',  'Current Liabilities',           'Nợ ngắn hạn',                  'LIABILITY'),
    ('31', 'Short-term Loans',              'Vay ngắn hạn',                 'LIABILITY'),
    ('33', 'Payables & Accruals',           'Phải trả và phải nộp',         'LIABILITY'),
    ('4',  'Non-Current Liabilities',       'Nợ dài hạn',                   'LIABILITY'),
    ('41', 'Long-term Loans',               'Vay dài hạn',                  'LIABILITY'),
    ('5',  'Equity',                        'Vốn chủ sở hữu',               'EQUITY'),
    ('41', 'Charter Capital',               'Vốn điều lệ',                  'EQUITY'),
    ('51', 'Revenue',                       'Doanh thu bán hàng',           'REVENUE'),
    ('52', 'Deductions from Revenue',       'Các khoản giảm trừ',           'REVENUE'),
    ('61', 'Cost of Goods Sold',            'Giá vốn hàng bán',             'EXPENSE'),
    ('64', 'General & Admin Expenses',      'Chi phí quản lý DN',           'EXPENSE'),
    ('63', 'Selling Expenses',              'Chi phí bán hàng',             'EXPENSE'),
    ('71', 'Other Income',                  'Thu nhập khác',                'REVENUE'),
    ('81', 'Other Expenses',                'Chi phí khác',                 'EXPENSE'),
    ('82', 'Corporate Income Tax',          'Chi phí thuế TNDN',            'EXPENSE');

CREATE TABLE financial.accounts (
    account_id      UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_code    VARCHAR(20) NOT NULL,
    account_name    VARCHAR(300) NOT NULL,
    account_name_vi VARCHAR(300),
    category_code   VARCHAR(10) REFERENCES financial.account_categories(category_code),
    account_class   VARCHAR(20) NOT NULL
        CHECK (account_class IN ('ASSET', 'LIABILITY', 'EQUITY', 'REVENUE', 'EXPENSE', 'OFF_BALANCE')),
    account_type    VARCHAR(30)
        CHECK (account_type IN ('BALANCE_SHEET', 'INCOME_STATEMENT', 'CASH_FLOW', 'MEMO')),
    normal_balance  CHAR(6)     CHECK (normal_balance IN ('DEBIT', 'CREDIT')),
    parent_account_id UUID      REFERENCES financial.accounts(account_id),
    level           SMALLINT    DEFAULT 1,  -- 1=class, 2=group, 3=account, 4=subaccount
    is_control      BOOLEAN     DEFAULT FALSE,
    is_active       BOOLEAN     DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_accounts_code ON financial.accounts(account_code);

-- ─────────────────────────────────────────────────────────────────────────────
-- FINANCIAL PERIODS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.fiscal_periods (
    period_id       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID        NOT NULL REFERENCES financial.entities(entity_id),
    fiscal_year     SMALLINT    NOT NULL,
    period_type     VARCHAR(10) NOT NULL
        CHECK (period_type IN ('ANNUAL', 'H1', 'H2', 'Q1', 'Q2', 'Q3', 'Q4', 'MONTHLY')),
    period_number   SMALLINT,                   -- 1-12 for monthly, 1-4 for quarterly
    start_date      DATE        NOT NULL,
    end_date        DATE        NOT NULL,
    status          VARCHAR(20) DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'CLOSED', 'AUDITED', 'RESTATED')),
    close_date      DATE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (entity_id, fiscal_year, period_type, period_number)
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FINANCIAL STATEMENTS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.financial_statements (
    statement_id        UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    period_id           UUID        NOT NULL REFERENCES financial.fiscal_periods(period_id),
    statement_type      VARCHAR(30) NOT NULL
        CHECK (statement_type IN (
            'BALANCE_SHEET',        -- Bảng cân đối kế toán
            'INCOME_STATEMENT',     -- Báo cáo kết quả kinh doanh
            'CASH_FLOW',            -- Báo cáo lưu chuyển tiền tệ
            'EQUITY_CHANGES',       -- Báo cáo thay đổi vốn chủ
            'NOTES'                 -- Thuyết minh BCTC
        )),
    reporting_currency  CHAR(3)     DEFAULT 'VND' REFERENCES financial.currencies(code),
    reporting_unit      VARCHAR(20) DEFAULT 'VND'
        CHECK (reporting_unit IN ('VND', 'THOUSANDS_VND', 'MILLIONS_VND', 'BILLIONS_VND', 'USD')),
    reporting_standard  VARCHAR(20) DEFAULT 'VAS',
    consolidation_type  VARCHAR(20) DEFAULT 'STANDALONE'
        CHECK (consolidation_type IN ('STANDALONE', 'CONSOLIDATED', 'COMBINED')),
    audit_status        VARCHAR(30) DEFAULT 'UNAUDITED'
        CHECK (audit_status IN ('UNAUDITED', 'REVIEWED', 'AUDITED', 'QUALIFIED', 'ADVERSE', 'DISCLAIMED')),
    submission_date     DATE,
    source_file_name    VARCHAR(500),
    source_blob_uri     TEXT,
    checksum_md5        VARCHAR(32),
    extracted_by        VARCHAR(100),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_fs_entity_period ON financial.financial_statements(entity_id, period_id);
CREATE INDEX idx_fs_type          ON financial.financial_statements(statement_type);
CREATE INDEX idx_fs_status        ON financial.financial_statements(audit_status);

-- ─────────────────────────────────────────────────────────────────────────────
-- BALANCE SHEET LINE ITEMS (Bảng cân đối kế toán)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.balance_sheet_items (
    item_id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id    UUID        NOT NULL REFERENCES financial.financial_statements(statement_id) ON DELETE CASCADE,
    entity_id       UUID        NOT NULL REFERENCES financial.entities(entity_id),
    period_id       UUID        NOT NULL REFERENCES financial.fiscal_periods(period_id),

    -- Line item classification
    line_code       VARCHAR(20) NOT NULL,   -- B01, B02... (Vietnam standard codes)
    line_name       VARCHAR(300) NOT NULL,
    line_name_vi    VARCHAR(300),
    account_id      UUID        REFERENCES financial.accounts(account_id),
    account_class   VARCHAR(20) CHECK (account_class IN ('ASSET', 'LIABILITY', 'EQUITY')),
    is_subtotal     BOOLEAN     DEFAULT FALSE,
    sort_order      INTEGER,
    level           SMALLINT    DEFAULT 1,  -- Indent level for hierarchy
    parent_line_code VARCHAR(20),

    -- Values
    current_year_amount     NUMERIC(22, 2) NOT NULL DEFAULT 0,
    prior_year_amount       NUMERIC(22, 2),
    beginning_year_amount   NUMERIC(22, 2),

    -- Currency
    currency        CHAR(3)     DEFAULT 'VND' REFERENCES financial.currencies(code),
    reporting_unit  VARCHAR(20) DEFAULT 'VND',

    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_bs_entity_period ON financial.balance_sheet_items(entity_id, period_id);
CREATE INDEX idx_bs_line_code     ON financial.balance_sheet_items(line_code);

-- ─────────────────────────────────────────────────────────────────────────────
-- INCOME STATEMENT (Báo cáo kết quả kinh doanh)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.income_statement_items (
    item_id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id        UUID        NOT NULL REFERENCES financial.financial_statements(statement_id) ON DELETE CASCADE,
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    period_id           UUID        NOT NULL REFERENCES financial.fiscal_periods(period_id),

    line_code           VARCHAR(20) NOT NULL,
    line_name           VARCHAR(300) NOT NULL,
    line_name_vi        VARCHAR(300),
    account_id          UUID        REFERENCES financial.accounts(account_id),
    is_subtotal         BOOLEAN     DEFAULT FALSE,
    sort_order          INTEGER,
    level               SMALLINT    DEFAULT 1,
    parent_line_code    VARCHAR(20),

    current_period_amount   NUMERIC(22, 2) NOT NULL DEFAULT 0,
    prior_period_amount     NUMERIC(22, 2),
    ytd_amount              NUMERIC(22, 2),

    currency            CHAR(3)     DEFAULT 'VND' REFERENCES financial.currencies(code),
    reporting_unit      VARCHAR(20) DEFAULT 'VND',
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_is_entity_period ON financial.income_statement_items(entity_id, period_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- CASH FLOW STATEMENT (Báo cáo lưu chuyển tiền tệ)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.cash_flow_items (
    item_id             UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id        UUID        NOT NULL REFERENCES financial.financial_statements(statement_id) ON DELETE CASCADE,
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    period_id           UUID        NOT NULL REFERENCES financial.fiscal_periods(period_id),

    line_code           VARCHAR(20) NOT NULL,
    line_name           VARCHAR(300) NOT NULL,
    line_name_vi        VARCHAR(300),
    activity_type       VARCHAR(20) NOT NULL
        CHECK (activity_type IN ('OPERATING', 'INVESTING', 'FINANCING')),
    method              VARCHAR(10) DEFAULT 'INDIRECT'
        CHECK (method IN ('DIRECT', 'INDIRECT')),
    sort_order          INTEGER,
    is_subtotal         BOOLEAN     DEFAULT FALSE,

    current_period_amount   NUMERIC(22, 2) NOT NULL DEFAULT 0,
    prior_period_amount     NUMERIC(22, 2),

    currency            CHAR(3)     DEFAULT 'VND' REFERENCES financial.currencies(code),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- FINANCIAL RATIOS & KPIs (Computed / Derived metrics)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.financial_ratios (
    ratio_id        UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id       UUID        NOT NULL REFERENCES financial.entities(entity_id),
    period_id       UUID        NOT NULL REFERENCES financial.fiscal_periods(period_id),
    computed_at     TIMESTAMPTZ DEFAULT NOW(),

    -- Liquidity Ratios (Thanh khoản)
    current_ratio           NUMERIC(10, 4),   -- Current Assets / Current Liabilities
    quick_ratio             NUMERIC(10, 4),   -- (CA - Inventory) / CL
    cash_ratio              NUMERIC(10, 4),   -- Cash / CL
    operating_cash_flow_ratio NUMERIC(10, 4), -- CFO / CL

    -- Leverage / Solvency (Đòn bẩy tài chính)
    debt_to_equity          NUMERIC(10, 4),   -- Total Debt / Total Equity
    debt_to_assets          NUMERIC(10, 4),   -- Total Debt / Total Assets
    equity_ratio            NUMERIC(10, 4),   -- Total Equity / Total Assets
    interest_coverage       NUMERIC(10, 4),   -- EBIT / Interest Expense
    debt_service_coverage   NUMERIC(10, 4),

    -- Profitability (Khả năng sinh lời)
    gross_profit_margin     NUMERIC(10, 4),   -- Gross Profit / Revenue
    operating_profit_margin NUMERIC(10, 4),   -- EBIT / Revenue
    net_profit_margin       NUMERIC(10, 4),   -- Net Income / Revenue
    return_on_assets        NUMERIC(10, 4),   -- Net Income / Avg Total Assets (ROA)
    return_on_equity        NUMERIC(10, 4),   -- Net Income / Avg Equity (ROE)
    return_on_capital_emp   NUMERIC(10, 4),   -- EBIT / Capital Employed (ROCE)
    ebitda_margin           NUMERIC(10, 4),

    -- Efficiency (Hiệu quả hoạt động)
    asset_turnover          NUMERIC(10, 4),   -- Revenue / Avg Total Assets
    inventory_turnover      NUMERIC(10, 4),
    receivables_turnover    NUMERIC(10, 4),
    days_sales_outstanding  NUMERIC(10, 2),   -- DSO
    days_inventory_outstanding NUMERIC(10, 2),-- DIO
    days_payable_outstanding NUMERIC(10, 2),  -- DPO
    cash_conversion_cycle   NUMERIC(10, 2),   -- DSO + DIO - DPO

    -- Growth Rates (so với cùng kỳ)
    revenue_growth_yoy      NUMERIC(10, 4),
    net_income_growth_yoy   NUMERIC(10, 4),
    total_assets_growth_yoy NUMERIC(10, 4),
    equity_growth_yoy       NUMERIC(10, 4),

    -- Per-share (if listed)
    eps                     NUMERIC(18, 4),   -- Earnings per Share
    book_value_per_share    NUMERIC(18, 4),
    dividend_per_share      NUMERIC(18, 4),

    -- Raw inputs used for computation
    total_revenue           NUMERIC(22, 2),
    gross_profit            NUMERIC(22, 2),
    ebit                    NUMERIC(22, 2),
    ebitda                  NUMERIC(22, 2),
    net_income              NUMERIC(22, 2),
    total_assets            NUMERIC(22, 2),
    total_liabilities       NUMERIC(22, 2),
    total_equity            NUMERIC(22, 2),
    current_assets          NUMERIC(22, 2),
    current_liabilities     NUMERIC(22, 2),
    cash_and_equivalents    NUMERIC(22, 2),
    operating_cash_flow     NUMERIC(22, 2),

    UNIQUE (entity_id, period_id)
);

CREATE INDEX idx_ratios_entity_period ON financial.financial_ratios(entity_id, period_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- GENERAL LEDGER TRANSACTIONS
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.gl_transactions (
    transaction_id      UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    journal_entry_no    VARCHAR(50) NOT NULL,
    transaction_date    DATE        NOT NULL,
    posting_date        DATE,
    value_date          DATE,

    -- Classification
    transaction_type    VARCHAR(50) NOT NULL
        CHECK (transaction_type IN (
            'REVENUE', 'EXPENSE', 'ASSET_PURCHASE', 'ASSET_DISPOSAL',
            'LOAN_DRAWDOWN', 'LOAN_REPAYMENT', 'DIVIDEND',
            'TAX_PAYMENT', 'PAYROLL', 'INTERCOMPANY', 'ADJUSTMENT', 'OTHER'
        )),
    document_type       VARCHAR(30),    -- Invoice, Receipt, Bank statement, etc.
    document_no         VARCHAR(100),   -- External doc reference

    -- Accounts
    debit_account_id    UUID        REFERENCES financial.accounts(account_id),
    credit_account_id   UUID        REFERENCES financial.accounts(account_id),

    -- Amounts
    amount              NUMERIC(22, 2) NOT NULL CHECK (amount > 0),
    currency            CHAR(3)     DEFAULT 'VND' REFERENCES financial.currencies(code),
    exchange_rate       NUMERIC(18, 6) DEFAULT 1,
    amount_vnd          NUMERIC(22, 2),            -- Converted to VND

    -- Counterparties
    counterparty_id     UUID        REFERENCES financial.entities(entity_id),
    counterparty_name   VARCHAR(500),

    description         TEXT,
    reference           VARCHAR(200),

    -- Data quality
    is_reconciled       BOOLEAN     DEFAULT FALSE,
    anomaly_score       NUMERIC(5, 4),     -- ML-derived anomaly score [0,1]
    anomaly_flag        BOOLEAN     DEFAULT FALSE,

    -- Lineage
    source_system       VARCHAR(100),
    source_file         VARCHAR(500),
    run_id              VARCHAR(50),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_gl_entity_date     ON financial.gl_transactions(entity_id, transaction_date);
CREATE INDEX idx_gl_type            ON financial.gl_transactions(transaction_type);
CREATE INDEX idx_gl_anomaly         ON financial.gl_transactions(anomaly_flag) WHERE anomaly_flag = TRUE;
CREATE INDEX idx_gl_amount          ON financial.gl_transactions(amount);
CREATE INDEX idx_gl_debit_acc       ON financial.gl_transactions(debit_account_id);
CREATE INDEX idx_gl_credit_acc      ON financial.gl_transactions(credit_account_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- AUDIT ENGAGEMENTS (Hợp đồng kiểm toán)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE audit.engagements (
    engagement_id       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    engagement_code     VARCHAR(50) UNIQUE NOT NULL,    -- e.g. KPMG-2025-VN-001
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),

    engagement_type     VARCHAR(50) NOT NULL
        CHECK (engagement_type IN (
            'STATUTORY_AUDIT',          -- Kiểm toán báo cáo tài chính
            'INTERNAL_AUDIT',
            'FORENSIC_AUDIT',           -- Kiểm toán pháp lý
            'TAX_REVIEW',
            'DUE_DILIGENCE',
            'IFRS_CONVERSION',
            'SPECIAL_PURPOSE',
            'ADVISORY'
        )),

    -- Period
    fiscal_year         SMALLINT    NOT NULL,
    period_start        DATE        NOT NULL,
    period_end          DATE        NOT NULL,

    -- Team
    partner_in_charge   VARCHAR(200),
    manager             VARCHAR(200),
    senior_associate    VARCHAR(200),

    -- Status
    status              VARCHAR(30) DEFAULT 'PLANNING'
        CHECK (status IN (
            'PLANNING', 'FIELDWORK', 'REVIEW',
            'REPORTING', 'COMPLETED', 'CANCELLED'
        )),
    planned_start       DATE,
    planned_end         DATE,
    actual_start        DATE,
    actual_end          DATE,

    -- Fees
    contracted_fee      NUMERIC(18, 2),
    fee_currency        CHAR(3)     DEFAULT 'VND',

    -- Opinion
    audit_opinion       VARCHAR(30)
        CHECK (audit_opinion IN (
            'UNQUALIFIED',      -- Không ngoại trừ
            'QUALIFIED',        -- Ngoại trừ
            'ADVERSE',          -- Từ chối
            'DISCLAIMER',       -- Không thể đưa ra ý kiến
            'EMPHASIS_OF_MATTER'
        )),
    opinion_date        DATE,
    report_date         DATE,
    report_signed_date  DATE,

    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_eng_entity   ON audit.engagements(entity_id);
CREATE INDEX idx_eng_year     ON audit.engagements(fiscal_year);
CREATE INDEX idx_eng_status   ON audit.engagements(status);

-- ─────────────────────────────────────────────────────────────────────────────
-- AUDIT FINDINGS (Phát hiện kiểm toán)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE audit.findings (
    finding_id          UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    engagement_id       UUID        NOT NULL REFERENCES audit.engagements(engagement_id),
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    finding_code        VARCHAR(50) UNIQUE NOT NULL,

    category            VARCHAR(50) NOT NULL
        CHECK (category IN (
            'MISSTATEMENT',             -- Sai sót
            'CONTROL_DEFICIENCY',       -- Yếu kém kiểm soát nội bộ
            'NON_COMPLIANCE',           -- Vi phạm quy định
            'GOING_CONCERN',            -- Nghi ngờ khả năng hoạt động liên tục
            'RELATED_PARTY',            -- Giao dịch bên liên quan
            'FRAUD_RISK',               -- Rủi ro gian lận
            'DISCLOSURE',               -- Thiếu thuyết minh
            'VALUATION',                -- Định giá tài sản
            'SIGNIFICANT_MATTER'
        )),

    severity            VARCHAR(20) NOT NULL
        CHECK (severity IN ('CRITICAL', 'SIGNIFICANT', 'MODERATE', 'MINOR', 'INFORMATIONAL')),

    title               VARCHAR(500) NOT NULL,
    description         TEXT         NOT NULL,
    financial_impact    NUMERIC(22, 2),
    financial_impact_currency CHAR(3) DEFAULT 'VND',
    account_affected    UUID        REFERENCES financial.accounts(account_id),
    period_affected     UUID        REFERENCES financial.fiscal_periods(period_id),

    -- Root cause
    root_cause          TEXT,
    management_response TEXT,
    recommendation      TEXT,

    -- Resolution
    status              VARCHAR(20) DEFAULT 'OPEN'
        CHECK (status IN ('OPEN', 'IN_PROGRESS', 'RESOLVED', 'ACCEPTED_RISK', 'CLOSED')),
    resolved_date       DATE,
    resolved_by         VARCHAR(200),

    -- Discovery
    discovered_by       VARCHAR(200),
    discovery_date      DATE,

    -- Document source
    source_document     VARCHAR(500),

    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_findings_engagement ON audit.findings(engagement_id);
CREATE INDEX idx_findings_severity   ON audit.findings(severity);
CREATE INDEX idx_findings_category   ON audit.findings(category);
CREATE INDEX idx_findings_status     ON audit.findings(status);

-- ─────────────────────────────────────────────────────────────────────────────
-- RISK ASSESSMENTS (Đánh giá rủi ro)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE risk.risk_assessments (
    assessment_id       UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),
    engagement_id       UUID        REFERENCES audit.engagements(engagement_id),
    assessment_date     DATE        NOT NULL DEFAULT CURRENT_DATE,
    assessed_by         VARCHAR(200),
    fiscal_year         SMALLINT,

    -- Overall scores
    inherent_risk_score     NUMERIC(3, 2) CHECK (inherent_risk_score BETWEEN 0 AND 1),
    control_risk_score      NUMERIC(3, 2) CHECK (control_risk_score BETWEEN 0 AND 1),
    detection_risk_score    NUMERIC(3, 2) CHECK (detection_risk_score BETWEEN 0 AND 1),
    audit_risk_score        NUMERIC(3, 2),   -- IR × CR × DR

    -- Risk level
    risk_level          VARCHAR(20)
        CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'VERY_HIGH', 'CRITICAL')),

    -- Specific risk flags
    fraud_risk          BOOLEAN     DEFAULT FALSE,
    going_concern_risk  BOOLEAN     DEFAULT FALSE,
    related_party_risk  BOOLEAN     DEFAULT FALSE,
    tax_compliance_risk BOOLEAN     DEFAULT FALSE,
    foreign_exchange_risk BOOLEAN   DEFAULT FALSE,
    liquidity_risk      BOOLEAN     DEFAULT FALSE,
    regulatory_risk     BOOLEAN     DEFAULT FALSE,

    notes               TEXT,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE risk.risk_factors (
    factor_id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    assessment_id       UUID        NOT NULL REFERENCES risk.risk_assessments(assessment_id),
    entity_id           UUID        NOT NULL REFERENCES financial.entities(entity_id),

    risk_area           VARCHAR(100) NOT NULL,   -- Revenue, Inventory, Payroll, etc.
    risk_description    TEXT         NOT NULL,
    likelihood          VARCHAR(10)  CHECK (likelihood IN ('LOW', 'MEDIUM', 'HIGH')),
    impact              VARCHAR(10)  CHECK (impact IN ('LOW', 'MEDIUM', 'HIGH')),
    risk_score          NUMERIC(3, 2),
    mitigation_controls TEXT,
    residual_risk       VARCHAR(10),

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────────────────────
-- DOCUMENTS REGISTRY (tài liệu đã xử lý)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE financial.documents (
    document_id         UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    entity_id           UUID        REFERENCES financial.entities(entity_id),
    engagement_id       UUID        REFERENCES audit.engagements(engagement_id),

    file_name           VARCHAR(500) NOT NULL,
    file_type           VARCHAR(20)  NOT NULL
        CHECK (file_type IN ('PDF', 'XLSX', 'DOCX', 'CSV', 'XML', 'TXT', 'JSON', 'IMAGE')),
    document_category   VARCHAR(50)
        CHECK (document_category IN (
            'FINANCIAL_STATEMENT',
            'AUDIT_REPORT',
            'TAX_FILING',
            'BOARD_RESOLUTION',
            'CONTRACT',
            'BANK_STATEMENT',
            'INVOICE',
            'PAYROLL',
            'COMPLIANCE_REPORT',
            'PROSPECTUS',
            'OTHER'
        )),
    fiscal_year         SMALLINT,

    -- Storage
    blob_uri            TEXT,
    blob_zone           VARCHAR(20)  DEFAULT 'raw',

    -- Content
    page_count          INTEGER,
    word_count          INTEGER,
    language            CHAR(2)     DEFAULT 'vi',
    checksum_md5        VARCHAR(32) UNIQUE,
    file_size_bytes     BIGINT,

    -- Extraction status
    extraction_status   VARCHAR(20) DEFAULT 'PENDING'
        CHECK (extraction_status IN ('PENDING', 'PROCESSING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    rag_indexed         BOOLEAN     DEFAULT FALSE,
    kg_indexed          BOOLEAN     DEFAULT FALSE,

    -- Lineage
    ingested_by         VARCHAR(100),
    run_id              VARCHAR(50),
    ingested_at         TIMESTAMPTZ DEFAULT NOW(),
    processed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_docs_entity       ON financial.documents(entity_id);
CREATE INDEX idx_docs_engagement   ON financial.documents(engagement_id);
CREATE INDEX idx_docs_category     ON financial.documents(document_category);
CREATE INDEX idx_docs_status       ON financial.documents(extraction_status);
CREATE INDEX idx_docs_rag          ON financial.documents(rag_indexed) WHERE rag_indexed = FALSE;

-- ─────────────────────────────────────────────────────────────────────────────
-- PIPELINE LINEAGE & MONITORING
-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE pipeline.pipeline_runs (
    run_id              VARCHAR(50)  PRIMARY KEY,
    pipeline_name       VARCHAR(200) NOT NULL,
    source_file         VARCHAR(500),
    entity_id           UUID         REFERENCES financial.entities(entity_id),
    target_table        VARCHAR(200),
    status              VARCHAR(20)  DEFAULT 'RUNNING'
        CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
    started_at          TIMESTAMPTZ  DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    duration_seconds    NUMERIC(10, 2),
    rows_ingested       INTEGER      DEFAULT 0,
    rows_processed      INTEGER      DEFAULT 0,
    rows_loaded         INTEGER      DEFAULT 0,
    rows_failed         INTEGER      DEFAULT 0,
    error_message       TEXT,
    metrics             JSONB,
    triggered_by        VARCHAR(100) DEFAULT 'airflow',
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE TABLE pipeline.data_quality_checks (
    check_id            UUID         PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id              VARCHAR(50)  REFERENCES pipeline.pipeline_runs(run_id),
    table_name          VARCHAR(200) NOT NULL,
    column_name         VARCHAR(200),
    rule_type           VARCHAR(50)  NOT NULL,
    expected_value      TEXT,
    actual_value        TEXT,
    passed              BOOLEAN      NOT NULL,
    severity            VARCHAR(20)  DEFAULT 'WARNING'
        CHECK (severity IN ('ERROR', 'WARNING', 'INFO')),
    details             TEXT,
    checked_at          TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX idx_dq_run    ON pipeline.data_quality_checks(run_id);
CREATE INDEX idx_dq_passed ON pipeline.data_quality_checks(passed) WHERE passed = FALSE;

-- ─────────────────────────────────────────────────────────────────────────────
-- VIEWS – Pre-built analytics queries
-- ─────────────────────────────────────────────────────────────────────────────

-- Company financial snapshot
CREATE OR REPLACE VIEW financial.v_company_financial_snapshot AS
SELECT
    e.entity_id,
    e.entity_code,
    e.legal_name,
    e.entity_type,
    e.industry_code,
    ic.name_en          AS industry_name,
    e.functional_currency,
    e.stock_exchange,
    e.ticker_symbol,

    fp.fiscal_year,
    fp.period_type,
    fp.start_date,
    fp.end_date,

    r.total_revenue,
    r.net_income,
    r.total_assets,
    r.total_liabilities,
    r.total_equity,
    r.operating_cash_flow,
    r.ebitda,

    -- Key ratios
    r.current_ratio,
    r.debt_to_equity,
    r.net_profit_margin,
    r.return_on_equity,
    r.return_on_assets,
    r.revenue_growth_yoy,
    r.net_income_growth_yoy,
    r.risk_level,

    ra.risk_level,
    ra.fraud_risk,
    ra.going_concern_risk

FROM financial.entities e
JOIN financial.fiscal_periods fp          ON fp.entity_id = e.entity_id
LEFT JOIN financial.financial_ratios r    ON r.entity_id = e.entity_id AND r.period_id = fp.period_id
LEFT JOIN financial.industry_codes ic     ON ic.code = e.industry_code
LEFT JOIN risk.risk_assessments ra        ON ra.entity_id = e.entity_id AND ra.fiscal_year = fp.fiscal_year
WHERE e.is_active = TRUE;

-- Audit engagement dashboard
CREATE OR REPLACE VIEW audit.v_engagement_dashboard AS
SELECT
    eng.engagement_id,
    eng.engagement_code,
    e.legal_name      AS client_name,
    e.entity_type,
    eng.engagement_type,
    eng.fiscal_year,
    eng.status,
    eng.partner_in_charge,
    eng.manager,
    eng.audit_opinion,
    eng.report_date,
    COUNT(f.finding_id)                                             AS total_findings,
    COUNT(f.finding_id) FILTER (WHERE f.severity = 'CRITICAL')     AS critical_findings,
    COUNT(f.finding_id) FILTER (WHERE f.severity = 'SIGNIFICANT')  AS significant_findings,
    COUNT(f.finding_id) FILTER (WHERE f.status = 'OPEN')           AS open_findings,
    SUM(ABS(f.financial_impact))                                    AS total_financial_impact
FROM audit.engagements eng
JOIN financial.entities e   ON e.entity_id = eng.entity_id
LEFT JOIN audit.findings f  ON f.engagement_id = eng.engagement_id
GROUP BY eng.engagement_id, e.legal_name, e.entity_type;

-- Anomalous GL transactions
CREATE OR REPLACE VIEW financial.v_anomalous_transactions AS
SELECT
    t.transaction_id,
    e.legal_name   AS entity_name,
    t.journal_entry_no,
    t.transaction_date,
    t.transaction_type,
    t.amount,
    t.currency,
    t.amount_vnd,
    t.counterparty_name,
    t.description,
    t.anomaly_score,
    t.source_system
FROM financial.gl_transactions t
JOIN financial.entities e ON e.entity_id = t.entity_id
WHERE t.anomaly_flag = TRUE
ORDER BY t.anomaly_score DESC, t.amount_vnd DESC;
