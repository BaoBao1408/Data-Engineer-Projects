"""
Seed script: Insert realistic Vietnamese financial entities + sample data.
Run once after migrations: python scripts/seed_financial_data.py
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import random
import uuid
from datetime import date, datetime, timedelta
from decimal import Decimal

from loguru import logger
from src.ingestion.connectors.sql_connector import get_warehouse

warehouse = get_warehouse()


# ─── Sample Entities (realistic Vietnamese companies) ─────────────────────────
ENTITIES = [
    {
        "entity_code": "VCB",
        "legal_name": "Ngân hàng TMCP Ngoại thương Việt Nam",
        "short_name": "Vietcombank",
        "tax_id": "0100112437",
        "entity_type": "BANK",
        "industry_code": "6412",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hà Nội",
        "stock_exchange": "HOSE",
        "ticker_symbol": "VCB",
        "listing_date": "2009-06-30",
        "incorporation_date": "1963-01-01",
    },
    {
        "entity_code": "VIC",
        "legal_name": "Tập đoàn Vingroup – Công ty CP",
        "short_name": "Vingroup",
        "tax_id": "0101245678",
        "entity_type": "PUBLIC_COMPANY",
        "industry_code": "6810",
        "functional_currency": "VND",
        "reporting_standard": "IFRS",
        "country": "VN",
        "province": "Hà Nội",
        "stock_exchange": "HOSE",
        "ticker_symbol": "VIC",
        "listing_date": "2007-09-19",
        "incorporation_date": "2001-08-03",
    },
    {
        "entity_code": "HPG",
        "legal_name": "Công ty CP Tập đoàn Hòa Phát",
        "short_name": "Hòa Phát Group",
        "tax_id": "0900100062",
        "entity_type": "PUBLIC_COMPANY",
        "industry_code": "2410",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hưng Yên",
        "stock_exchange": "HOSE",
        "ticker_symbol": "HPG",
        "listing_date": "2007-11-15",
        "incorporation_date": "1992-08-08",
    },
    {
        "entity_code": "FPT",
        "legal_name": "Công ty CP FPT",
        "short_name": "FPT Corporation",
        "tax_id": "0101248141",
        "entity_type": "PUBLIC_COMPANY",
        "industry_code": "6201",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hà Nội",
        "stock_exchange": "HOSE",
        "ticker_symbol": "FPT",
        "listing_date": "2006-12-13",
        "incorporation_date": "1988-09-13",
    },
    {
        "entity_code": "BVH",
        "legal_name": "Tập đoàn Bảo Việt",
        "short_name": "Bảo Việt Holdings",
        "tax_id": "0101248142",
        "entity_type": "INSURANCE",
        "industry_code": "6511",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hà Nội",
        "stock_exchange": "HOSE",
        "ticker_symbol": "BVH",
        "listing_date": "2009-07-02",
    },
    {
        "entity_code": "MBB",
        "legal_name": "Ngân hàng TMCP Quân đội",
        "short_name": "MB Bank",
        "tax_id": "0100283873",
        "entity_type": "BANK",
        "industry_code": "6412",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hà Nội",
        "stock_exchange": "HOSE",
        "ticker_symbol": "MBB",
        "listing_date": "2011-01-07",
    },
    {
        "entity_code": "SAMSUNG_VN",
        "legal_name": "Công ty TNHH Samsung Electronics Việt Nam",
        "short_name": "Samsung Vietnam",
        "tax_id": "2300392747",
        "entity_type": "FOREIGN_INVESTED",
        "industry_code": "6201",
        "functional_currency": "USD",
        "reporting_standard": "IFRS",
        "country": "VN",
        "province": "Bắc Ninh",
    },
    {
        "entity_code": "PVN",
        "legal_name": "Tập đoàn Dầu khí Việt Nam",
        "short_name": "PetroVietnam",
        "tax_id": "0100107788",
        "entity_type": "STATE_OWNED",
        "industry_code": "3510",
        "functional_currency": "VND",
        "reporting_standard": "VAS",
        "country": "VN",
        "province": "Hà Nội",
    },
]

# ─── Financial statement line items (Income Statement) ────────────────────────
INCOME_STMT_LINES = [
    ("01", "Doanh thu bán hàng và cung cấp dịch vụ",   "Revenue from goods and services"),
    ("02", "Các khoản giảm trừ doanh thu",               "Deductions from revenue"),
    ("10", "Doanh thu thuần",                             "Net revenue",                ),
    ("11", "Giá vốn hàng bán",                           "Cost of goods sold"),
    ("20", "Giá vốn hàng bán",                           "Cost of goods sold"),
    ("30", "Lợi nhuận gộp về bán hàng và cung cấp DV",  "Gross profit"),
    ("31", "Doanh thu hoạt động tài chính",              "Financial income"),
    ("32", "Chi phí tài chính",                          "Financial expenses"),
    ("33", "Trong đó: Chi phí lãi vay",                  "Of which: Interest expense"),
    ("25", "Chi phí bán hàng",                           "Selling expenses"),
    ("26", "Chi phí quản lý doanh nghiệp",               "G&A expenses"),
    ("30", "Lợi nhuận thuần từ HĐKD",                   "Operating profit (EBIT)"),
    ("40", "Thu nhập khác",                              "Other income"),
    ("41", "Chi phí khác",                               "Other expenses"),
    ("50", "Lợi nhuận khác",                             "Other profit/loss"),
    ("60", "Phần lãi/lỗ trong công ty liên doanh",      "Share of JV profit/loss"),
    ("70", "Tổng lợi nhuận kế toán trước thuế",         "Income before tax (EBT)"),
    ("71", "Chi phí thuế TNDN hiện hành",                "Current income tax"),
    ("72", "Chi phí thuế TNDN hoàn lại",                "Deferred income tax"),
    ("60", "Lợi nhuận sau thuế thu nhập doanh nghiệp",  "Net income after tax"),
    ("61", "Lợi ích của cổ đông thiểu số",              "Non-controlling interests"),
    ("62", "Lợi nhuận sau thuế của cổ đông công ty mẹ", "Net income to equity holders"),
    ("70", "Lãi cơ bản trên cổ phiếu (EPS)",           "Basic EPS"),
]


def seed_entities():
    logger.info("Seeding entities...")
    inserted = 0
    for e in ENTITIES:
        try:
            warehouse.execute("""
                INSERT INTO financial.entities
                    (entity_code, legal_name, short_name, tax_id, entity_type,
                     industry_code, functional_currency, reporting_standard,
                     country, province, stock_exchange, ticker_symbol,
                     listing_date, incorporation_date)
                VALUES
                    (:entity_code, :legal_name, :short_name, :tax_id, :entity_type,
                     :industry_code, :functional_currency, :reporting_standard,
                     :country, :province, :stock_exchange, :ticker_symbol,
                     :listing_date, :incorporation_date)
                ON CONFLICT (entity_code) DO NOTHING
            """, {
                "stock_exchange": e.get("stock_exchange"),
                "ticker_symbol":  e.get("ticker_symbol"),
                "listing_date":   e.get("listing_date"),
                "incorporation_date": e.get("incorporation_date"),
                **{k: e.get(k) for k in [
                    "entity_code", "legal_name", "short_name", "tax_id",
                    "entity_type", "industry_code", "functional_currency",
                    "reporting_standard", "country", "province",
                ]},
            })
            inserted += 1
        except Exception as ex:
            logger.warning(f"Entity {e['entity_code']} skipped: {ex}")

    logger.info(f"Entities seeded: {inserted}")


def seed_fiscal_periods():
    logger.info("Seeding fiscal periods...")
    entities = warehouse.execute("SELECT entity_id FROM financial.entities")
    inserted = 0

    for row in entities:
        eid = str(row["entity_id"])
        for year in [2022, 2023, 2024]:
            for period_type, pnum, start, end in [
                ("ANNUAL", None, f"{year}-01-01", f"{year}-12-31"),
                ("Q1", 1, f"{year}-01-01", f"{year}-03-31"),
                ("Q2", 2, f"{year}-04-01", f"{year}-06-30"),
                ("Q3", 3, f"{year}-07-01", f"{year}-09-30"),
                ("Q4", 4, f"{year}-10-01", f"{year}-12-31"),
            ]:
                try:
                    warehouse.execute("""
                        INSERT INTO financial.fiscal_periods
                            (entity_id, fiscal_year, period_type, period_number,
                             start_date, end_date, status)
                        VALUES (:eid, :year, :ptype, :pnum, :start, :end,
                                CASE WHEN :year < 2024 THEN 'CLOSED' ELSE 'OPEN' END)
                        ON CONFLICT (entity_id, fiscal_year, period_type, period_number)
                        DO NOTHING
                    """, {"eid": eid, "year": year, "ptype": period_type,
                          "pnum": pnum, "start": start, "end": end})
                    inserted += 1
                except Exception:
                    pass

    logger.info(f"Fiscal periods seeded: {inserted}")


def seed_sample_ratios():
    """Seed realistic financial ratios with slight random variation."""
    logger.info("Seeding financial ratios...")

    # Base financials per entity (VND billions)
    BASE_FINANCIALS = {
        "VCB":        dict(revenue=100_000, net_income=20_000, assets=1_800_000, equity=150_000, roe=0.154),
        "VIC":        dict(revenue=140_000, net_income=3_500,  assets=450_000,  equity=80_000,  roe=0.044),
        "HPG":        dict(revenue=130_000, net_income=8_000,  assets=120_000,  equity=60_000,  roe=0.133),
        "FPT":        dict(revenue=52_000,  net_income=6_500,  assets=40_000,   equity=18_000,  roe=0.361),
        "BVH":        dict(revenue=35_000,  net_income=1_500,  assets=160_000,  equity=22_000,  roe=0.068),
        "MBB":        dict(revenue=65_000,  net_income=14_000, assets=700_000,  equity=70_000,  roe=0.200),
        "SAMSUNG_VN": dict(revenue=500_000, net_income=25_000, assets=200_000,  equity=80_000,  roe=0.313),
        "PVN":        dict(revenue=400_000, net_income=30_000, assets=600_000,  equity=200_000, roe=0.150),
    }

    entities = warehouse.execute("""
        SELECT e.entity_id, e.entity_code, fp.period_id, fp.fiscal_year
        FROM financial.entities e
        JOIN financial.fiscal_periods fp ON fp.entity_id = e.entity_id
        WHERE fp.period_type = 'ANNUAL'
        ORDER BY e.entity_code, fp.fiscal_year
    """)

    growth_tracker = {}

    for row in entities:
        code = row["entity_code"]
        base = BASE_FINANCIALS.get(code, dict(
            revenue=10_000, net_income=500, assets=20_000, equity=8_000, roe=0.06
        ))
        year = row["fiscal_year"]
        growth = 1 + (random.uniform(0.05, 0.20) * (year - 2021))
        variation = random.uniform(0.90, 1.10)

        rev   = round(base["revenue"]    * growth * variation * 1_000_000_000, 2)
        ni    = round(base["net_income"] * growth * variation * 1_000_000_000, 2)
        assets = round(base["assets"]   * growth * 1_000_000_000, 2)
        equity = round(base["equity"]   * growth * 1_000_000_000, 2)
        liab   = assets - equity

        prev_rev = growth_tracker.get(code, {}).get("revenue")
        rev_growth = round((rev - prev_rev) / prev_rev, 4) if prev_rev else None
        growth_tracker.setdefault(code, {})["revenue"] = rev

        current_assets = round(assets * random.uniform(0.30, 0.55), 2)
        current_liab   = round(liab * random.uniform(0.45, 0.65), 2)
        cash           = round(current_assets * random.uniform(0.10, 0.25), 2)
        gross_profit   = round(rev * random.uniform(0.18, 0.45), 2)
        ebit           = round(rev * random.uniform(0.08, 0.28), 2)

        try:
            warehouse.execute("""
                INSERT INTO financial.financial_ratios
                    (entity_id, period_id,
                     total_revenue, net_income, total_assets, total_liabilities,
                     total_equity, current_assets, current_liabilities, cash_and_equivalents,
                     gross_profit, ebit, ebitda, operating_cash_flow,
                     current_ratio, quick_ratio, debt_to_equity, debt_to_assets,
                     equity_ratio, net_profit_margin, gross_profit_margin,
                     return_on_assets, return_on_equity, ebitda_margin,
                     asset_turnover, revenue_growth_yoy)
                VALUES
                    (:eid, :pid,
                     :rev, :ni, :assets, :liab,
                     :equity, :ca, :cl, :cash,
                     :gp, :ebit, :ebitda, :ocf,
                     :cr, :qr, :de, :da, :er,
                     :npm, :gpm, :roa, :roe, :em, :at, :rg)
                ON CONFLICT (entity_id, period_id) DO UPDATE
                SET total_revenue = EXCLUDED.total_revenue,
                    net_income = EXCLUDED.net_income,
                    computed_at = NOW()
            """, {
                "eid": str(row["entity_id"]), "pid": str(row["period_id"]),
                "rev": rev, "ni": ni, "assets": assets, "liab": liab,
                "equity": equity, "ca": current_assets, "cl": current_liab, "cash": cash,
                "gp": gross_profit, "ebit": ebit,
                "ebitda": round(ebit * 1.15, 2),
                "ocf": round(ni * random.uniform(1.0, 1.5), 2),
                "cr": round(current_assets / current_liab, 4) if current_liab else None,
                "qr": round((current_assets - cash * 2) / current_liab, 4) if current_liab else None,
                "de": round(liab / equity, 4) if equity else None,
                "da": round(liab / assets, 4) if assets else None,
                "er": round(equity / assets, 4) if assets else None,
                "npm": round(ni / rev, 4) if rev else None,
                "gpm": round(gross_profit / rev, 4) if rev else None,
                "roa": round(ni / assets, 4) if assets else None,
                "roe": round(ni / equity, 4) if equity else None,
                "em":  round(ebit * 1.15 / rev, 4) if rev else None,
                "at":  round(rev / assets, 4) if assets else None,
                "rg":  rev_growth,
            })
        except Exception as ex:
            logger.warning(f"Ratio insert failed for {code} {year}: {ex}")

    logger.info("Financial ratios seeded")


def seed_audit_engagements():
    logger.info("Seeding audit engagements...")
    entities = warehouse.execute("SELECT entity_id, entity_code FROM financial.entities")
    partners = ["Nguyen Van An", "Tran Thi Bich", "Le Minh Duc", "Pham Hong Hai"]
    managers = ["Hoang Thu Huong", "Nguyen Quoc Bao", "Vu Thi Lan", "Do Minh Khoa"]
    opinions = ["UNQUALIFIED", "UNQUALIFIED", "UNQUALIFIED", "QUALIFIED"]

    inserted = 0
    for ent in entities:
        for year in [2022, 2023, 2024]:
            code = f"KPMG-{year}-VN-{ent['entity_code']}"
            try:
                warehouse.execute("""
                    INSERT INTO audit.engagements
                        (engagement_code, entity_id, engagement_type, fiscal_year,
                         period_start, period_end, partner_in_charge, manager,
                         status, audit_opinion, opinion_date, contracted_fee)
                    VALUES
                        (:code, :eid, 'STATUTORY_AUDIT', :year,
                         :start, :end, :partner, :manager,
                         :status, :opinion, :odate, :fee)
                    ON CONFLICT (engagement_code) DO NOTHING
                """, {
                    "code": code, "eid": str(ent["entity_id"]), "year": year,
                    "start": f"{year}-01-01", "end": f"{year}-12-31",
                    "partner": random.choice(partners),
                    "manager": random.choice(managers),
                    "status": "COMPLETED" if year < 2024 else "FIELDWORK",
                    "opinion": random.choice(opinions) if year < 2024 else None,
                    "odate": f"{year+1}-03-31" if year < 2024 else None,
                    "fee": random.choice([500_000_000, 800_000_000, 1_200_000_000, 2_000_000_000]),
                })
                inserted += 1
            except Exception as ex:
                logger.warning(f"Engagement {code} skipped: {ex}")

    logger.info(f"Engagements seeded: {inserted}")


if __name__ == "__main__":
    logger.info("Starting financial data seed...")
    seed_entities()
    seed_fiscal_periods()
    seed_sample_ratios()
    seed_audit_engagements()
    logger.info("✅ Seed complete!")
