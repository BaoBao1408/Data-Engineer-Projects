import random
from datetime import date, timedelta
from sqlalchemy import text

from config.settings import DATA_VOLUME, PROMOTION_TYPES, DISCOUNT_TYPES
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from db.connection import engine
from utils.checkpoint import load_checkpoint, save_checkpoint
from utils.time_helper import random_date_between

PROMO_START_DATE = date(2022, 1, 1)
PROMO_END_DATE = date(2025, 12, 31)


def generate_promotion_discount():
    discount_type = random.choice(DISCOUNT_TYPES)

    if discount_type == "percentage":
        discount_value = random.randint(5, 20)   # realistic %

    else:  # fixed_amount
        discount_value = random.choice([10, 20, 30, 50, 100])

    return discount_type, discount_value

def generate_promotions():

    state = load_checkpoint()

    if state.get("promotion_done"):
        log("Promotions already generated. Skipping.")

        # vẫn phải trả promotion_ids cho step sau
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT promotion_id FROM promotion")
            )
            return [r.promotion_id for r in result]
        
    log("Generating promotions...")

    rows = []   

    for i in range(DATA_VOLUME["promotion"]):
        # start_date trong 2022–2025
        start = random_date_between(
            PROMO_START_DATE,
            PROMO_END_DATE - timedelta(days=7)
        )

        # duration hợp lý
        duration_days = random.randint(7, 60)
        end = min(
            start + timedelta(days=duration_days),
            PROMO_END_DATE
        )

        discount_type, discount_value = generate_promotion_discount()

        rows.append((
            f"Campaign {i}",
            random.choice(PROMOTION_TYPES),
            discount_type,
            discount_value,
            start,
            end,
        ))

    bulk_insert(
        table_name="promotion",
        columns=[
            "promotion_name",
            "promotion_type",
            "discount_type",
            "discount_value",
            "start_date",
            "end_date",
        ],
        rows=rows,
    )

    # 🔥 LẤY promotion_id SAU KHI INSERT
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT promotion_id "
                "FROM promotion "
                "ORDER BY promotion_id DESC LIMIT :n"
            ),
            {"n": len(rows)}
        )
        promotion_ids = [r.promotion_id for r in result][::-1]

    state["promotion_done"] = True
    save_checkpoint(state)

    log(f"Inserted {len(promotion_ids)} promotions")

    return promotion_ids
