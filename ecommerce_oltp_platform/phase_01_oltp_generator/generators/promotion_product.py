import random
from datetime import datetime, timezone
from sqlalchemy import text
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from utils.checkpoint import load_checkpoint, save_checkpoint
from utils.time_helper import random_datetime_between
from db.connection import engine

def generate_promotion_products(
    product_ids: list[int],
    promotion_ids: list[int],
):
    """
    product_ids    : List[int]
    promotion_ids  : List[int]
    """

    state = load_checkpoint()

    if state.get("promotion_product_done"):
        log("Promotion-product mapping already generated. Skipping.")
        return
    
    log("Generating promotion-product mapping...")

    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT promotion_id, start_date, end_date
                FROM promotion
            """)
        )
        promo_date_map = {
            r.promotion_id: (r.start_date, r.end_date)
            for r in result
        }

    rows = []

    for promo_id in promotion_ids:
        start_date, end_date = promo_date_map[promo_id]

        num_products = random.randint(20, 80)
        sampled_products = random.sample(
            product_ids,
            k=min(num_products, len(product_ids))
        )

        for product_id in sampled_products:
            rows.append((
                promo_id,
                product_id,
                random_datetime_between(start_date, end_date),
            ))
            
    bulk_insert(
        table_name="promotion_product",
        columns=[
            "promotion_id",
            "product_id",
            "created_at",
        ],
        rows=rows,
    )
    
    state["promotion_product_done"] = True
    save_checkpoint(state)

    log(f"Inserted {len(rows)} promotion-product rows")