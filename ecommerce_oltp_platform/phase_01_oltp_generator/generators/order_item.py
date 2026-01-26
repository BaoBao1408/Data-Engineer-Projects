import random
from sqlalchemy import text
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from config.settings import BATCH_SIZE
from utils.checkpoint import load_checkpoint, save_checkpoint
from db.connection import engine
from tqdm import tqdm

def load_order_metadata():
    log("Loading order_date & created_at from orders...")
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT order_id, order_date, created_at
            FROM orders
        """)).fetchall()

    return {
        r.order_id: {
            "order_date": r.order_date,
            "created_at": r.created_at,
        }
        for r in rows
    }

def generate_order_items(order_ids: list[int], product_ids: list[int]):
    log("Generating order items (resumable)...")

    state = load_checkpoint()
    start_idx = state.get("order_items_order_idx", 0)

    batch_size = BATCH_SIZE["order_item"]

    order_meta = load_order_metadata()

    pbar = tqdm(
        total=len(order_ids),
        initial=start_idx,
        desc="Generating order_items",
        unit="orders",
    )

    for offset in range(start_idx, len(order_ids), batch_size):
        rows = []
        current_batch = min(
            batch_size,
            len(order_ids) - offset
        )

        for i in range(current_batch):
            order_id = order_ids[offset + i]

            meta = order_meta[order_id]
            order_date = meta["order_date"]
            created_at = meta["created_at"]

            num_items = random.randint(2, 5)

            sampled_products = random.sample(
                product_ids,
                k=min(num_items, len(product_ids))
            )

            for product_id in sampled_products:
                qty = random.randint(1, 3)
                unit_price = round(random.uniform(10, 200), 2)

                rows.append((
                    order_id,
                    product_id,
                    order_date,
                    qty,
                    unit_price,
                    round(qty * unit_price, 2),
                    created_at,
                ))

        bulk_insert(
            table_name="order_item",
            columns=[
                "order_id",
                "product_id",
                "order_date",
                "quantity",
                "unit_price",
                "subtotal",
                "created_at",
            ],
            rows=rows,
        )

        # checkpoint
        state["order_items_generated"] += len(rows)
        state["order_items_order_idx"] += current_batch
        save_checkpoint(state)

        pbar.update(current_batch)

    log(
        f"Total order items generated: "
        f"{state['order_items_generated']}"
    )

# def generate_order_items(order_ids: list[int], product_ids: list[int]):
#     """
#     order_ids  : List[int]
#     product_ids: List[int]
#     """

#     log("Generating order items...")

#     rows = []

#     for order_id in order_ids:
#         # số item / order theo phân phối thực tế
#         num_items = max(
#             1,
#             int(random.gauss(DATA_VOLUME["avg_order_items"], 0.7))
#         )

#         sampled_products = random.sample(
#             product_ids,
#             k=min(num_items, len(product_ids))
#         )

#         for product_id in sampled_products:
#             qty = random.randint(1, 3)
#             unit_price = round(random.uniform(10, 200), 2)

#             rows.append((
#                 order_id,
#                 product_id,
#                 qty,
#                 unit_price,
#                 round(qty * unit_price, 2),
#             ))

#     bulk_insert(
#         table_name="order_item",
#         columns=[
#             "order_id",
#             "product_id",
#             "quantity",
#             "unit_price",
#             "subtotal",
#         ],
#         rows=rows,
#     )

#     log(f"Inserted {len(rows)} order items")
