import random
from datetime import timedelta
from sqlalchemy import text
from utils.checkpoint import load_checkpoint, save_checkpoint
from db.connection import engine
from config.settings import DATA_VOLUME, ORDER_STATUSES, START_YEAR, END_YEAR, BATCH_SIZE
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from utils.time_helper import random_datetime
from tqdm import tqdm

def generate_orders(seller_ids_by_category: dict):
    log("Generating orders (resumable)...")

    seller_ids = [
        sid for v in seller_ids_by_category.values() for sid in v
    ]

    state = load_checkpoint()

    total_orders = DATA_VOLUME["order"]
    batch_size = BATCH_SIZE["order"]

    start_index = state["last_order_index"]

    all_order_ids = []

    pbar = tqdm(
    total=total_orders,
    initial=start_index,
    desc="Generating orders",
    unit="orders",
    )

    for offset in range(start_index, total_orders, batch_size):
        rows = []
        current_batch = min(
            batch_size,
            total_orders - offset
        )

        for _ in range(current_batch):
            order_date = random_datetime(START_YEAR, END_YEAR)

            rows.append((
                order_date,
                random.choice(seller_ids),
                random.choice(ORDER_STATUSES),
                0.0,
                order_date + timedelta(seconds=random.randint(30, 1800)),
            ))

        bulk_insert(
            table_name="orders",
            columns=[
                "order_date",
                "seller_id",
                "status",
                "total_amount",
                "created_at",
            ],
            rows=rows,
        )
        # ===== update checkpoint =====
        state["orders_generated"] += len(rows)
        state["last_order_index"] += len(rows)
        save_checkpoint(state)

        # === update progress bar ===
        pbar.update(len(rows))

        # lấy order_ids của chunk
        with engine.connect() as conn:
            result = conn.execute(
                text("""
                    SELECT order_id
                    FROM orders
                    ORDER BY order_id DESC
                    LIMIT :n
                """),
                {"n": len(rows)},
            )
            chunk_order_ids = [
                r.order_id for r in result
            ][::-1]

        all_order_ids.extend(chunk_order_ids)

    log(f"Inserted {len(all_order_ids)} orders")
    return all_order_ids


# def generate_orders(seller_ids_by_category: dict):
#     log("Generating orders...")

#     # flatten seller_ids
#     seller_ids = [
#         sid for v in seller_ids_by_category.values() for sid in v
#     ]

#     rows = []

#     for _ in range(DATA_VOLUME["order"]):
#         order_date = random_datetime(START_YEAR, END_YEAR)

#         rows.append((
#             order_date,
#             random.choice(seller_ids),
#             random.choice(ORDER_STATUSES),
#             0.0,
#             order_date + timedelta(seconds=random.randint(30, 1800)),
#         ))

#     bulk_insert(
#         table_name="orders",
#         columns=[
#             "order_date",
#             "seller_id",
#             "status",
#             "total_amount",
#             "created_at",
#         ],
#         rows=rows,
#     )

#     # lấy order_id sau insert
#     with engine.connect() as conn:
#         result = conn.execute(
#             text(
#                 "SELECT order_id FROM orders "
#                 "ORDER BY order_id DESC LIMIT :n"
#             ),
#             {"n": len(rows)},
#         )
#         order_ids = [r.order_id for r in result][::-1]

#     log(f"Inserted {len(order_ids)} orders")
#     return order_ids
