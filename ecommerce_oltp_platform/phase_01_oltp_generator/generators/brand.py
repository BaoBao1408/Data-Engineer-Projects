from config.settings import BRANDS_BY_CATEGORY, DATA_VOLUME
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from generators.base import faker
from sqlalchemy import text
from db.connection import engine
import random
from utils.checkpoint import load_checkpoint, save_checkpoint   

def generate_brands(category_id_map: dict):
    """
    Return:
        brand_ids_by_category: { parent_category: [brand_id] }
        brand_id_to_name: { brand_id: brand_name }
    """
    state = load_checkpoint()
    if state.get("brand_done"):
        log("Brands already generated. Skipping.")

        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT brand_id, brand_name FROM brand")
            )
            brand_id_to_name = {r.brand_id: r.brand_name for r in result}

        brand_ids_by_category = {}
        for parent in BRANDS_BY_CATEGORY.keys():
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT b.brand_id
                        FROM brand b
                        JOIN product p ON p.brand_id = b.brand_id
                        JOIN category c ON p.category_id = c.category_id
                        WHERE c.level = 2
                    """)
                )
                brand_ids_by_category[parent] = [r.brand_id for r in result]

        return brand_ids_by_category, brand_id_to_name
    
    log("Generating brands...")

    rows = []
    brand_category = []
    brand_names = []

    all_parent_categories = list(BRANDS_BY_CATEGORY.keys())

    for _ in range(DATA_VOLUME["brand"]):
        parent = random.choice(all_parent_categories)
        base_name = random.choice(BRANDS_BY_CATEGORY[parent])
        brand_name = f"{base_name} {faker.company_suffix()}"

        rows.append((
            brand_name,
            faker.country()[:50],
            faker.date_time_this_decade()
        ))

        brand_category.append(parent)
        brand_names.append(brand_name)

    bulk_insert(
        table_name="brand",
        columns=["brand_name", "country", "created_at"],
        rows=rows
    )

    # ---- lấy brand_id vừa insert ----
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT brand_id FROM brand ORDER BY brand_id DESC LIMIT :n"),
            {"n": len(rows)}
        )
        brand_ids = [r.brand_id for r in result][::-1]

    brand_ids_by_category = {}
    brand_id_to_name = {}

    for bid, parent, name in zip(brand_ids, brand_category, brand_names):
        brand_ids_by_category.setdefault(parent, []).append(bid)
        brand_id_to_name[bid] = name


    state["brand_done"] = True
    save_checkpoint(state)

    log(f"Inserted {len(brand_ids)} brands")
    
    return brand_ids_by_category, brand_id_to_name

