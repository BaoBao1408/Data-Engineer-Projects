from config.settings import CATEGORY_TREE
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from generators.base import faker
from sqlalchemy import text
from db.connection import engine
from utils.checkpoint import load_checkpoint, save_checkpoint

def generate_categories():

    state = load_checkpoint()
    if state.get("category_done"):
        log("Categories already generated. Skipping.")
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT category_id, category_name FROM category")
            )
            return {r.category_name: r.category_id for r in result}
        
    log("Generating categories...")

    # ---------- LEVEL 1 ----------
    rows = []
    for parent in CATEGORY_TREE.keys():
        rows.append((parent, None, 1, faker.date_time_this_year()))

    bulk_insert(
        table_name="category",
        columns=["category_name", "parent_category_id", "level", "created_at"],
        rows=rows
    )

    # LẤY ID LEVEL 1
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT category_id, category_name FROM category WHERE level = 1")
        )
        category_id_map = {row.category_name: row.category_id for row in result}

    # ---------- LEVEL 2 ----------
    rows = []
    for parent, children in CATEGORY_TREE.items():
        parent_id = category_id_map[parent]
        for child in children:
            rows.append((child, parent_id, 2, faker.date_time_this_year()))

    bulk_insert(
        table_name="category",
        columns=["category_name", "parent_category_id", "level", "created_at"],
        rows=rows
    )

    # LẤY FULL MAP
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT category_id, category_name FROM category")
        )
        category_id_map = {row.category_name: row.category_id for row in result}


    state["category_done"] = True
    save_checkpoint(state)

    log(f"Inserted {len(category_id_map)} categories")
    
    return category_id_map
