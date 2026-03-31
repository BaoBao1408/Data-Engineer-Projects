import random
from config.settings import DATA_VOLUME, SELLER_TYPES, CATEGORY_TREE
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from generators.base import faker
from sqlalchemy import text
from db.connection import engine
from utils.checkpoint import load_checkpoint, save_checkpoint

SELLER_DISTRIBUTION = {
    "Electronics": 0.35,
    "Fashion": 0.25,
    "Grocery": 0.30,
    "Home": 0.10,
}


# ------------------ helpers ------------------

def validate_distribution(dist: dict):
    total = round(sum(dist.values()), 2)
    if total != 1.0:
        raise ValueError(
            f"SELLER_DISTRIBUTION must sum to 1.0, got {total}"
        )


def choose_marketplace_categories(main_category, all_categories):
    """
    Marketplace sellers:
    - 60% sell 1–2 categories
    - 40% sell 2–3 categories
    """
    r = random.random()
    if r < 0.6:
        k = random.randint(1, 2)
    else:
        k = random.randint(2, min(3, len(all_categories)))

    cats = {main_category}
    while len(cats) < k:
        cats.add(random.choice(all_categories))

    return list(cats)


# ------------------ main ------------------

def generate_sellers():
    """
    Return:
        {
          "Electronics": [seller_id,...],
          "Grocery": [...],
          ...
        }
    """
    state = load_checkpoint()
    if state.get("seller_done"):
        log("Sellers already generated. Skipping.")

        seller_ids_by_category = {}
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT seller_id, seller_type FROM seller")
            )
            seller_ids = [r.seller_id for r in result]

        # map đơn giản (đã có data rồi)
        for parent in CATEGORY_TREE.keys():
            seller_ids_by_category[parent] = seller_ids

        return seller_ids_by_category
    
    log("Generating sellers...")

    validate_distribution(SELLER_DISTRIBUTION)

    rows = []
    seller_categories = []

    parent_categories = list(CATEGORY_TREE.keys())

    # weighted category choice
    categories = list(SELLER_DISTRIBUTION.keys())
    weights = list(SELLER_DISTRIBUTION.values())

    for i in range(DATA_VOLUME["seller"]):
        seller_type = random.choice(SELLER_TYPES)

        main_category = random.choices(categories, weights=weights, k=1)[0]

        if seller_type == "Official":
            assigned_categories = [main_category]
        else:
            assigned_categories = choose_marketplace_categories(
                main_category,
                parent_categories
            )

        rows.append((
            f"{faker.company()} Shop {i}",
            faker.date_between("-3y", "today"),
            seller_type,
            round(random.uniform(3.5, 5.0), 1),
            "Vietnam"
        ))

        seller_categories.append(assigned_categories)

    bulk_insert(
        table_name="seller",
        columns=[
            "seller_name",
            "join_date",
            "seller_type",
            "rating",
            "country",
        ],
        rows=rows
    )

    # ---- fetch seller ids ----
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT seller_id "
                "FROM seller "
                "ORDER BY seller_id DESC "
                "LIMIT :n"
            ),
            {"n": len(rows)}
        )
        seller_ids = [r.seller_id for r in result][::-1]

    # ---- map seller -> category ----
    seller_ids_by_category = {}
    for sid, cats in zip(seller_ids, seller_categories):
        for c in cats:
            seller_ids_by_category.setdefault(c, []).append(sid)


    state["seller_done"] = True
    save_checkpoint(state)

    log(f"Inserted {len(seller_ids)} sellers")
    
    return seller_ids_by_category
