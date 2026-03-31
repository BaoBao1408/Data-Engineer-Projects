import random
from config.settings import (
    CATEGORY_TREE,
    DATA_VOLUME,
    CATEGORY_PRICE_RANGE,
)
from loaders.bulk_insert import bulk_insert
from utils.logger import log
from generators.base import faker
from sqlalchemy import text
from db.connection import engine
from utils.checkpoint import load_checkpoint, save_checkpoint

# ================== DISTRIBUTION ==================

CATEGORY_DISTRIBUTION = {
    "Electronics": 0.35,
    "Fashion": 0.25,
    "Grocery": 0.30,
    "Home": 0.10,
}


# ================== HELPERS ==================

def validate_distribution(dist: dict):
    total = round(sum(dist.values()), 2)
    if total != 1.0:
        raise ValueError(
            f"CATEGORY_DISTRIBUTION must sum to 1.0, got {total}"
        )


def get_parent_category(child_name: str) -> str:
    for parent, children in CATEGORY_TREE.items():
        if child_name in children:
            return parent
    raise ValueError(f"Invalid level-2 category: {child_name}")


def generate_product_name(brand: str, category: str) -> str:
    """Generate realistic e-commerce product names"""
    state = load_checkpoint()
    if state.get("product_done"):
        log("Products already generated. Skipping.")

        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT product_id FROM product")
            )
            return [r.product_id for r in result]
    if category in ["Smartphone", "Laptop", "Tablet"]:
        return (
            f"{brand} {category} "
            f"{random.choice(['Pro', 'Plus', 'Max'])} "
            f"{random.choice([64, 128, 256])}GB"
        )

    if category in ["Milk", "Coffee", "Rice"]:
        return f"{brand} {category} {random.choice([500, 1000, 2000])}g"

    if category in ["T-Shirt", "Jeans", "Sneakers"]:
        return f"{brand} {category} {random.choice(['Classic', 'Slim Fit', 'Premium'])}"

    if category in ["Chair", "Table", "Lamp"]:
        return f"{brand} {random.choice(['Wooden', 'Modern', 'Minimal'])} {category}"

    return f"{brand} {category}"


# ================== MAIN ==================

def generate_products(
    category_ids: dict,
    brand_ids_by_category: dict,
    brand_id_to_name: dict,
    seller_ids_by_category: dict,
):
    """
    category_ids: {category_name -> category_id}
    brand_ids_by_category: {parent_category -> [brand_id]}
    brand_id_to_name: {brand_id -> brand_name}
    seller_ids_by_category: {parent_category -> [seller_id]}
    """

    state = load_checkpoint()
    if state.get("product_done"):
        log("Products already generated. Skipping.")

        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT product_id FROM product")
            )
            return [r.product_id for r in result]
        
    log("Generating products...")
    validate_distribution(CATEGORY_DISTRIBUTION)

    rows = []

    # ----- LEVEL 2 categories with proper weights -----
    level_2_categories = []
    weights = []

    for parent, children in CATEGORY_TREE.items():
        per_child_weight = CATEGORY_DISTRIBUTION[parent] / len(children)
        for child in children:
            level_2_categories.append(child)
            weights.append(per_child_weight)

    # ----- generate products -----
    for _ in range(DATA_VOLUME["product"]):
        cat_name = random.choices(level_2_categories, weights=weights, k=1)[0]
        cat_id = category_ids[cat_name]

        parent_cat = get_parent_category(cat_name)

        price_min, price_max, max_discount = CATEGORY_PRICE_RANGE[parent_cat]

        price = round(random.uniform(price_min, price_max), 2)
        discount = round(
            max(price - random.uniform(0, max_discount), price_min),
            2
        )

        brand_id = random.choice(brand_ids_by_category[parent_cat])
        brand_name = brand_id_to_name[brand_id]
        seller_id = random.choice(seller_ids_by_category[parent_cat])

        rows.append((
            generate_product_name(
                brand=brand_name,      # ✅ FIX CHUẨN
                category=cat_name
            ),
            cat_id,
            brand_id,
            seller_id,
            price,
            discount,
            random.randint(0, 500),
            round(random.uniform(3.5, 5.0), 1),
            faker.date_time_between("-3y"),
            True
        ))

    bulk_insert(
        table_name="product",
        columns=[
            "product_name",
            "category_id",
            "brand_id",
            "seller_id",
            "price",
            "discount_price",
            "stock_qty",
            "rating",
            "created_at",
            "is_active",
        ],
        rows=rows
    )

    with engine.connect() as conn:
        result = conn.execute(text("SELECT product_id FROM product"))
        product_ids = [r.product_id for r in result]


    state["product_done"] = True
    save_checkpoint(state)
    
    log(f"Inserted {len(product_ids)} products")

    return product_ids
