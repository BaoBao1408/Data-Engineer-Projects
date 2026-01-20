# ===============================
# GLOBAL PROJECT SETTINGS
# ===============================

# ---------- DATA VOLUME ----------
DATA_VOLUME = {
    "brand": 100,
    "category_level_1": 10,
    "category_level_2_per_parent": 4,
    "seller": 189,
    "product": 1_055,
    "promotion": 499,
    "promotion_product": 299_000,
    "order": 3_806_003,
    "avg_order_items": 3.5,     
}

# ---------- BATCH SIZE ----------
BATCH_SIZE = {
    "order": 20_000,
    "order_item": 20_000
}

# ---------- BUSINESS LOGIC ----------
CATEGORY_TREE = {
    "Electronics": ["Smartphone", "Laptop", "Tablet"],
    "Fashion": ["T-Shirt", "Jeans", "Sneakers"],
    "Home": ["Chair", "Table", "Lamp"],
    "Grocery": ["Rice", "Coffee", "Milk"],
}

CATEGORY_PRICE_RANGE = {
    "Electronics": (500, 2000, 400),
    "Fashion": (20, 200, 80),
    "Home": (50, 800, 300),
    "Grocery": (2, 20, 8),
}
BRANDS_BY_CATEGORY = {
    "Electronics": ["Apple", "Samsung", "Sony"],
    "Fashion": ["Nike", "Adidas"],
    "Home": ["IKEA"],
    "Grocery": ["Nestle"],
}
SELLER_TYPES = ["Official", "Marketplace"]
ORDER_STATUSES = [
    "PLACED",
    "PAID",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
]

DISCOUNT_TYPES = ["percentage", "fixed_amount"]

PROMOTION_TYPES = [
    "product",
    "category",
    "seller",
    "flash_sale",
]

# ---------- TIME RANGE ----------
START_YEAR = 2022
END_YEAR = 2025
