import json
import re
from tqdm import tqdm
from google.cloud import storage
from config.mongo_connection import connect_mongo
import time
# =========================
# CONFIG
# =========================
BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/product/"

MAX_FILE_SIZE = 100 * 1024 * 1024
FLUSH_INTERVAL = 1000
CHECKPOINT_FILE = "checkpoint_product.txt"

client, db = connect_mongo()
collection = db["products_raw"]

storage_client = storage.Client()
bucket = storage_client.bucket(BUCKET_NAME)

# =========================
# UTILS (FIX FULL DATA ISSUE)
# =========================

CURRENCY_MAP = {
    "glpl": "PLN",
    "glro": "RON",
    "glfr": "EUR",
    "glgb": "GBP",
    "glus": "USD",
    "glvn": "VND",
    "glde": "EUR",
    "glit": "EUR",
    "glnl": "EUR",
    "glcz": "CZK",
    "glsk": "EUR",
    "glhu": "HUF",
    "glse": "SEK",
    "gldk": "DKK",
    "glno": "NOK",
    "glae": "AED",
    "glaz": "AZN",
    "glaz_en": "AZN",
    "glbo": "BOB",
    "glch": "CHF",
    "glch_fr": "CHF",
    "glch_it": "CHF",
    "glcl": "CLP",
    "glcn": "CNY",
    "glcr": "CRC",
    "glgt": "GTQ",
    "glhn": "HNL",
    "glin": "INR",
    "glis": "ISK",
    "gljp": "JPY",
    "glkr": "KRW",
    "glmd": "MDL",
    "glmx": "MXN",
    "glmy": "MYR",
    "glpa": "USD",
    "glpe": "PEN",
    "glph": "PHP",
    "glrs": "RSD",
    "glza": "ZAR",
}

def extract_currency(text):
    if not text:
        return None

    text = str(text)

    if "€" in text:
        return "EUR"
    if "£" in text:
        return "GBP"
    if "$" in text:
        return "USD"

    return None

def safe_json(x):
    if isinstance(x, str):
        x = x.strip()
        if x.startswith("{") or x.startswith("["):
            try:
                return json.loads(x)
            except:
                return {}
    return x


def safe_str(x):
    if x in [None, "", "null", "None"]:
        return None
    return str(x)


def parse_float(x):
    if x in [None, "", "null", "None"]:
        return None
    try:
        x = re.sub(r"[^\d.]", "", str(x))
        return float(x) if x else None
    except:
        return None


def ensure_dict(x):
    if isinstance(x, dict):
        return x
    return {}

# =========================
# WRITER
# =========================

class Writer:
    def __init__(self, prefix):
        self.prefix = prefix
        self.buffer = []
        self.size = 0
        self.index = 0

    def write(self, record):
        line = json.dumps(record, ensure_ascii=False) + "\n"
        self.buffer.append(line)
        self.size += len(line)

        if self.size >= MAX_FILE_SIZE or len(self.buffer) >= FLUSH_INTERVAL:
            self.flush()

    def flush(self):
        if not self.buffer:
            return

        blob = bucket.blob(
            f"{self.prefix}part_{int(time.time())}_{self.index}.jsonl")
        blob.upload_from_string("".join(self.buffer))

        print(f"✅ Uploaded {blob.name}")

        self.buffer = []
        self.size = 0
        self.index += 1


# =========================
# CHECKPOINT
# =========================

def load_checkpoint():
    try:
        with open(CHECKPOINT_FILE) as f:
            return int(f.read())
    except:
        return 0


def save_checkpoint(i):
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(str(i))


# =========================
# TRANSFORM PRODUCT (FIX FULL)
# =========================

def transform_product(doc):
    react = ensure_dict(safe_json(doc.get("react_data")))

    product_id = safe_str(
        react.get("product_id") or doc.get("product_id")
    )

    store_code = react.get("store_code")

    currency = CURRENCY_MAP.get(store_code)

    if not currency:
        currency = extract_currency(
            react.get("min_price_format")
            or react.get("max_price_format")
        )

    # =====================
    # FALLBACK PRICE (IMPORTANT)
    # =====================
    product_price = ensure_dict(react.get("product_price"))
    final_price = (
        ensure_dict(product_price.get("prices"))
        .get("finalPrice", {})
        .get("amount")
    )

    return {
        "product_id": product_id,
        "name": safe_str(react.get("name")),
        "sku": safe_str(react.get("sku")),
        "category_name": safe_str(
            react.get("category_name") or react.get("attribute_set")
        ),
        "product_type": safe_str(react.get("product_type")),
        "collection": safe_str(react.get("collection")),
        "store_code": safe_str(react.get("store_code")),
        "gender": safe_str(react.get("gender")),

        # ===== PRICE LOGIC =====
        "price": parse_float(
            final_price or react.get("price")
        ),
        "min_price": parse_float(react.get("min_price")),
        "max_price": parse_float(react.get("max_price")),

        # ===== FIXED =====
        "currency": currency
    }


# =========================
# MAIN
# =========================

def run():
    start = load_checkpoint()
    cursor = collection.find({}, no_cursor_timeout=True).skip(start)

    writer = Writer(PREFIX)

    for i, doc in enumerate(tqdm(cursor, initial=start), start=start):
        try:
            record = transform_product(doc)

            # skip nếu product_id null
            if not record["product_id"]:
                continue

            writer.write(record)

            if i % 1000 == 0:
                save_checkpoint(i)

        except Exception as e:
            print(f"❌ PRODUCT ERROR at {i}: {e}")

    writer.flush()
    save_checkpoint(i)
    print("🎯 DONE PRODUCT")


if __name__ == "__main__":
    run()