import json
import gzip
from pathlib import Path
from pymongo import MongoClient


# =========================
# CONFIG
# =========================

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "glamira"

OUTPUT_DIR = Path("data/export")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# CONNECT
# =========================

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# =========================
# JSON SAFE (fix ObjectId)
# =========================

def json_safe(doc):
    return json.dumps(doc, default=str, ensure_ascii=False)


# =========================
# FLATTEN PRODUCT
# =========================

def flatten_product(doc):

    react = doc.get("react_data", {})

    return {
        "product_id": doc.get("product_id"),
        "url": doc.get("url"),
        "name": react.get("name"),
        "price": react.get("price"),
        "category": react.get("category_name"),
        "collection": react.get("collection"),
        "gender": react.get("gender"),
        "min_price": react.get("min_price"),
        "max_price": react.get("max_price")
    }


# =========================
# GENERIC EXPORT (STREAM + GZIP)
# =========================

def export_stream(collection_name, output_file, transform=None):

    collection = db[collection_name]

    count = 0

    print(f"🚀 Start export: {collection_name}")

    with gzip.open(output_file, "wt", encoding="utf-8") as f:

        cursor = collection.find({}, {"_id": 0}).batch_size(1000)

        for doc in cursor:

            if transform:
                doc = transform(doc)

            f.write(json_safe(doc) + "\n")

            count += 1

            if count % 10000 == 0:
                print(f"{collection_name}: {count}")
                f.flush()

    print(f"✅ DONE {collection_name}: {count}")


# =========================
# MAIN
# =========================

def main():

    # 1️⃣ Export raw user behavior
    export_stream(
        collection_name="glamira_raw",
        output_file=OUTPUT_DIR / "glamira_raw.jsonl.gz"
    )

    # 2️⃣ Export product (flatten)
    export_stream(
        collection_name="products_raw",
        output_file=OUTPUT_DIR / "products_flat.jsonl.gz",
        transform=flatten_product
    )


if __name__ == "__main__":
    main()