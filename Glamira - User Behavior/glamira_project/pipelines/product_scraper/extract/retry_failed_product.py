from pymongo import MongoClient
import json
from pathlib import Path


# =========================
# CONFIG
# =========================

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "glamira"   # ⚠️ sửa đúng DB

OUTPUT_DIR = Path("data/export")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# CONNECT
# =========================

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


# =========================
# SAFE JSON
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
# STREAM EXPORT
# =========================

def export_stream(collection_name, output_file, transform=None):

    collection = db[collection_name]

    count = 0

    print(f"🚀 Start export {collection_name}")

    with open(output_file, "w", encoding="utf-8") as f:

        # ⚡ cursor streaming (không load RAM)
        cursor = collection.find({}, {"_id": 0}).batch_size(1000)

        for doc in cursor:

            if transform:
                doc = transform(doc)

            # ✅ ghi ngay từng dòng
            f.write(json_safe(doc) + "\n")

            count += 1

            # log nhẹ tránh lag
            if count % 10000 == 0:
                print(f"{collection_name}: {count}")

    print(f"✅ DONE {collection_name}: {count}")


# =========================
# MAIN
# =========================

def main():

    # 1. raw event (41M rows)
    export_stream(
        "glamira_raw",
        OUTPUT_DIR / "glamira_raw.jsonl"
    )

    # 2. product (18k rows)
    export_stream(
        "products_raw",
        OUTPUT_DIR / "products_flat.jsonl",
        transform=flatten_product
    )


if __name__ == "__main__":
    main()