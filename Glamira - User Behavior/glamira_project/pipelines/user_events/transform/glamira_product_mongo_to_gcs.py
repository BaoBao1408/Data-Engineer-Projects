import json
import time
from pathlib import Path
from google.cloud import storage
from tqdm import tqdm
from config.mongo_connection import connect_mongo

# ========================
# CONFIG
# ========================
BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/products/"

MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
FLUSH_INTERVAL = 5000

# 👉 RESUME CONFIG
START_FROM = 0    
START_FILE_INDEX = 0   

TEMP_DIR = Path("tmp_products")
TEMP_DIR.mkdir(exist_ok=True)

# ========================
# INIT
# ========================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

mongo_client, db = connect_mongo()
collection = db["products_raw"]

print("Count:", collection.count_documents({}))
print(f"Resume from record: {START_FROM}, file_index: {START_FILE_INDEX}")

# ========================
# HELPER
# ========================
def safe_float(val):
    try:
        if val is None:
            return 0.0
        return float(val)
    except:
        return 0.0


def transform(doc):
    react = doc.get("react_data", {})

    return {
        "_id": str(doc.get("_id")),
        "product_id": doc.get("product_id"),
        "url": doc.get("url"),

        "name": react.get("name"),
        "sku": react.get("sku"),

        "price": safe_float(react.get("price")),
        "min_price": safe_float(react.get("min_price")),
        "max_price": safe_float(react.get("max_price")),

        "category": react.get("category_name"),
        "gender": react.get("gender"),
        "collection": react.get("collection"),

        "react_data": react
    }


def upload_with_retry(blob, file_path, retries=3):
    for i in range(retries):
        try:
            blob.upload_from_filename(file_path, timeout=300)
            print(f"✅ Uploaded {file_path.name}")
            return
        except Exception as e:
            print(f"❌ Upload failed (attempt {i+1}): {e}")
            time.sleep(5)
    raise Exception("Upload failed after retries")


# ========================
# STREAM WRITE
# ========================
file_index = START_FILE_INDEX
current_size = 0


def new_file():
    return TEMP_DIR / f"products_part_{file_index}.jsonl"


file_path = new_file()

if file_path.exists():
    file_path.unlink()

f = open(file_path, "w", encoding="utf-8")

cursor = collection.find({}, no_cursor_timeout=True)

for i, doc in enumerate(tqdm(cursor)):

    if i < START_FROM:
        continue

    try:
        record = transform(doc)
    except Exception as e:
        print(f"❌ Transform error at record {i}: {e}")
        continue

    try:
        line = json.dumps(record, ensure_ascii=False)
    except Exception as e:
        print(f"❌ JSON dump error at record {i}: {e}")
        continue

    f.write(line + "\n")
    current_size += len(line.encode("utf-8"))

    # flush
    if i % FLUSH_INTERVAL == 0:
        f.flush()

    # rotate file
    if current_size >= MAX_FILE_SIZE:
        f.close()

        blob = bucket.blob(f"{PREFIX}{file_path.name}")
        blob.chunk_size = 5 * 1024 * 1024  # 5MB
        upload_with_retry(blob, file_path)

        file_index += 1
        file_path = new_file()

        if file_path.exists():
            file_path.unlink()

        f = open(file_path, "w", encoding="utf-8")
        current_size = 0


# close last file
f.close()

if current_size > 0:
    blob = bucket.blob(f"{PREFIX}{file_path.name}")
    blob.chunk_size = 5 * 1024 * 1024
    upload_with_retry(blob, file_path)

print("DONE 🚀")