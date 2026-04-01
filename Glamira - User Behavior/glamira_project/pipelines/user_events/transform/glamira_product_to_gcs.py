import gzip
import json
from tqdm import tqdm
from google.cloud import storage

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/products_raw.jsonl.gz"

BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/products/"

MAX_FILE_SIZE = 500 * 1024 * 1024
FLUSH_INTERVAL = 10000

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# ========================
# HELPERS
# ========================

def safe_str(x):
    if x is None:
        return None
    return str(x)


def safe_float(x):
    if x is None:
        return None
    try:
        if isinstance(x, str):
            x = x.replace(",", "").strip()
        return float(x)
    except:
        return None


def clean_text(x):
    if isinstance(x, str):
        return (
            x.replace("\u202f", " ")
             .replace("\x00", "")
             .strip()
        )
    return x


def deep_clean(obj):
    """Clean recursive toàn bộ object"""
    if isinstance(obj, dict):
        return {str(k): deep_clean(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [deep_clean(v) for v in obj]
    else:
        return clean_text(obj)


def to_json_string(obj):
    try:
        return json.dumps(obj, ensure_ascii=False)
    except:
        return None


def is_valid_record(r):
    """Hard validation trước khi ghi"""
    return (
        r.get("product_id") is not None and
        isinstance(r.get("product_id"), str)
    )


# ========================
# TRANSFORM
# ========================
def transform(record):
    try:
        react = record.get("react_data", {})
        react_clean = deep_clean(react)

        product_id = safe_str(record.get("product_id"))

        result = {
            # ========================
            # DIM LEVEL
            # ========================
            "product_id": product_id,
            "url": clean_text(record.get("url")),

            "name": react_clean.get("name"),
            "sku": react_clean.get("sku"),
            "category_name": react_clean.get("category_name"),
            "product_type": react_clean.get("product_type"),
            "collection": react_clean.get("collection"),
            "store_code": react_clean.get("store_code"),
            "gender": react_clean.get("gender"),

            "price": safe_float(react_clean.get("price")),
            "min_price": safe_float(react_clean.get("min_price")),
            "max_price": safe_float(react_clean.get("max_price")),

            # ========================
            # RAW JSON (FULL TRACE)
            # ========================
            "react_data_json": to_json_string(react_clean)
        }

        if not is_valid_record(result):
            return None

        return result

    except Exception:
        return None


# ========================
# INIT
# ========================
file_index = 1
current_size = 0

blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")
gcs_file = blob.open("w", encoding="utf-8")

print(f"🚀 Start upload → {PREFIX}")

bad_count = 0
total = 0

# debug sample
sample_print = 0

# ========================
# MAIN
# ========================
with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:
    for i, line in enumerate(tqdm(f, desc="Processing Products")):

        total += 1

        try:
            record = json.loads(line)
        except:
            bad_count += 1
            continue

        transformed = transform(record)

        if not transformed:
            bad_count += 1
            continue

        try:
            json_line = json.dumps(transformed, ensure_ascii=False) + "\n"
        except:
            bad_count += 1
            continue

        # DEBUG 5 dòng đầu
        if sample_print < 5:
            print("SAMPLE:", json_line[:200])
            sample_print += 1

        byte_size = len(json_line.encode("utf-8"))

        # rotate file
        if current_size + byte_size > MAX_FILE_SIZE:
            gcs_file.close()
            print(f"✅ Uploaded part_{file_index}")

            file_index += 1
            current_size = 0

            blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")
            gcs_file = blob.open("w", encoding="utf-8")

        gcs_file.write(json_line)
        current_size += byte_size

        if i % FLUSH_INTERVAL == 0:
            gcs_file.flush()

# close
gcs_file.close()

print(f"\n🎉 DONE PRODUCT PIPELINE")
print(f"Total: {total}")
print(f"Bad: {bad_count}")
print(f"Kept: {total - bad_count}")