import gzip
import json
from tqdm import tqdm
from datetime import datetime
from google.cloud import storage

# ========================
# CONFIG
# ========================
INPUT_FILE = "data/export/glamira_raw.jsonl.gz"

BUCKET_NAME = "glamira-data-lake-qb"
PREFIX = "raw/glamira_upgrade_2/"

CHUNK_SIZE = 100_000
FLUSH_INTERVAL = 1000

# ========================
# GCS
# ========================
client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

# ========================
# TRANSFORM
# ========================
def transform(record):
    ts = record.get("time_stamp")
    if not ts:
        return None

    base = {
        "event_id": str(record.get("_id")),
        "event_time": datetime.fromtimestamp(ts).isoformat(),
        "event_type": record.get("collection"),

        # user
        "user_id": record.get("user_id_db"),
        "session_id": record.get("device_id"),
        "email_address": record.get("email_address"),

        # device
        "ip": record.get("ip"),
        "user_agent": record.get("user_agent"),
        "device": "mobile" if "Mobile" in (record.get("user_agent") or "") else "desktop",
        "resolution": record.get("resolution"),

        # navigation
        "current_url": record.get("current_url"),
        "referrer_url": record.get("referrer_url"),

        # business
        "store_id": record.get("store_id"),

        # tracking
        "utm_source": record.get("utm_source"),
        "utm_medium": record.get("utm_medium"),
        "recommendation": record.get("recommendation"),

        # time
        "local_time": record.get("local_time"),
    }

    cart_products = record.get("cart_products", [])

    # ========================
    # CASE 1: No cart_products
    # ========================
    if not cart_products:
        base["product_id"] = None
        base["quantity"] = None
        return [base]

    # ========================
    # CASE 2: EXPLODE cart_products
    # ========================
    rows = []
    for item in cart_products:
        row = base.copy()

        row["product_id"] = item.get("product_id")
        row["quantity"] = item.get("amount")

        # optional (future-proof)
        row["price"] = item.get("price")
        row["currency"] = item.get("currency")

        rows.append(row)

    return rows


# ========================
# MAIN STREAM WRITE
# ========================
file_index = 1
row_count = 0

blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")
gcs_file = blob.open("w")

with gzip.open(INPUT_FILE, "rt", encoding="utf-8") as f:
    for i, line in enumerate(tqdm(f, desc="Processing")):

        try:
            record = json.loads(line)
        except:
            continue

        transformed_rows = transform(record)
        if not transformed_rows:
            continue

        # 👇 WRITE MULTI ROWS
        for row in transformed_rows:
            gcs_file.write(json.dumps(row) + "\n")
            row_count += 1

            if row_count % FLUSH_INTERVAL == 0:
                gcs_file.flush()

            if row_count >= CHUNK_SIZE:
                gcs_file.close()
                print(f"Uploaded part_{file_index}")

                file_index += 1
                row_count = 0

                blob = bucket.blob(f"{PREFIX}part_{file_index}.jsonl")
                gcs_file = blob.open("w")

gcs_file.close()
print("Done")